// One perf session's worth of Felix on real Azure networks.
//
// The unit of deployment is a *session*: one resource group holding
// everything, created for a run and deleted after it — nothing here is meant
// to idle (docs/perf-real-network.md owns the budget argument). The same
// template serves every tier: `tier` selects placement, which is the only
// thing the tiers differ in.
//
//   t1  one zone, proximity placement group   — the latency baseline
//   t2  brokers across zones 1/2/3            — what Quorum costs
//   (t3 is a second deployment of loadgen.bicep in another region)

@description('Where the session lives. Every resource follows the group.')
param location string = resourceGroup().location

@allowed(['t1', 't2'])
param tier string = 't1'

@description('URL of a release-layout tarball the brokers and control plane run. Empty when brokerRefs is set: then generator 0 builds the refs and seed.sh installs them.')
param releaseUrl string = ''

@description('name@sha the load generator builds felix-loadgen from (session.sh resolves the ref). This is the *instrument*, not the measured system, so it is deliberately separate from the broker build under test. Built once during provisioning, before any run.')
param loadgenRef string

@description('Space-separated name@sha specs generator 0 builds the broker and control plane from, served to the VNet on :8088 for seed.sh to install. Empty to run releaseUrl instead.')
param brokerRefs string = ''

@description('Subset of brokerRefs to build again with frame pointers (served as <name>-fp) for perf profiling.')
param fpRefs string = ''

param adminUsername string = 'felix'

@description('SSH public key for the admin user on every VM.')
param sshPublicKey string

@description('CIDR allowed to SSH in for manual debugging — the operator\'s address, not 0.0.0.0/0. Orchestration uses run-command and needs no inbound port.')
param allowedSshCidr string

@secure()
@description('Token the control plane\'s bootstrap listener requires.')
param bootstrapToken string

// Sized to fit a 20-vCPU Total Regional Cores quota (the Azure subscription's
// default): 3x4 brokers + 2 control plane + 4 load generator = 18 cores. The
// brokers stay at 4 vCPU — they are the system under test, kept comparable to
// the local runs — so the loadgen took the cut. Raise loadgenVmSize (and the
// quota) if the fanout/throughput cases show it CPU-bound.
param brokerCount int = 3
param brokerVmSize string = 'Standard_D4as_v5'
param controlPlaneVmSize string = 'Standard_D2as_v5'
param loadgenVmSize string = 'Standard_D4as_v5'

@description('Generator 0 SKU. It builds the broker refs during provisioning, so a larger size shortens the wait; empty means loadgenVmSize. Its CPU is sampled like every generator\'s.')
param loadgen0VmSize string = ''

@description('How many load generators. One D4 generator is crypto-bound near ~1.15 GB/s, well under what a broker can absorb, so measuring a broker\'s ceiling needs several driving it at once. Only generator 0 gets a public address; the rest are reachable through run-command like every other VM.')
@minValue(1)
@maxValue(8)
param loadgenCount int = 1

@description('Broker data disk, GiB. Premium, so fsync latency is a real number. Ignored when useLocalNvme is true.')
param brokerDataDiskGib int = 128

@description('Put the broker durable log on the SKU\'s ephemeral local disk instead of a Premium managed disk. For a Dadsv5-class SKU (e.g. Standard_D4ads_v5) this bypasses the managed-disk throughput cap that makes durable ingest disk-bound at ~170 MB/s on Premium SSD. Ephemeral, which is fine for a benchmark. When true, no managed data disk is attached and the broker mounts /dev/disk/azure/resource at /data.')
param useLocalNvme bool = false

@description('How many client-facing QUIC listeners each broker binds, on consecutive ports from 5000. One socket is one quinn endpoint driver, and that driver is a single task on a single core -- the per-broker throughput ceiling. More listeners spread inbound datagrams across more drivers. The internal listener is at 7000 and is clear of any sane range.')
@minValue(1)
@maxValue(64)
param brokerListeners int = 1

var prefix = 'felixperf'
var vnetCidr = '10.60.0.0/24'

// ---------------------------------------------------------------- network
resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: '${prefix}-nsg'
  location: location
  properties: {
    securityRules: [
      {
        name: 'ssh-from-operator'
        properties: {
          priority: 1000
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: allowedSshCidr
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      // Intra-VNet traffic (QUIC 5000/udp and up -- a broker may bind a run of
      // consecutive client listeners -- internal 7000/udp, CP 8080/tcp,
      // metrics 9<xx>) rides the default AllowVnetInBound rule; nothing else
      // is opened. That rule is port-agnostic, so widening the listener range
      // needs no NSG change. The operator drives the session with `az vm run-command`
      // (HTTPS to the Azure control plane), so no inbound port is needed for
      // orchestration; this SSH rule exists only so a human CAN open a shell
      // for debugging from their own address (never 0.0.0.0/0).
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${prefix}-vnet'
  location: location
  properties: {
    addressSpace: { addressPrefixes: [vnetCidr] }
    subnets: [
      {
        name: 'cluster'
        properties: {
          addressPrefix: vnetCidr
          networkSecurityGroup: { id: nsg.id }
        }
      }
    ]
  }
}

// Proximity keeps t1's numbers about the software, not about which rack the
// scheduler felt like. t2 wants the opposite: real inter-zone distance.
resource ppg 'Microsoft.Compute/proximityPlacementGroups@2024-07-01' = if (tier == 't1') {
  name: '${prefix}-ppg'
  location: location
  properties: { proximityPlacementGroupType: 'Standard' }
}

// ---------------------------------------------------------------- cloud-init
var brokerInit = base64(format(
  loadTextContent('cloudinit/broker.yaml'),
  releaseUrl,
  controlPlaneNic.properties.ipConfigurations[0].properties.privateIPAddress,
  useLocalNvme ? '1' : '0',
  string(brokerListeners)
))
var controlPlaneInit = base64(format(
  loadTextContent('cloudinit/controlplane.yaml'),
  releaseUrl,
  bootstrapToken
))
var loadgenInit = base64(format(
  loadTextContent('cloudinit/loadgen.yaml'),
  loadgenRef,
  brokerRefs,
  fpRefs,
  base64(loadTextContent('cloudinit/provision-loadgen.sh'))
))

// ---------------------------------------------------------------- machines
func nicName(role string, index int) string => '${prefix}-${role}-${index}-nic'

resource brokerNics 'Microsoft.Network/networkInterfaces@2024-05-01' = [
  for i in range(0, brokerCount): {
    name: nicName('broker', i)
    location: location
    properties: {
      enableAcceleratedNetworking: true
      ipConfigurations: [
        {
          name: 'primary'
          properties: {
            subnet: { id: vnet.properties.subnets[0].id }
            privateIPAllocationMethod: 'Dynamic'
          }
        }
      ]
    }
  }
]

resource controlPlaneNic 'Microsoft.Network/networkInterfaces@2024-05-01' = {
  name: nicName('controlplane', 0)
  location: location
  properties: {
    enableAcceleratedNetworking: true
    ipConfigurations: [
      {
        name: 'primary'
        properties: {
          subnet: { id: vnet.properties.subnets[0].id }
          privateIPAllocationMethod: 'Dynamic'
        }
      }
    ]
  }
}

// The operator reaches the session through the load generator alone: it is
// the only machine with a public address, and doubles as the jump host.
resource loadgenIp 'Microsoft.Network/publicIPAddresses@2024-05-01' = {
  name: '${prefix}-loadgen-ip'
  location: location
  sku: { name: 'Standard' }
  properties: { publicIPAllocationMethod: 'Static' }
}

resource loadgenNics 'Microsoft.Network/networkInterfaces@2024-05-01' = [
  for i in range(0, loadgenCount): {
    name: nicName('loadgen', i)
    location: location
    properties: {
      enableAcceleratedNetworking: true
      ipConfigurations: [
        {
          name: 'primary'
          properties: {
            subnet: { id: vnet.properties.subnets[0].id }
            privateIPAllocationMethod: 'Dynamic'
            publicIPAddress: i == 0 ? { id: loadgenIp.id } : null
          }
        }
      ]
    }
  }
]

var linuxConfiguration = {
  disablePasswordAuthentication: true
  ssh: {
    publicKeys: [
      { path: '/home/${adminUsername}/.ssh/authorized_keys', keyData: sshPublicKey }
    ]
  }
}

var image = {
  publisher: 'Canonical'
  offer: 'ubuntu-24_04-lts'
  sku: 'server'
  version: 'latest'
}

resource brokers 'Microsoft.Compute/virtualMachines@2024-07-01' = [
  for i in range(0, brokerCount): {
    name: '${prefix}-broker-${i}'
    location: location
    // t2's whole point: one broker per zone, so replication pays real
    // inter-zone RTT. t1 pins everything to one placement group instead.
    zones: tier == 't2' ? [string((i % 3) + 1)] : null
    properties: {
      hardwareProfile: { vmSize: brokerVmSize }
      proximityPlacementGroup: tier == 't1' ? { id: ppg.id } : null
      storageProfile: {
        imageReference: image
        osDisk: { createOption: 'FromImage', managedDisk: { storageAccountType: 'Premium_LRS' } }
        // No managed data disk when the durable log lives on the SKU's local
        // disk — the broker mounts /dev/disk/azure/resource at /data instead.
        dataDisks: useLocalNvme ? [] : [
          {
            lun: 0
            createOption: 'Empty'
            diskSizeGB: brokerDataDiskGib
            managedDisk: { storageAccountType: 'Premium_LRS' }
          }
        ]
      }
      osProfile: {
        computerName: '${prefix}-broker-${i}'
        adminUsername: adminUsername
        linuxConfiguration: linuxConfiguration
        customData: brokerInit
      }
      networkProfile: { networkInterfaces: [{ id: brokerNics[i].id }] }
    }
  }
]

// On t2 the control plane and generators sit in zone 1, so each broker's RTT
// is a known zone pair rather than wherever the scheduler put the client.
resource controlPlane 'Microsoft.Compute/virtualMachines@2024-07-01' = {
  name: '${prefix}-controlplane'
  location: location
  zones: tier == 't2' ? ['1'] : null
  properties: {
    hardwareProfile: { vmSize: controlPlaneVmSize }
    proximityPlacementGroup: tier == 't1' ? { id: ppg.id } : null
    storageProfile: {
      imageReference: image
      osDisk: { createOption: 'FromImage', managedDisk: { storageAccountType: 'Premium_LRS' } }
    }
    osProfile: {
      computerName: '${prefix}-controlplane'
      adminUsername: adminUsername
      linuxConfiguration: linuxConfiguration
      customData: controlPlaneInit
    }
    networkProfile: { networkInterfaces: [{ id: controlPlaneNic.id }] }
  }
}

// Generator 0 keeps the bare name: it is the jump host and every existing
// script addresses it as `<prefix>-loadgen`. The rest are -2, -3, ... matching
// what run-nvme-multi.sh already expects.
resource loadgens 'Microsoft.Compute/virtualMachines@2024-07-01' = [
  for i in range(0, loadgenCount): {
    name: i == 0 ? '${prefix}-loadgen' : '${prefix}-loadgen-${i + 1}'
    location: location
    zones: tier == 't2' ? ['1'] : null
    properties: {
      hardwareProfile: { vmSize: i == 0 && !empty(loadgen0VmSize) ? loadgen0VmSize : loadgenVmSize }
      proximityPlacementGroup: tier == 't1' ? { id: ppg.id } : null
      storageProfile: {
        imageReference: image
        // Room for two release target dirs (plain and frame-pointer) on the
        // builder; the stock 30 GiB image disk fills up.
        osDisk: {
          createOption: 'FromImage'
          diskSizeGB: 128
          managedDisk: { storageAccountType: 'Premium_LRS' }
        }
      }
      osProfile: {
        computerName: i == 0 ? '${prefix}-loadgen' : '${prefix}-loadgen-${i + 1}'
        adminUsername: adminUsername
        linuxConfiguration: linuxConfiguration
        customData: loadgenInit
      }
      networkProfile: { networkInterfaces: [{ id: loadgenNics[i].id }] }
    }
  }
]

output loadgenPublicIp string = loadgenIp.properties.ipAddress
output loadgenPrivateIp string = loadgenNics[0].properties.ipConfigurations[0].properties.privateIPAddress
output controlPlaneIp string = controlPlaneNic.properties.ipConfigurations[0].properties.privateIPAddress
output brokerIps array = [for i in range(0, brokerCount): brokerNics[i].properties.ipConfigurations[0].properties.privateIPAddress]
output loadgenNames array = [for i in range(0, loadgenCount): i == 0 ? '${prefix}-loadgen' : '${prefix}-loadgen-${i + 1}']
