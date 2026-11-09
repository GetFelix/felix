// A comparison session: the same Azure hardware Felix's t1 run used, but the
// brokers run a *different* system (Redpanda, Kafka, or NATS) so its numbers can
// be read next to Felix's. The systems are installed and torn down over
// run-command AFTER provisioning, so one deployment of these VMs serves all
// three in turn — every system is measured on the identical physical machines,
// which removes the cloud hardware lottery from the comparison between them.
//
// Deliberately matched to main.bicep: D4as_v5 brokers, Premium SSD, accelerated
// networking, one proximity placement group. What is dropped is Felix-specific:
// no control plane, no IdP, no seeding. Two client VMs (not one) so the run can
// ramp load the way the Felix multi-loadgen run did — one client to find the
// single-generator number, two to push the brokers toward their wall.

@description('Where the session lives. Every resource follows the group.')
param location string = resourceGroup().location

param brokerCount int = 3
param clientCount int = 2

// Matched to the Felix t1 brokers exactly — the system under test is kept
// comparable to the local and Felix runs. 3x4 brokers + 2x4 clients = 20 vCPU,
// the Azure subscription's Total Regional quota to the core.
param brokerVmSize string = 'Standard_D4as_v5'
param clientVmSize string = 'Standard_D4as_v5'

@description('Broker data disk, GiB. Premium, so fsync latency is a real number — same as the Felix run.')
param brokerDataDiskGib int = 128

param adminUsername string = 'felix'

@description('SSH public key for the admin user on every VM.')
param sshPublicKey string

@description('CIDR allowed to SSH in for manual debugging — the operator\'s address, not 0.0.0.0/0. Orchestration uses run-command and needs no inbound port.')
param allowedSshCidr string

var prefix = 'cmp'
var vnetCidr = '10.61.0.0/24'

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
      // Everything the systems need is intra-VNet (Kafka 9092, Redpanda 9092 +
      // 33145 rpc, NATS 4222 + 6222 cluster, iperf3 5201) and rides the default
      // AllowVnetInBound rule. The operator drives the session with run-command
      // over HTTPS, so nothing else is opened.
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

// Proximity keeps the comparison about the software, not about which racks the
// scheduler spread the VMs across — same reasoning as the Felix t1 baseline.
resource ppg 'Microsoft.Compute/proximityPlacementGroups@2024-07-01' = {
  name: '${prefix}-ppg'
  location: location
  properties: { proximityPlacementGroupType: 'Standard' }
}

// One cloud-init for every machine: mount the data disk if present, install the
// common tooling (a JRE for the Kafka perf CLIs, iperf3 for the network-ceiling
// check, jq), and apply the socket-buffer tuning the Felix brokers also got so
// no side of the comparison is throttled by stock 212 KiB TCP buffers.
var commonInit = base64(loadTextContent('cloudinit/base.yaml'))

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

resource clientNics 'Microsoft.Network/networkInterfaces@2024-05-01' = [
  for i in range(0, clientCount): {
    name: nicName('client', i)
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
    properties: {
      hardwareProfile: { vmSize: brokerVmSize }
      proximityPlacementGroup: { id: ppg.id }
      storageProfile: {
        imageReference: image
        osDisk: { createOption: 'FromImage', managedDisk: { storageAccountType: 'Premium_LRS' } }
        dataDisks: [
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
        customData: commonInit
      }
      networkProfile: { networkInterfaces: [{ id: brokerNics[i].id }] }
    }
  }
]

resource clients 'Microsoft.Compute/virtualMachines@2024-07-01' = [
  for i in range(0, clientCount): {
    name: '${prefix}-client-${i}'
    location: location
    properties: {
      hardwareProfile: { vmSize: clientVmSize }
      proximityPlacementGroup: { id: ppg.id }
      storageProfile: {
        imageReference: image
        osDisk: { createOption: 'FromImage', managedDisk: { storageAccountType: 'Premium_LRS' } }
      }
      osProfile: {
        computerName: '${prefix}-client-${i}'
        adminUsername: adminUsername
        linuxConfiguration: linuxConfiguration
        customData: commonInit
      }
      networkProfile: { networkInterfaces: [{ id: clientNics[i].id }] }
    }
  }
]

output brokerIps array = [for i in range(0, brokerCount): brokerNics[i].properties.ipConfigurations[0].properties.privateIPAddress]
output clientIps array = [for i in range(0, clientCount): clientNics[i].properties.ipConfigurations[0].properties.privateIPAddress]
