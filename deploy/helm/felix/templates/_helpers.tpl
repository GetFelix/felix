{{/*
Names and labels. Two workloads share one release, so every name carries
which one it is.
*/}}
{{- define "felix.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "felix.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Node ids are DNS names under peer mTLS, so the broker name must be one. */}}
{{- define "felix.controlplane.fullname" -}}
{{- printf "%s-controlplane" (include "felix.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "felix.broker.fullname" -}}
{{- printf "%s-broker" (include "felix.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "felix.broker.headless" -}}
{{- printf "%s-headless" (include "felix.broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "felix.controlplane.headless" -}}
{{- printf "%s-headless" (include "felix.controlplane.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "felix.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "felix.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "felix.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "felix.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "felix.controlplane.selectorLabels" -}}
app.kubernetes.io/name: {{ include "felix.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controlplane
{{- end -}}

{{- define "felix.broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "felix.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end -}}

{{/*
Image references. A digest wins over a tag: the tag is what a human reads,
the digest is what runs.
*/}}
{{- define "felix.image" -}}
{{- $registry := .root.Values.image.registry -}}
{{- $tag := default .root.Chart.AppVersion .image.tag -}}
{{- if .image.digest -}}
{{- printf "%s/%s@%s" $registry .image.repository .image.digest -}}
{{- else -}}
{{- printf "%s/%s:%s" $registry .image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* The URL brokers reach the control plane at. */}}
{{- define "felix.controlplane.url" -}}
{{- if .Values.broker.controlplaneUrl -}}
{{- .Values.broker.controlplaneUrl -}}
{{- else -}}
{{- printf "%s://%s.%s.svc.%s:%d" (ternary "https" "http" .Values.controlplane.tls.enabled) (include "felix.controlplane.fullname" .) .Release.Namespace .Values.clusterDomain (int .Values.controlplane.service.port) -}}
{{- end -}}
{{- end -}}

{{/* What each broker tells discovery to hand clients. */}}
{{- define "felix.broker.clientAdvertiseAddr" -}}
{{- if .Values.broker.clientAdvertiseAddr -}}
{{- .Values.broker.clientAdvertiseAddr -}}
{{- else -}}
{{- printf "$(POD_NAME).%s.$(POD_NAMESPACE).svc.%s:%d" (include "felix.broker.headless" .) .Values.clusterDomain (int .Values.broker.ports.client) -}}
{{- end -}}
{{- end -}}

{{/*
The Raft peers map every member is told: "1=<pod-0>.<headless>:<peerPort>,...".
Every member carries the same map, since initialising two disjoint groups
is two control planes.
*/}}
{{/*
The ConfigMap whose presence says the Raft group has formed once.
*/}}
{{- define "felix.controlplane.raftFormed" -}}
{{- printf "%s-raft-formed" (include "felix.controlplane.fullname" .) -}}
{{- end -}}

{{- define "felix.controlplane.raftPeers" -}}
{{- $name := include "felix.controlplane.fullname" . -}}
{{- $headless := include "felix.controlplane.headless" . -}}
{{- $port := int .Values.controlplane.storage.raft.peerPort -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.controlplane.replicas) -}}
{{- $peers = append $peers (printf "%d=%s-%d.%s:%d" (add $i 1) $name $i $headless $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{/*
Grace periods. The drain has to finish before SIGKILL, so the period is
derived from the drain unless the operator sets one that still fits.
*/}}
{{- define "felix.broker.terminationGracePeriodSeconds" -}}
{{- $needed := add (int .Values.broker.shutdown.preStopSeconds) (div (add (int .Values.broker.shutdown.handoffTimeoutMs) (int .Values.broker.shutdown.drainTimeoutMs)) 1000) 5 -}}
{{- if .Values.broker.shutdown.terminationGracePeriodSeconds -}}
{{- $set := int .Values.broker.shutdown.terminationGracePeriodSeconds -}}
{{- if lt $set $needed -}}
{{- fail (printf "broker.shutdown.terminationGracePeriodSeconds (%d) is shorter than the preStop sleep plus the handoff and drain budgets (%d): SIGKILL would arrive mid-drain" $set $needed) -}}
{{- end -}}
{{- $set -}}
{{- else -}}
{{- $needed -}}
{{- end -}}
{{- end -}}

{{- define "felix.controlplane.terminationGracePeriodSeconds" -}}
{{- $needed := add (div (add (int .Values.controlplane.shutdown.predrainMs) (int .Values.controlplane.shutdown.drainTimeoutMs)) 1000) 5 -}}
{{- if .Values.controlplane.shutdown.terminationGracePeriodSeconds -}}
{{- $set := int .Values.controlplane.shutdown.terminationGracePeriodSeconds -}}
{{- if lt $set $needed -}}
{{- fail (printf "controlplane.shutdown.terminationGracePeriodSeconds (%d) is shorter than predrain plus drain (%d): SIGKILL would arrive mid-drain" $set $needed) -}}
{{- end -}}
{{- $set -}}
{{- else -}}
{{- $needed -}}
{{- end -}}
{{- end -}}

{{/*
Combinations that are each fine alone and wrong together. Checked once, from
the workload templates, so a bad release fails to render rather than half
applies.
*/}}
{{- define "felix.validate" -}}
{{- $cp := .Values.controlplane -}}
{{- $b := .Values.broker -}}
{{- if $cp.enabled -}}
{{- if not (has $cp.storage.backend (list "memory" "postgres" "raft")) -}}
{{- fail (printf "controlplane.storage.backend must be memory, postgres or raft, not %q" $cp.storage.backend) -}}
{{- end -}}
{{- if and (eq $cp.storage.backend "postgres") (not $cp.storage.postgres.existingSecret) -}}
{{- fail "controlplane.storage.backend=postgres needs controlplane.storage.postgres.existingSecret: a Secret holding the connection URL" -}}
{{- end -}}
{{- if eq $cp.storage.backend "raft" -}}
{{- if lt (int $cp.replicas) 3 -}}
{{- fail (printf "controlplane.storage.backend=raft needs at least three members, not %d: a group of one or two cannot lose an instance" (int $cp.replicas)) -}}
{{- end -}}
{{- if eq (mod (int $cp.replicas) 2) 0 -}}
{{- fail (printf "controlplane.storage.backend=raft needs an odd number of members, not %d: an even group tolerates the same failures as the odd one below it and adds an election tie" (int $cp.replicas)) -}}
{{- end -}}
{{- if and (not $cp.storage.raft.peerToken.existingSecret) (not $cp.storage.raft.insecurePeers) -}}
{{- fail "controlplane.storage.backend=raft needs controlplane.storage.raft.peerToken.existingSecret: a Secret holding the Raft peer token, without which any pod that reaches the peer port can replace the metadata store" -}}
{{- end -}}
{{- if eq (int $cp.storage.raft.peerPort) (int $cp.service.port) -}}
{{- fail "controlplane.storage.raft.peerPort and controlplane.service.port share a port; the Raft RPCs need a listener of their own" -}}
{{- end -}}
{{- if and $cp.storage.raft.tls.enabled (not $cp.storage.raft.tls.existingSecret) -}}
{{- fail "controlplane.storage.raft.tls.enabled needs controlplane.storage.raft.tls.existingSecret: a Secret with the members' peer certificate, key and CA" -}}
{{- end -}}
{{- if not (has $cp.storage.raft.initialClusterState (list "" "new" "existing")) -}}
{{- fail (printf "controlplane.storage.raft.initialClusterState must be new, existing or empty, not %q" $cp.storage.raft.initialClusterState) -}}
{{- end -}}
{{- end -}}
{{- if and (eq $cp.storage.backend "memory") (gt (int $cp.replicas) 1) -}}
{{- fail "controlplane.storage.backend=memory keeps metadata in one process; more than one replica would be two control planes that disagree" -}}
{{- end -}}
{{- if and $cp.tls.enabled (not $cp.tls.existingSecret) -}}
{{- fail "controlplane.tls.enabled needs controlplane.tls.existingSecret: a kubernetes.io/tls Secret with the API certificate" -}}
{{- end -}}
{{- if and $cp.bootstrap.enabled (not $cp.bootstrap.existingSecret) -}}
{{- fail "controlplane.bootstrap.enabled needs controlplane.bootstrap.existingSecret: a Secret holding the bootstrap token" -}}
{{- end -}}
{{- if and $cp.podDisruptionBudget.enabled (gt (int $cp.replicas) 1) (ge (int $cp.podDisruptionBudget.minAvailable) (int $cp.replicas)) -}}
{{- fail (printf "controlplane.podDisruptionBudget.minAvailable (%d) is not below controlplane.replicas (%d): no voluntary disruption could ever proceed, including the rolling update itself" (int $cp.podDisruptionBudget.minAvailable) (int $cp.replicas)) -}}
{{- end -}}
{{- end -}}
{{- if $b.enabled -}}
{{- if not $b.credential.existingSecret -}}
{{- fail "broker.credential.existingSecret is empty: a broker with a node id refuses to start without a credential. Install with broker.enabled=false, mint the credential through bootstrap, create the Secret, then upgrade" -}}
{{- end -}}
{{- if eq (int $b.ports.client) (int $b.ports.internal) -}}
{{- fail "broker.ports.client and broker.ports.internal share a port; the broker refuses that at startup" -}}
{{- end -}}
{{- if and (not $cp.enabled) (not $b.controlplaneUrl) -}}
{{- fail "controlplane.enabled=false needs broker.controlplaneUrl: brokers cannot advertise a control plane they have to guess" -}}
{{- end -}}
{{- if and $b.podDisruptionBudget.enabled (ge (int $b.podDisruptionBudget.maxUnavailable) (int $b.replicas)) -}}
{{- fail (printf "broker.podDisruptionBudget.maxUnavailable (%d) is not below broker.replicas (%d): the budget would permit evicting every broker at once" (int $b.podDisruptionBudget.maxUnavailable) (int $b.replicas)) -}}
{{- end -}}
{{- if and $b.podDisruptionBudget.enabled (ge (int $b.replicas) 3) (gt (int $b.podDisruptionBudget.maxUnavailable) 1) -}}
{{- fail (printf "broker.podDisruptionBudget.maxUnavailable (%d) is above one: a replication-factor-three shard loses its quorum when two of its replicas are evicted together" (int $b.podDisruptionBudget.maxUnavailable)) -}}
{{- end -}}
{{- if not (or $b.peerTls.enabled $b.peerTls.allowUnauthenticated) -}}
{{- fail "brokers refuse to start without peer mTLS: set broker.peerTls.enabled=true (needs cert-manager and its CSI driver), or broker.peerTls.allowUnauthenticated=true to rely on the NetworkPolicy alone" -}}
{{- end -}}
{{- if and $b.clientTls.enabled (not $b.clientTls.existingSecret) -}}
{{- fail "broker.clientTls.enabled needs broker.clientTls.existingSecret: a kubernetes.io/tls Secret with the certificate clients verify" -}}
{{- end -}}
{{- if and $b.peerTls.enabled (not $b.peerTls.issuerName) -}}
{{- fail "broker.peerTls.enabled needs broker.peerTls.issuerName: the cert-manager issuer of the peer CA" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Pod scheduling shared by both workloads: anti-affinity and spread. */}}
{{- define "felix.scheduling" -}}
{{- $w := .workload -}}
{{- $labels := .selectorLabels -}}
{{- if or (ne $w.antiAffinity "none") $w.topologySpread.enabled }}
{{- if ne $w.antiAffinity "none" }}
affinity:
  podAntiAffinity:
    {{- if eq $w.antiAffinity "hard" }}
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            {{- $labels | nindent 12 }}
        topologyKey: kubernetes.io/hostname
    {{- else }}
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              {{- $labels | nindent 14 }}
          topologyKey: kubernetes.io/hostname
    {{- end }}
{{- end }}
{{- if $w.topologySpread.enabled }}
topologySpreadConstraints:
  - maxSkew: {{ $w.topologySpread.maxSkew }}
    topologyKey: {{ $w.topologySpread.topologyKey }}
    whenUnsatisfiable: {{ $w.topologySpread.whenUnsatisfiable }}
    labelSelector:
      matchLabels:
        {{- $labels | nindent 8 }}
{{- end }}
{{- end }}
{{- if $w.nodeSelector }}
nodeSelector:
  {{- toYaml $w.nodeSelector | nindent 2 }}
{{- end }}
{{- if $w.tolerations }}
tolerations:
  {{- toYaml $w.tolerations | nindent 2 }}
{{- end }}
{{- if $w.priorityClassName }}
priorityClassName: {{ $w.priorityClassName }}
{{- end }}
{{- end -}}

{{/* The images run as uid 65532 and never need more. */}}
{{- define "felix.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
fsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "felix.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end -}}
