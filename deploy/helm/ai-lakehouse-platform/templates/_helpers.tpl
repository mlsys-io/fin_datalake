{{- define "ai-lakehouse-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ai-lakehouse-platform.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "ai-lakehouse-platform.name" . -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ai-lakehouse-platform.labels" -}}
helm.sh/chart: {{ include "ai-lakehouse-platform.chart" . }}
app.kubernetes.io/name: {{ include "ai-lakehouse-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ai-lakehouse-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ai-lakehouse-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ai-lakehouse-platform.gatewaySecretName" -}}
{{- if .Values.gateway.secretEnv.name -}}
{{- .Values.gateway.secretEnv.name -}}
{{- else -}}
{{- printf "%s-gateway" (include "ai-lakehouse-platform.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.gatewayTlsMountPath" -}}
{{- default "/opt/certs" .Values.global.minioCa.mountPath -}}
{{- end -}}

{{- define "ai-lakehouse-platform.sharedSecretName" -}}
{{- default "etl-secrets" .Values.sharedSecrets.existingSecret -}}
{{- end -}}

{{- define "ai-lakehouse-platform.redisUrlDb0" -}}
{{- if .Values.global.externalServices.redisUrl -}}
{{- .Values.global.externalServices.redisUrl -}}
{{- else if .Values.redis.enabled -}}
{{- printf "redis://:%s@redis-master.%s.svc.cluster.local:6379/0" .Values.redis.auth.password .Release.Namespace -}}
{{- else -}}
{{- printf "redis://:redis-lakehouse-pass@redis-master.%s.svc.cluster.local:6379/0" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.redisUrlDb1" -}}
{{- $db0 := include "ai-lakehouse-platform.redisUrlDb0" . -}}
{{- regexReplaceAll "/0$" $db0 "/1" -}}
{{- end -}}

{{- define "ai-lakehouse-platform.prefectUiUrl" -}}
{{- if .Values.global.externalServices.prefectUiUrl -}}
{{- .Values.global.externalServices.prefectUiUrl -}}
{{- else -}}
{{- printf "http://prefect-server.%s.svc.cluster.local:4200" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.prefectHost" -}}
{{- if .Values.global.externalServices.prefectHost -}}
{{- .Values.global.externalServices.prefectHost -}}
{{- else -}}
{{- printf "prefect-server.%s.svc.cluster.local" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.rayHeadService" -}}
{{- printf "%s-head-svc" .Values.rayCluster.name -}}
{{- end -}}

{{- define "ai-lakehouse-platform.rayAddress" -}}
{{- if .Values.global.externalServices.rayAddress -}}
{{- .Values.global.externalServices.rayAddress -}}
{{- else -}}
{{- printf "ray://%s.%s.svc.cluster.local:10001" (include "ai-lakehouse-platform.rayHeadService" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.rayHost" -}}
{{- if .Values.global.externalServices.rayHost -}}
{{- .Values.global.externalServices.rayHost -}}
{{- else -}}
{{- printf "%s.%s.svc.cluster.local" (include "ai-lakehouse-platform.rayHeadService" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.rayDashboardUrl" -}}
{{- if .Values.global.externalServices.rayDashboardUrl -}}
{{- .Values.global.externalServices.rayDashboardUrl -}}
{{- else -}}
{{- printf "http://%s.%s.svc.cluster.local:8265" (include "ai-lakehouse-platform.rayHeadService" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.gatewayInternalUrl" -}}
{{- if .Values.global.externalServices.gatewayInternalUrl -}}
{{- .Values.global.externalServices.gatewayInternalUrl -}}
{{- else -}}
{{- printf "http://etl-gateway-svc.%s.svc.cluster.local:8000" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.gatewayHost" -}}
{{- if .Values.global.externalServices.gatewayHost -}}
{{- .Values.global.externalServices.gatewayHost -}}
{{- else -}}
{{- printf "etl-gateway-svc.%s.svc.cluster.local" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.tsdbHost" -}}
{{- if .Values.global.externalServices.tsdbHost -}}
{{- .Values.global.externalServices.tsdbHost -}}
{{- else -}}
{{- printf "%s-ha.%s.svc.cluster.local" .Values.timescaledb.name .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.tsdbPort" -}}
{{- default "5432" .Values.global.externalServices.tsdbPort -}}
{{- end -}}

{{- define "ai-lakehouse-platform.tsdbUser" -}}
{{- default .Values.timescaledb.user .Values.global.externalServices.tsdbUser -}}
{{- end -}}

{{- define "ai-lakehouse-platform.tsdbDatabase" -}}
{{- default .Values.timescaledb.database .Values.global.externalServices.tsdbDatabase -}}
{{- end -}}

{{- define "ai-lakehouse-platform.tsdbPasswordSecretName" -}}
{{- if .Values.global.externalServices.tsdbPasswordSecretName -}}
{{- .Values.global.externalServices.tsdbPasswordSecretName -}}
{{- else -}}
{{- include "ai-lakehouse-platform.sharedSecretName" . -}}
{{- end -}}
{{- end -}}

{{- define "ai-lakehouse-platform.awsEndpointUrl" -}}
{{- if .Values.global.externalServices.awsEndpointUrl -}}
{{- .Values.global.externalServices.awsEndpointUrl -}}
{{- else -}}
{{- default "https://luyao-storage-head.ddns.comp.nus.edu.sg:4000" .Values.overseer.env.AWS_ENDPOINT_URL -}}
{{- end -}}
{{- end -}}
