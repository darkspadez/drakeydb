# drakeydb

![Version: 1.40.1](https://img.shields.io/badge/Version-1.40.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v1.40.1](https://img.shields.io/badge/AppVersion-v1.40.1-informational?style=flat-square)

drakeydb is a Dragonfly fork with KeyDB-style multi-master replication, fully compatible with Redis and Memcached APIs.

**Homepage:** <https://github.com/darkspadez/drakeydb>

## Source Code

* <https://github.com/darkspadez/drakeydb>

## Requirements

Kubernetes: `>=1.23.0-0`

## Installing from this repository

The fork chart is not published as an OCI package yet. Install it from a checkout of this
repository:

```shell
helm upgrade --install drakeydb ./contrib/charts/drakeydb
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicaCount | int | `1` | Number of replicas to deploy |
| image.repository | string | `"docker.dragonflydb.io/dragonflydb/dragonfly"` | Container Image Registry to pull the image from TODO(drakeydb): switch to a drakeydb image once fork images are published; until then this chart deploys the upstream Dragonfly image. |
| image.pullPolicy | string | `"IfNotPresent"` | Dragonfly image pull policy |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` | Container Registry Secret names in an array |
| nameOverride | string | `""` | String to partially override dragonfly.fullname |
| fullnameOverride | string | `""` | String to fully override dragonfly.fullname |
| commonLabels | object | `{}` | Common labels to add to all resources |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| podAnnotations | object | `{}` | Annotations for pods |
| podSecurityContext | object | `{}` | Set securityContext for pod itself |
| securityContext | object | `{}` | Set securityContext for containers |
| hostNetwork | bool | `false` | Set hostNetwork for pod |
| service.type | string | `"ClusterIP"` | Accepted values are NodePort, ClusterIP, and LoadBalancer. |
| service.loadBalancerIP | string | `""` | Load balancer static ip to use when service type is set to LoadBalancer |
| service.clusterIP | string | `""` | Cluster IP address to assign to the service. Leave empty to auto-allocate |
| service.port | int | `6379` | Dragonfly service port |
| service.annotations | object | `{}` | Extra annotations for the service |
| service.labels | object | `{}` | Extra labels for the service |
| service.metrics.portName | string | `"metrics"` | name for the metrics port |
| service.metrics.serviceType | string | `"ClusterIP"` | serviceType for the metrics service |
| serviceMonitor.enabled | bool | `false` | If true, a ServiceMonitor CRD is created for a prometheus operator |
| serviceMonitor.namespace | string | `""` | namespace in which to deploy the ServiceMonitor CR. defaults to the application namespace |
| serviceMonitor.labels | object | `{}` | additional labels to apply to the metrics |
| serviceMonitor.annotations | object | `{}` | additional annotations to apply to the metrics |
| serviceMonitor.interval | string | `"10s"` | scrape interval |
| serviceMonitor.scrapeTimeout | string | `"10s"` | scrape timeout |
| serviceMonitor.tlsConfig | object | `{}` | TLS settings for scraping. serverName defaults to the metrics Service DNS name. Configure ca (for example, ca.secret.name and ca.secret.key) when TLS certificate validation should use a private CA. |
| prometheusRule.enabled | bool | `false` | Deploy a PrometheusRule |
| prometheusRule.spec | list | `[]` | PrometheusRule.Spec https://awesome-prometheus-alerts.grep.to/rules |
| storage.enabled | bool | `false` | If /data should persist. This will provision a StatefulSet instead. |
| storage.storageClassName | string | `""` | Global StorageClass for Persistent Volume(s) |
| storage.requests | string | `"128Mi"` | Volume size to request for the PVC |
| storage.useFullnameForVolumes | bool | `false` | Use the chart's fullname (instead of the release name) for the StatefulSet's serviceName and the data volume/claim name. This avoids name collisions when the chart is installed as a subchart dependency. Do not enable this on an existing storage-enabled release: Kubernetes does not allow changing serviceName or renaming volumeClaimTemplates in place, so it only applies cleanly to new installs. |
| tls.enabled | bool | `false` | enable TLS |
| tls.createCerts | bool | `false` | use cert-manager to automatically create the certificate |
| tls.duration | string | `"87600h0m0s"` | duration or ttl of the validity of the created certificate |
| tls.issuer.kind | string | `"ClusterIssuer"` | cert-manager issuer kind. Usually Issuer or ClusterIssuer |
| tls.issuer.name | string | `"selfsigned"` | name of the referenced issuer |
| tls.issuer.group | string | `"cert-manager.io"` | group of the referenced issuer if you are using an external issuer, change this to that issuer group. |
| tls.existing_secret | string | `""` | use TLS certificates from existing secret |
| tls.cert | string | `""` | TLS certificate |
| tls.key | string | `""` | TLS private key |
| passwordFromSecret | object | `{"enable":false,"existingSecret":{"key":"","name":""}}` | Set the password environment variable from the specified existing Secret. If enabled and the Secret does not exist, pods will not start. |
| passwordFromSecret.existingSecret | object | `{"key":"","name":""}` | Existing Secret containing the password. |
| passwordFromSecret.existingSecret.name | string | `""` | Existing Secret name. |
| passwordFromSecret.existingSecret.key | string | `""` | Secret key containing the password. |
| probes.livenessProbe.exec.command[0] | string | `"/bin/sh"` |  |
| probes.livenessProbe.exec.command[1] | string | `"/usr/local/bin/healthcheck.sh"` |  |
| probes.livenessProbe.initialDelaySeconds | int | `10` |  |
| probes.livenessProbe.periodSeconds | int | `10` |  |
| probes.livenessProbe.timeoutSeconds | int | `5` |  |
| probes.livenessProbe.failureThreshold | int | `3` |  |
| probes.livenessProbe.successThreshold | int | `1` |  |
| probes.readinessProbe.exec.command[0] | string | `"/bin/sh"` |  |
| probes.readinessProbe.exec.command[1] | string | `"/usr/local/bin/healthcheck.sh"` |  |
| probes.readinessProbe.initialDelaySeconds | int | `10` |  |
| probes.readinessProbe.periodSeconds | int | `10` |  |
| probes.readinessProbe.timeoutSeconds | int | `5` |  |
| probes.readinessProbe.failureThreshold | int | `3` |  |
| probes.readinessProbe.successThreshold | int | `1` |  |
| command | list | `[]` | Allow overriding the container's command |
| extraArgs | list | `[]` | Extra arguments to pass to the dragonfly binary |
| cluster.mode | string | `""` | Redis cluster protocol mode passed to Dragonfly as `--cluster_mode`; accepts `emulated` or `yes`, and empty disables cluster mode |
| cluster.experimentalShardBySlot | bool | `false` | When cluster mode is enabled, pass `--experimental_cluster_shard_by_slot=true` so sharding follows slots instead of hash tags |
| extraVolumes | list | `[]` | Extra volumes to mount into the pods |
| extraVolumeMounts | list | `[]` | Extra volume mounts corresponding to the volumes mounted above |
| initContainers | list | `[]` | A list of initContainers to run before each pod starts |
| extraContainers | list | `[]` | Additional sidecar containers |
| extraObjects | list | `[]` | extra K8s manifests to deploy |
| resources.requests | object | `{}` | The requested resources for the containers |
| resources.limits | object | `{}` | The resource limits for the containers |
| env | list | `[]` | extra environment variables |
| envFrom | list | `[]` | extra environment variables from K8s objects |
| priorityClassName | string | `""` | Priority class name for pod assignment |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| tolerations | list | `[]` | Tolerations for pod assignment |
| affinity | object | `{}` | Affinity for pod assignment |
| topologySpreadConstraints | list | `[]` | Topology Spread Constraints for pod assignment |
