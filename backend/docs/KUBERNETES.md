# Guía de Despliegue en Kubernetes

Esta guía describe cómo desplegar el Sistema de Archivos Distribuido (DFS) en un cluster de Kubernetes usando Helm.

## Prerrequisitos

Antes de comenzar el despliegue, asegúrese de tener configurado lo siguiente en su cluster de Kubernetes.

### Requisitos del Cluster

**Versión de Kubernetes**: Se requiere Kubernetes 1.20 o superior. El chart utiliza APIs estables disponibles desde esa versión.

**Nodos Worker**: Se recomienda al menos 3 nodos worker para garantizar distribución adecuada de los DataNodes y tolerancia a fallos. Cada nodo debe tener suficiente capacidad de almacenamiento para los volúmenes persistentes.

**Recursos Mínimos**: Por nodo worker se recomienda:
- CPU: 4 cores
- RAM: 8GB
- Disco: 200GB de almacenamiento disponible para PVs

**StorageClass**: Debe existir un StorageClass configurado que pueda provisionar PersistentVolumes dinámicamente. Puede ser local-path, NFS, Ceph, AWS EBS, GCP PD, etc.

### Herramientas Necesarias

**kubectl**: Cliente de línea de comandos de Kubernetes instalado y configurado para acceder a su cluster.

```bash
# Verificar versión
kubectl version --client

# Verificar acceso al cluster
kubectl cluster-info
kubectl get nodes
```

**Helm**: Gestor de paquetes de Kubernetes versión 3.0 o superior.

```bash
# Instalar Helm (si no está instalado)
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Verificar versión
helm version
```

## Preparación del Cluster

### Crear Namespace

Aunque el Helm chart crea el namespace automáticamente, puede crearlo manualmente si lo prefiere:

```bash
kubectl create namespace dfs
kubectl label namespace dfs name=dfs
```

### Verificar StorageClass

Verifique que existe un StorageClass disponible:

```bash
# Listar StorageClasses
kubectl get storageclass

# Ver detalles de una StorageClass
kubectl describe storageclass standard
```

Si no existe ninguna StorageClass, deberá crear una. Ejemplo para local-path:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-path
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
```

### Configurar RBAC (si es necesario)

Si su cluster tiene RBAC habilitado (recomendado), el chart creará automáticamente los ServiceAccounts y permisos necesarios. No se requiere configuración adicional.

## Instalación con Helm

### Instalación Básica

Para una instalación rápida con valores por defecto:

```bash
cd /home/ubuntu/dfs-system/infra/helm/dfs-chart

helm install dfs-prod . \
  --namespace dfs \
  --create-namespace
```

Este comando despliega:
- 1 Metadata Service
- 3 DataNodes (StatefulSet)
- Prometheus para monitoreo
- Grafana para visualización

### Instalación con Valores Personalizados

Para producción, se recomienda crear un archivo de valores personalizado:

```bash
# Crear archivo de valores para producción
cat > production-values.yaml <<EOF
global:
  namespace: dfs
  storageClass: fast-ssd

metadata:
  replicaCount: 1
  persistence:
    enabled: true
    size: 20Gi
    storageClass: fast-ssd
  resources:
    requests:
      memory: "2Gi"
      cpu: "2000m"
    limits:
      memory: "4Gi"
      cpu: "4000m"

datanode:
  replicaCount: 5
  persistence:
    enabled: true
    size: 500Gi
    storageClass: standard
  resources:
    requests:
      memory: "4Gi"
      cpu: "2000m"
    limits:
      memory: "8Gi"
      cpu: "4000m"

prometheus:
  enabled: true
  persistence:
    enabled: true
    size: 50Gi
  retention: 30d

grafana:
  enabled: true
  service:
    type: LoadBalancer
  adminPassword: "CHANGE-ME-IN-PRODUCTION"
  persistence:
    enabled: true
    size: 10Gi

ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
  hosts:
    - host: dfs.example.com
      paths:
        - path: /
          pathType: Prefix
          service: dfs-metadata
          port: 8000
    - host: grafana.example.com
      paths:
        - path: /
          pathType: Prefix
          service: dfs-grafana
          port: 3000
  tls:
    - secretName: dfs-tls
      hosts:
        - dfs.example.com
        - grafana.example.com

podDisruptionBudget:
  metadata:
    enabled: true
    minAvailable: 1
  datanode:
    enabled: true
    minAvailable: 3
EOF

# Instalar con valores personalizados
helm install dfs-prod . \
  -f production-values.yaml \
  --namespace dfs \
  --create-namespace
```

### Verificar Instalación

Una vez completada la instalación, verifique que todos los componentes están corriendo:

```bash
# Ver todos los recursos
kubectl get all -n dfs

# Ver pods con más detalle
kubectl get pods -n dfs -o wide

# Ver servicios
kubectl get svc -n dfs

# Ver PVCs
kubectl get pvc -n dfs

# Ver estado del Helm release
helm status dfs-prod -n dfs
```

Todos los pods deben estar en estado `Running` y `Ready 1/1`. Si algún pod está en estado `Pending` o `CrashLoopBackOff`, consulte la sección de Troubleshooting.

## Configuración Post-Instalación

### Acceder a Grafana

Si configuró Grafana con LoadBalancer:

```bash
# Obtener IP externa
kubectl get svc -n dfs dfs-grafana

# Acceder en navegador
# http://<EXTERNAL-IP>:3000
# Usuario: admin
# Password: el configurado en values.yaml
```

Si usa port-forward:

```bash
kubectl port-forward -n dfs svc/dfs-grafana 3000:3000

# Acceder en navegador
# http://localhost:3000
```

### Configurar Data Source en Grafana

1. Login en Grafana
2. Ir a Configuration → Data Sources
3. Agregar Prometheus:
   - URL: `http://dfs-prometheus:9090`
   - Access: Server (default)
   - Click "Save & Test"

### Importar Dashboards

Puede crear dashboards personalizados o importar plantillas. Ejemplo de queries útiles:

**Panel de Estado del Cluster:**
```promql
dfs_metadata_nodes_active
dfs_metadata_files_total
sum(dfs_datanode_chunks_stored)
```

**Panel de Uso de Disco:**
```promql
(dfs_datanode_disk_used_bytes / dfs_datanode_disk_total_bytes) * 100
```

**Panel de Throughput:**
```promql
rate(dfs_datanode_bytes_written_total[5m])
rate(dfs_datanode_bytes_read_total[5m])
```

### Configurar Ingress (si está habilitado)

Si habilitó Ingress, verifique que está funcionando:

```bash
# Ver Ingress
kubectl get ingress -n dfs

# Describir Ingress
kubectl describe ingress -n dfs dfs-ingress

# Verificar que cert-manager creó el certificado (si usa TLS)
kubectl get certificate -n dfs
```

Asegúrese de que los DNS apuntan a la IP del Ingress Controller:

```bash
# Obtener IP del Ingress
kubectl get ingress -n dfs

# Verificar DNS
nslookup dfs.example.com
nslookup grafana.example.com
```

## Operaciones

### Escalar DataNodes

Para agregar más capacidad de almacenamiento:

```bash
# Escalar a 7 DataNodes
helm upgrade dfs-prod . \
  --set datanode.replicaCount=7 \
  --namespace dfs \
  --reuse-values

# Verificar que los nuevos pods están corriendo
kubectl get pods -n dfs -l app=dfs-datanode

# Verificar que se registraron en el Metadata Service
kubectl exec -n dfs deployment/dfs-metadata -- \
  curl -s http://localhost:8000/api/v1/nodes | jq 'length'
```

Los nuevos DataNodes comenzarán a recibir chunks automáticamente en las próximas operaciones de upload.

### Actualizar Configuración

Para cambiar configuración sin cambiar imágenes:

```bash
# Editar values.yaml o crear nuevo archivo
vim updated-values.yaml

# Aplicar cambios
helm upgrade dfs-prod . \
  -f updated-values.yaml \
  --namespace dfs
```

### Actualizar Imágenes

Para actualizar a una nueva versión del DFS:

```bash
# Actualizar imágenes
helm upgrade dfs-prod . \
  --set metadata.image.tag=1.1.0 \
  --set datanode.image.tag=1.1.0 \
  --namespace dfs \
  --reuse-values

# Monitorear el rollout
kubectl rollout status deployment/dfs-metadata -n dfs
kubectl rollout status statefulset/dfs-datanode -n dfs
```

Para un rolling update más controlado de los DataNodes:

```bash
# Actualizar uno por uno
for i in 0 1 2; do
  kubectl delete pod dfs-datanode-$i -n dfs
  kubectl wait --for=condition=ready pod/dfs-datanode-$i -n dfs --timeout=300s
  sleep 30
done
```

### Backup de Metadata

Hacer backup de la base de datos de metadata:

```bash
# Identificar el pod del Metadata Service
METADATA_POD=$(kubectl get pod -n dfs -l app=dfs-metadata -o jsonpath='{.items[0].metadata.name}')

# Copiar base de datos
kubectl cp dfs/$METADATA_POD:/tmp/dfs_metadata.db ./backup/metadata_$(date +%Y%m%d).db

# Comprimir
gzip ./backup/metadata_$(date +%Y%m%d).db
```

Para automatizar backups, puede crear un CronJob de Kubernetes:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: dfs-metadata-backup
  namespace: dfs
spec:
  schedule: "0 2 * * *"  # Diariamente a las 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: bitnami/kubectl:latest
            command:
            - /bin/sh
            - -c
            - |
              METADATA_POD=$(kubectl get pod -n dfs -l app=dfs-metadata -o jsonpath='{.items[0].metadata.name}')
              kubectl cp dfs/$METADATA_POD:/tmp/dfs_metadata.db /backups/metadata_$(date +%Y%m%d).db
              gzip /backups/metadata_$(date +%Y%m%d).db
            volumeMounts:
            - name: backup-storage
              mountPath: /backups
          restartPolicy: OnFailure
          volumes:
          - name: backup-storage
            persistentVolumeClaim:
              claimName: backup-pvc
```

### Restaurar desde Backup

```bash
# Detener Metadata Service
kubectl scale deployment dfs-metadata -n dfs --replicas=0

# Esperar a que el pod se detenga
kubectl wait --for=delete pod -l app=dfs-metadata -n dfs --timeout=60s

# Copiar backup al PVC
# Primero, crear un pod temporal para acceder al PVC
kubectl run -n dfs restore-pod --image=busybox --restart=Never --overrides='
{
  "spec": {
    "containers": [{
      "name": "restore",
      "image": "busybox",
      "command": ["sleep", "3600"],
      "volumeMounts": [{
        "name": "metadata-storage",
        "mountPath": "/tmp"
      }]
    }],
    "volumes": [{
      "name": "metadata-storage",
      "persistentVolumeClaim": {
        "claimName": "dfs-metadata-pvc"
      }
    }]
  }
}'

# Copiar backup al pod
kubectl cp ./backup/metadata_20240101.db dfs/restore-pod:/tmp/dfs_metadata.db

# Eliminar pod temporal
kubectl delete pod restore-pod -n dfs

# Reiniciar Metadata Service
kubectl scale deployment dfs-metadata -n dfs --replicas=1

# Verificar que funciona
kubectl logs -n dfs deployment/dfs-metadata -f
```

## Monitoreo y Alertas

### Configurar Alertas en Prometheus

Crear reglas de alertas:

```yaml
# alerts.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-alerts
  namespace: dfs
data:
  alerts.yml: |
    groups:
      - name: dfs_alerts
        interval: 30s
        rules:
          - alert: DFSNodeDown
            expr: dfs_metadata_nodes_active < 3
            for: 1m
            labels:
              severity: critical
            annotations:
              summary: "DFS cluster degradado"
              description: "Solo {{ $value }} nodos activos"

          - alert: DFSDiskSpaceLow
            expr: (dfs_datanode_disk_free_bytes / dfs_datanode_disk_total_bytes) < 0.15
            for: 5m
            labels:
              severity: warning
            annotations:
              summary: "Espacio en disco bajo"
              description: "Nodo {{ $labels.pod }} tiene menos de 15% libre"
```

Aplicar:

```bash
kubectl apply -f alerts.yaml

# Actualizar Prometheus para cargar las alertas
kubectl rollout restart deployment/dfs-prometheus -n dfs
```

### Integrar con Alertmanager

Para enviar alertas por email, Slack, PagerDuty, etc., despliegue Alertmanager:

```bash
# Instalar Alertmanager con Helm
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install alertmanager prometheus-community/alertmanager \
  --namespace dfs \
  --set config.receivers[0].name=default \
  --set config.receivers[0].slack_configs[0].api_url=YOUR_SLACK_WEBHOOK
```

## Troubleshooting

### Pods en Pending

Si los pods están en estado `Pending`:

```bash
# Describir el pod para ver eventos
kubectl describe pod -n dfs <pod-name>

# Causas comunes:
# 1. Recursos insuficientes en nodos
kubectl top nodes

# 2. PVC no puede ser provisionado
kubectl get pvc -n dfs
kubectl describe pvc -n dfs <pvc-name>

# 3. No hay nodos que cumplan los nodeSelector/affinity
kubectl get nodes --show-labels
```

**Solución**: Agregar más nodos al cluster o ajustar los recursos solicitados en values.yaml.

### Pods en CrashLoopBackOff

Si los pods se reinician constantemente:

```bash
# Ver logs del pod
kubectl logs -n dfs <pod-name>

# Ver logs del contenedor anterior (antes del crash)
kubectl logs -n dfs <pod-name> --previous

# Causas comunes:
# 1. Error en la aplicación (ver logs)
# 2. Configuración incorrecta (variables de entorno)
# 3. Falta de recursos (OOMKilled)

# Verificar eventos
kubectl describe pod -n dfs <pod-name>
```

**Solución**: Revisar logs, corregir configuración, o aumentar límites de recursos.

### DataNodes No Se Registran

Si los DataNodes no aparecen en el Metadata Service:

```bash
# Verificar conectividad
kubectl exec -n dfs dfs-datanode-0 -- \
  curl -v http://dfs-metadata:8000/api/v1/health

# Verificar variable de entorno
kubectl exec -n dfs dfs-datanode-0 -- env | grep METADATA_SERVICE_URL

# Ver logs del DataNode
kubectl logs -n dfs dfs-datanode-0 | grep -i heartbeat

# Ver logs del Metadata Service
kubectl logs -n dfs deployment/dfs-metadata | grep -i heartbeat
```

**Solución**: Verificar que la variable `METADATA_SERVICE_URL` es correcta y que el servicio `dfs-metadata` está accesible.

### Problemas de Rendimiento

Si el sistema está lento:

```bash
# Verificar uso de recursos
kubectl top pods -n dfs
kubectl top nodes

# Verificar métricas en Prometheus
# Acceder a Prometheus y ejecutar queries:
# - rate(dfs_metadata_http_request_duration_seconds_sum[5m]) / rate(dfs_metadata_http_request_duration_seconds_count[5m])
# - rate(dfs_datanode_chunk_write_operations_total[5m])

# Verificar I/O de disco
kubectl exec -n dfs dfs-datanode-0 -- iostat -x 1 5
```

**Solución**: Escalar DataNodes, aumentar recursos, o usar StorageClass más rápida (SSD).

## Desinstalación

Para eliminar completamente el DFS del cluster:

```bash
# Desinstalar Helm release
helm uninstall dfs-prod -n dfs

# Eliminar PVCs (ADVERTENCIA: esto elimina todos los datos)
kubectl delete pvc -n dfs --all

# Eliminar namespace
kubectl delete namespace dfs
```

Para mantener los datos pero desinstalar la aplicación:

```bash
# Solo desinstalar Helm release
helm uninstall dfs-prod -n dfs

# Los PVCs permanecen y pueden ser reutilizados en una futura instalación
```

## Mejores Prácticas

### Seguridad

**Usar Secrets**: No almacenar passwords en values.yaml. Usar Kubernetes Secrets:

```bash
kubectl create secret generic grafana-admin \
  --from-literal=password=my-secure-password \
  -n dfs
```

**NetworkPolicy**: Habilitar NetworkPolicy para restringir tráfico:

```yaml
networkPolicy:
  enabled: true
```

**RBAC**: El chart crea RBAC automáticamente. No deshabilitar en producción.

### Alta Disponibilidad

**Múltiples Nodos**: Distribuir pods en múltiples nodos usando anti-affinity:

```yaml
datanode:
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
          - key: app
            operator: In
            values:
            - dfs-datanode
        topologyKey: kubernetes.io/hostname
```

**PodDisruptionBudget**: Ya está habilitado por defecto. Asegura que siempre haya mínimo de pods disponibles durante mantenimiento.

### Backups

**Automatizar Backups**: Usar CronJob para backups automáticos diarios.

**Backup Externo**: Copiar backups fuera del cluster (S3, GCS, etc.).

**Probar Restauración**: Probar el proceso de restauración regularmente.

### Monitoreo

**Alertas Críticas**: Configurar alertas para métricas críticas.

**Retención de Métricas**: Ajustar según necesidades (default: 15 días).

**Dashboards**: Crear dashboards en Grafana para visualización.

---

**Autor**: Manus AI  
**Versión**: 1.0.0  
**Última Actualización**: 2024
