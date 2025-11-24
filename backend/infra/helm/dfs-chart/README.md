# DFS Helm Chart

Helm chart para desplegar el Sistema de Archivos Distribuido (DFS) en Kubernetes.

## Prerrequisitos

- Kubernetes 1.20+
- Helm 3.0+
- PersistentVolume provisioner configurado en el cluster
- Al menos 3 nodos worker con suficiente almacenamiento

## Instalación

### Instalación Rápida

```bash
# Agregar repositorio (si está publicado)
helm repo add dfs https://charts.dfs.example.com
helm repo update

# Instalar con valores por defecto
helm install my-dfs dfs/dfs --namespace dfs --create-namespace
```

### Instalación desde Código Fuente

```bash
# Clonar repositorio
git clone https://github.com/your-org/dfs-system.git
cd dfs-system/infra/helm/dfs-chart

# Instalar
helm install my-dfs . --namespace dfs --create-namespace
```

### Instalación con Valores Personalizados

```bash
# Crear archivo de valores personalizados
cat > custom-values.yaml <<EOF
datanode:
  replicaCount: 5
  persistence:
    size: 500Gi
    storageClass: fast-ssd

metadata:
  resources:
    requests:
      memory: "2Gi"
      cpu: "2000m"

grafana:
  service:
    type: LoadBalancer
  adminPassword: "my-secure-password"
EOF

# Instalar con valores personalizados
helm install my-dfs . -f custom-values.yaml --namespace dfs --create-namespace
```

## Configuración

### Parámetros Principales

| Parámetro | Descripción | Valor por Defecto |
|-----------|-------------|-------------------|
| `global.namespace` | Namespace de Kubernetes | `dfs` |
| `metadata.replicaCount` | Número de réplicas del Metadata Service | `1` |
| `metadata.persistence.size` | Tamaño del PVC para metadata | `10Gi` |
| `datanode.replicaCount` | Número de DataNodes | `3` |
| `datanode.persistence.size` | Tamaño del PVC por DataNode | `100Gi` |
| `prometheus.enabled` | Habilitar Prometheus | `true` |
| `grafana.enabled` | Habilitar Grafana | `true` |
| `grafana.adminPassword` | Password de admin de Grafana | `admin` |

### Configuración de Recursos

```yaml
metadata:
  resources:
    requests:
      memory: "512Mi"
      cpu: "500m"
    limits:
      memory: "1Gi"
      cpu: "1000m"

datanode:
  resources:
    requests:
      memory: "1Gi"
      cpu: "1000m"
    limits:
      memory: "2Gi"
      cpu: "2000m"
```

### Configuración de Persistencia

```yaml
metadata:
  persistence:
    enabled: true
    size: 10Gi
    storageClass: "fast-ssd"  # Usar StorageClass específica
    accessMode: ReadWriteOnce

datanode:
  persistence:
    enabled: true
    size: 100Gi
    storageClass: "standard"
    accessMode: ReadWriteOnce
```

### Configuración de Ingress

```yaml
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
  tls:
    - secretName: dfs-tls
      hosts:
        - dfs.example.com
```

## Operaciones

### Verificar Estado del Despliegue

```bash
# Ver pods
kubectl get pods -n dfs

# Ver servicios
kubectl get svc -n dfs

# Ver PVCs
kubectl get pvc -n dfs

# Ver logs del Metadata Service
kubectl logs -n dfs deployment/dfs-metadata -f

# Ver logs de un DataNode
kubectl logs -n dfs dfs-datanode-0 -f
```

### Escalar DataNodes

```bash
# Escalar a 5 DataNodes
helm upgrade my-dfs . --set datanode.replicaCount=5 --namespace dfs

# O editar values.yaml y aplicar
helm upgrade my-dfs . -f values.yaml --namespace dfs
```

### Actualizar a Nueva Versión

```bash
# Actualizar imágenes
helm upgrade my-dfs . \
  --set metadata.image.tag=1.1.0 \
  --set datanode.image.tag=1.1.0 \
  --namespace dfs

# O con archivo de valores
helm upgrade my-dfs . -f new-values.yaml --namespace dfs
```

### Hacer Rollback

```bash
# Ver historial de releases
helm history my-dfs --namespace dfs

# Rollback a versión anterior
helm rollback my-dfs 1 --namespace dfs
```

### Desinstalar

```bash
# Desinstalar el chart
helm uninstall my-dfs --namespace dfs

# Eliminar PVCs (opcional, si quieres eliminar datos)
kubectl delete pvc -n dfs --all

# Eliminar namespace
kubectl delete namespace dfs
```

## Acceso a Servicios

### Metadata Service API

```bash
# Port-forward para acceso local
kubectl port-forward -n dfs svc/dfs-metadata 8000:8000

# Acceder a la API
curl http://localhost:8000/api/v1/health
```

### Prometheus

```bash
# Port-forward
kubectl port-forward -n dfs svc/dfs-prometheus 9090:9090

# Acceder en navegador
open http://localhost:9090
```

### Grafana

```bash
# Port-forward
kubectl port-forward -n dfs svc/dfs-grafana 3000:3000

# Acceder en navegador
open http://localhost:3000

# Credenciales por defecto: admin / admin
```

### Acceso Externo

Si configuraste `service.type: LoadBalancer`:

```bash
# Obtener IP externa
kubectl get svc -n dfs dfs-grafana

# Acceder usando la EXTERNAL-IP
```

## Monitoreo

### Métricas de Prometheus

Las métricas están disponibles en:
- Metadata Service: `http://dfs-metadata:8000/metrics`
- DataNodes: `http://dfs-datanode-{0,1,2}:8001/metrics`

### Queries Útiles

```promql
# Número de nodos activos
dfs_metadata_nodes_active

# Uso de disco por DataNode
dfs_datanode_disk_used_bytes / dfs_datanode_disk_total_bytes

# Throughput de escritura
rate(dfs_datanode_bytes_written_total[5m])

# Latencia p99 de requests
histogram_quantile(0.99, rate(dfs_metadata_http_request_duration_seconds_bucket[5m]))
```

## Troubleshooting

### Pods en CrashLoopBackOff

```bash
# Ver logs del pod
kubectl logs -n dfs <pod-name>

# Describir el pod para ver eventos
kubectl describe pod -n dfs <pod-name>

# Verificar recursos disponibles
kubectl top nodes
kubectl top pods -n dfs
```

### PVC Pendiente

```bash
# Ver estado del PVC
kubectl describe pvc -n dfs <pvc-name>

# Verificar StorageClass
kubectl get storageclass

# Verificar PersistentVolumes disponibles
kubectl get pv
```

### DataNodes No Se Registran

```bash
# Verificar conectividad con Metadata Service
kubectl exec -n dfs dfs-datanode-0 -- curl http://dfs-metadata:8000/api/v1/health

# Verificar logs del DataNode
kubectl logs -n dfs dfs-datanode-0

# Verificar variable de entorno
kubectl exec -n dfs dfs-datanode-0 -- env | grep METADATA_SERVICE_URL
```

## Alta Disponibilidad

Para configurar HA del Metadata Service:

1. Migrar de SQLite a etcd
2. Aumentar `metadata.replicaCount` a 3
3. Configurar etcd cluster
4. Actualizar código del Metadata Service para usar etcd

Ejemplo:

```yaml
metadata:
  replicaCount: 3
  env:
    - name: BACKEND
      value: "etcd"
    - name: ETCD_ENDPOINTS
      value: "http://etcd-0:2379,http://etcd-1:2379,http://etcd-2:2379"
```

## Seguridad

### Secrets

Para usar Secrets de Kubernetes en lugar de valores en plain text:

```bash
# Crear secret para Grafana password
kubectl create secret generic grafana-admin-password \
  --from-literal=password=my-secure-password \
  -n dfs

# Actualizar values.yaml
grafana:
  adminPasswordSecret: grafana-admin-password
```

### NetworkPolicy

Para habilitar NetworkPolicy:

```yaml
networkPolicy:
  enabled: true
  policyTypes:
    - Ingress
    - Egress
```

### RBAC

El chart crea automáticamente ServiceAccount y RBAC para Prometheus. Para componentes adicionales, agregar en `templates/`.

## Soporte

Para issues, preguntas o contribuciones:
- GitHub Issues: https://github.com/your-org/dfs-system/issues
- Documentación: https://dfs.example.com/docs

---

**Mantenedor**: Manus AI  
**Versión**: 1.0.0
