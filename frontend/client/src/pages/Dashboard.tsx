import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Activity, Database, HardDrive, FileText, AlertCircle, CheckCircle2 } from "lucide-react";
import { api, HealthResponse, NodeInfo, FileMetadata } from "@/lib/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import DashboardLayout from "@/components/DashboardLayout";

function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;
  
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }
  
  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

  /*
  Agrupa los nodos por host, devolviendo el espacio total y usado por host único.
  Solo incluye hosts que tengan al menos un nodo 'active'.
  */
function computeStorageByHost(nodesList: NodeInfo[]) {
  const groups = new Map<string, NodeInfo[]>();
  for (const n of nodesList) {
    const host = n.host ?? "unknown";
    if (!groups.has(host)) groups.set(host, []);
    groups.get(host)!.push(n);
  }

  let total = 0;
  let used = 0;

  for (const [, group] of Array.from(groups.entries())) {
    // Filtrar nodos activos del host
    const activeInHost = group.filter(g => g.state === "active");

    // Si no hay nodos activos en ese host, lo ignoramos
    if (activeInHost.length === 0) continue;

    // Elegimos representante: el activo con last_heartbeat más reciente
    const rep = activeInHost.reduce((a, b) => {
      return new Date(a.last_heartbeat) > new Date(b.last_heartbeat) ? a : b;
    });

    total += rep.total_space;
    used += (rep.total_space - rep.free_space);
  }

  return { total, used };
}

export default function Dashboard() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [files, setFiles] = useState<FileMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, []);

  async function loadData() {
    try {
      const [healthData, nodesData, filesData] = await Promise.all([
        api.getHealth(),
        api.listNodes(),
        api.listFiles(),
      ]);
      
      setHealth(healthData);
      setNodes(nodesData);
      setFiles(filesData);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error al cargar los datos");
    } finally {
      setLoading(false);
    }
  }

  const activeNodes = nodes.filter(n => n.state === "active");

  // Usamos la función que agrupa por host para calcular almacenamiento real
  const { total: totalStorage, used: usedStorage } = computeStorageByHost(nodes);

  // Si quieres mantener totalChunks como suma por nodo, lo dejamos así.
  const totalChunks = nodes.reduce((sum, n) => sum + n.chunk_count, 0);

  return (
    <DashboardLayout>
      <div className="p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
        <div className="mb-6 sm:mb-8">
          <h1 className="text-2xl sm:text-3xl font-bold text-foreground">Panel de Control</h1>
          <p className="text-muted-foreground mt-2 text-sm sm:text-base">
            Resumen del sistema de archivos distribuidos
          </p>
        </div>

        {error && (
          <Alert variant="destructive" className="mb-6">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Estado del Sistema */}
        <div className="mb-6">
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                {health?.status === "healthy" ? (
                  <>
                    <CheckCircle2 className="h-6 w-6 text-accent flex-shrink-0" />
                    <div>
                      <p className="font-semibold text-foreground">Sistema en Buen Estado</p>
                      <p className="text-sm text-muted-foreground">
                        Todos los nodos están operativos.
                      </p>
                    </div>
                  </>
                ) : (
                  <>
                    <AlertCircle className="h-6 w-6 text-destructive flex-shrink-0" />
                    <div>
                      <p className="font-semibold text-foreground">Sistema Degradado</p>
                      <p className="text-sm text-muted-foreground">
                        Algunos nodos pueden no estar disponibles.
                      </p>
                    </div>
                  </>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Cuadro de Estadísticas */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 lg:gap-6 mb-6 sm:mb-8">
          {/* Total Nodes */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
              <CardTitle className="text-xs sm:text-sm font-medium text-muted-foreground">
                Nodos
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-xl sm:text-2xl font-bold text-foreground">{nodes.length}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {activeNodes.length} activo(s)
              </p>
            </CardContent>
          </Card>

          {/* Archivos Totales */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
              <CardTitle className="text-xs sm:text-sm font-medium text-muted-foreground">
                Archivos
              </CardTitle>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-xl sm:text-2xl font-bold text-foreground">{files.length}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {totalChunks} chunks
              </p>
            </CardContent>
          </Card>

          {/* Almacenamiento Usado (ahora por host, sin duplicados) */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
              <CardTitle className="text-xs sm:text-sm font-medium text-muted-foreground">
                Usado
              </CardTitle>
              <Database className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-xl sm:text-2xl font-bold text-foreground">
                {formatBytes(usedStorage)}
              </div>
              <p className="text-xs text-muted-foreground mt-1 truncate">
                de {formatBytes(totalStorage)}
              </p>
            </CardContent>
          </Card>

          {/* Factor de Replicación */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
              <CardTitle className="text-xs sm:text-sm font-medium text-muted-foreground">
                Replicación
              </CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-xl sm:text-2xl font-bold text-foreground">
                {health?.details?.replication_factor || 3}x
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                Factor
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Resumen de los Nodos */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
          {/* Nodos Activos */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base sm:text-lg text-foreground">Nodos Activos</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <p className="text-muted-foreground text-sm">Cargando...</p>
              ) : activeNodes.length === 0 ? (
                <p className="text-muted-foreground text-sm">No hay nodos activos</p>
              ) : (
                <div className="space-y-3">
                  {activeNodes.slice(0, 5).map((node) => (
                    <div
                      key={node.node_id}
                      className="flex items-center justify-between p-3 bg-muted rounded-lg gap-2"
                    >
                      <div className="flex items-center gap-2 sm:gap-3 min-w-0 flex-1">
                        <HardDrive className="h-4 w-4 sm:h-5 sm:w-5 text-primary flex-shrink-0" />
                        <div className="min-w-0 flex-1">
                          <p className="font-medium text-foreground text-sm truncate">{node.node_id.slice(0, 8)}...</p>
                          <p className="text-xs text-muted-foreground truncate">
                            {node.host}:{node.port}
                          </p>
                        </div>
                      </div>
                      <div className="text-right flex-shrink-0">
                        <p className="text-xs sm:text-sm font-medium text-foreground">
                          {formatBytes(node.free_space)}
                        </p>
                        <p className="text-xs text-muted-foreground">libre</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Archivos Recientes */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base sm:text-lg text-foreground">Archivos Recientes</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <p className="text-muted-foreground text-sm">Cargando...</p>
              ) : files.length === 0 ? (
                <p className="text-muted-foreground text-sm">Aún no se han subido archivos</p>
              ) : (
                <div className="space-y-3">
                  {files.slice(0, 5).map((file) => (
                    <div
                      key={file.file_id}
                      className="flex items-center justify-between p-3 bg-muted rounded-lg gap-2"
                    >
                      <div className="flex items-center gap-2 sm:gap-3 min-w-0 flex-1">
                        <FileText className="h-4 w-4 sm:h-5 sm:w-5 text-primary flex-shrink-0" />
                        <div className="min-w-0 flex-1">
                          <p className="font-medium text-foreground text-sm truncate">{file.path}</p>
                          <p className="text-xs text-muted-foreground">
                            {file.chunks.length} chunks
                          </p>
                        </div>
                      </div>
                      <div className="text-right flex-shrink-0">
                        <p className="text-xs sm:text-sm font-medium text-foreground">
                          {formatBytes(file.size)}
                        </p>
                        <p className="text-xs text-muted-foreground hidden sm:block">
                          {new Date(file.created_at).toLocaleDateString()}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  );
}
