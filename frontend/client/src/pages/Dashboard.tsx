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

export default function Dashboard() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [files, setFiles] = useState<FileMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 10000); // Refresh every 10s
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
      setError(err instanceof Error ? err.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  }

  const activeNodes = nodes.filter(n => n.state === "active");
  const totalStorage = nodes.reduce((sum, n) => sum + n.total_space, 0);
  const usedStorage = nodes.reduce((sum, n) => sum + (n.total_space - n.free_space), 0);
  const totalChunks = nodes.reduce((sum, n) => sum + n.chunk_count, 0);

  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-foreground">Panel de Control</h1>
          <p className="text-muted-foreground mt-2">
            Descripción general del sistema de archivos distribuidos
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
                    <CheckCircle2 className="h-6 w-6 text-accent" />
                    <div>
                      <p className="font-semibold text-foreground">Sistema en Buen Estado</p>
                      <p className="text-sm text-muted-foreground">
                        Todos los nodos están operativos.
                      </p>
                    </div>
                  </>
                ) : (
                  <>
                    <AlertCircle className="h-6 w-6 text-destructive" />
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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {/* Total Nodes */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Nodos Totales
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">{nodes.length}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {activeNodes.length} activo
              </p>
            </CardContent>
          </Card>

          {/* Archivos Totales */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Archivos Totales
              </CardTitle>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">{files.length}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {totalChunks} chunks
              </p>
            </CardContent>
          </Card>

          {/* Almacenamiento Usado */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Almacenamiento Usado
              </CardTitle>
              <Database className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">
                {formatBytes(usedStorage)}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                de {formatBytes(totalStorage)}
              </p>
            </CardContent>
          </Card>

          {/* Factor de Replicación */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Replicación
              </CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">
                {health?.details?.replication_factor || 3}x
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                Factor de redundancia
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Resumen de los Nodos */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Nodos Activos */}
          <Card>
            <CardHeader>
              <CardTitle className="text-foreground">Nodos Activos</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <p className="text-muted-foreground">Cargando...</p>
              ) : activeNodes.length === 0 ? (
                <p className="text-muted-foreground">No hay nodos activos</p>
              ) : (
                <div className="space-y-3">
                  {activeNodes.slice(0, 5).map((node) => (
                    <div
                      key={node.node_id}
                      className="flex items-center justify-between p-3 bg-muted rounded-lg"
                    >
                      <div className="flex items-center gap-3">
                        <HardDrive className="h-5 w-5 text-primary" />
                        <div>
                          <p className="font-medium text-foreground">{node.node_id}</p>
                          <p className="text-xs text-muted-foreground">
                            {node.host}:{node.port}
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-sm font-medium text-foreground">
                          {formatBytes(node.free_space)}
                        </p>
                        <p className="text-xs text-muted-foreground">disponible</p>
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
              <CardTitle className="text-foreground">Archivos Recientes</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <p className="text-muted-foreground">Cargando...</p>
              ) : files.length === 0 ? (
                <p className="text-muted-foreground">Aún no se han subido archivos</p>
              ) : (
                <div className="space-y-3">
                  {files.slice(0, 5).map((file) => (
                    <div
                      key={file.file_id}
                      className="flex items-center justify-between p-3 bg-muted rounded-lg"
                    >
                      <div className="flex items-center gap-3">
                        <FileText className="h-5 w-5 text-primary" />
                        <div>
                          <p className="font-medium text-foreground">{file.path}</p>
                          <p className="text-xs text-muted-foreground">
                            {file.chunks.length} chunks
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-sm font-medium text-foreground">
                          {formatBytes(file.size)}
                        </p>
                        <p className="text-xs text-muted-foreground">
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
