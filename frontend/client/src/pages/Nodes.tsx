import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { HardDrive, AlertCircle, Loader2, CheckCircle2, XCircle } from "lucide-react";
import { api, NodeInfo } from "@/lib/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import DashboardLayout from "@/components/DashboardLayout";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

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

function getNodeStatusColor(state: string): "default" | "secondary" | "destructive" {
  switch (state) {
    case "active":
      return "default";
    case "inactive":
      return "secondary";
    default:
      return "destructive";
  }
}

function getNodeStatusIcon(state: string) {
  switch (state) {
    case "active":
      return <CheckCircle2 className="h-4 w-4" />;
    case "inactive":
      return <XCircle className="h-4 w-4" />;
    default:
      return <AlertCircle className="h-4 w-4" />;
  }
}

const translateNodeState = (state: string) => {
  switch (state) {
    case "active":
      return "Activo";
    case "inactive":
      return "Inactivo";
    case "pending":
      return "Pendiente";
    case "error":
      return "Error";
    default:
      return state; // fallback por si aparece algún estado nuevo
  }
};

export default function Nodes() {
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadNodes();
    const interval = setInterval(loadNodes, 10000); // Refresh every 10s
    return () => clearInterval(interval);
  }, []);

  async function loadNodes() {
    try {
      const data = await api.listNodes();
      setNodes(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error al cargar los nodos");
    } finally {
      setLoading(false);
    }
  }

  const activeNodes = nodes.filter(n => n.state === "active");

  /*
   Agrupamos los nodos por host para evitar duplicar el almacenamiento.
   Se selecciona un "representante" por cada host:
    - Solo se consideran los nodo activo (el más reciente por last_heartbeat)
    - si no hay nodos activos en el host, se usa el nodo con last_heartbeat más reciente
   
    Solo incluyen hosts que tengan al menos un nodo activo, esto evita contar hosts totalmente inactivos.
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
    // Cuenta los hosts únicos (Por si acaso)
    const uniqueHostsCount = groups.size;

    for (const [host, group] of groups.entries()) {
      // Filtra los nodos activos del host
      const activeInHost = group.filter(g => g.state === "active");

      // Si no hay nodos activos en ese host, lo ignoramos.
      if (activeInHost.length === 0) continue;

      // Elegimos el representante del host: el activo con last_heartbeat más reciente
      const rep = activeInHost.reduce((a, b) => {
        return new Date(a.last_heartbeat) > new Date(b.last_heartbeat) ? a : b;
      });

      /* Sumamos una sola vez por host con los valores del representante
        asumiendo que total_space/free_space son del host físico compartido. */
      total += rep.total_space;
      used += (rep.total_space - rep.free_space);
    }

    return { total, used, uniqueHostsCount };
  }

  const { total: totalStorage, used: usedStorage } = computeStorageByHost(nodes);
  const storageUsagePercent = totalStorage > 0 ? (usedStorage / totalStorage) * 100 : 0;

  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-foreground">Nodos</h1>
          <p className="text-muted-foreground mt-2">
            Nodos de almacenamiento en su clúster
          </p>
        </div>

        {error && (
          <Alert variant="destructive" className="mb-6">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Stats */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
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
                {activeNodes.length} activo(s)
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Almacenamiento Total
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">
                {formatBytes(totalStorage)}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {formatBytes(usedStorage)} usados
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Uso del Almacenamiento
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">
                {storageUsagePercent.toFixed(1)}%
              </div>
              <div className="w-full bg-muted rounded-full h-2 mt-2">
                <div
                  className="bg-primary h-2 rounded-full transition-all"
                  style={{ width: `${storageUsagePercent}%` }}
                />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Nodes Table */}
        <Card>
          <CardHeader>
            <CardTitle className="text-foreground">Nodos: ({nodes.length})</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : nodes.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                No hay nodos registrados
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID del Nodo</TableHead>
                    <TableHead>Host</TableHead>
                    <TableHead>Puerto</TableHead>
                    <TableHead>Estado</TableHead>
                    <TableHead>Espacio Disponible</TableHead>
                    <TableHead>Espacio Total</TableHead>
                    <TableHead>Chunks</TableHead>
                    <TableHead>Última Comunicación</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {nodes.map((node) => {
                    const usagePercent = node.total_space > 0
                      ? ((node.total_space - node.free_space) / node.total_space) * 100
                      : 0;

                    return (
                      <TableRow key={node.node_id}>
                        <TableCell className="font-medium">
                          <div className="flex items-center gap-2">
                            <HardDrive className="h-4 w-4 text-primary" />
                            {node.node_id}
                          </div>
                        </TableCell>
                        <TableCell>{node.host}</TableCell>
                        <TableCell>{node.port}</TableCell>
                        <TableCell>
                          <Badge variant={getNodeStatusColor(node.state)}>
                            <span className="flex items-center gap-1">
                              {getNodeStatusIcon(node.state)}
                              {translateNodeState(node.state)}
                            </span>
                          </Badge>
                        </TableCell>
                        <TableCell>{formatBytes(node.free_space)}</TableCell>
                        <TableCell>
                          <div>
                            <div className="text-sm">{formatBytes(node.total_space)}</div>
                            <div className="w-24 bg-muted rounded-full h-1.5 mt-1">
                              <div
                                className="bg-primary h-1.5 rounded-full transition-all"
                                style={{ width: `${usagePercent}%` }}
                              />
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>{node.chunk_count}</TableCell>
                        <TableCell>
                          {new Date(node.last_heartbeat).toLocaleString()}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
