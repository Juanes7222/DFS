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
      setError(err instanceof Error ? err.message : "Failed to load nodes");
    } finally {
      setLoading(false);
    }
  }

  const activeNodes = nodes.filter(n => n.state === "active");
  const totalStorage = nodes.reduce((sum, n) => sum + n.total_space, 0);
  const usedStorage = nodes.reduce((sum, n) => sum + (n.total_space - n.free_space), 0);
  const storageUsagePercent = totalStorage > 0 ? (usedStorage / totalStorage) * 100 : 0;

  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-foreground">Nodes</h1>
          <p className="text-muted-foreground mt-2">
            Storage nodes in your cluster
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
                Total Nodes
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">{nodes.length}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {activeNodes.length} active
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Total Storage
              </CardTitle>
              <HardDrive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-foreground">
                {formatBytes(totalStorage)}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {formatBytes(usedStorage)} used
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Storage Usage
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
            <CardTitle className="text-foreground">Nodes ({nodes.length})</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : nodes.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                No nodes registered
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Node ID</TableHead>
                    <TableHead>Host</TableHead>
                    <TableHead>Port</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Free Space</TableHead>
                    <TableHead>Total Space</TableHead>
                    <TableHead>Chunks</TableHead>
                    <TableHead>Last Heartbeat</TableHead>
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
                              {node.state}
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
