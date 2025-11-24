import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { 
  FileText, 
  Upload, 
  Download, 
  Trash2, 
  Search,
  AlertCircle,
  Loader2,
  Info
} from "lucide-react";
import { api, FileMetadata } from "@/lib/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import DashboardLayout from "@/components/DashboardLayout";
import { toast } from "sonner";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

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

export default function Files() {
  const [files, setFiles] = useState<FileMetadata[]>([]);
  const [filteredFiles, setFilteredFiles] = useState<FileMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [uploadDialogOpen, setUploadDialogOpen] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [remotePath, setRemotePath] = useState("");
  const [selectedFileInfo, setSelectedFileInfo] = useState<FileMetadata | null>(null);
  const [infoDialogOpen, setInfoDialogOpen] = useState(false);

  useEffect(() => {
    loadFiles();
  }, []);

  useEffect(() => {
    if (searchQuery) {
      setFilteredFiles(
        files.filter(f => 
          f.path.toLowerCase().includes(searchQuery.toLowerCase())
        )
      );
    } else {
      setFilteredFiles(files);
    }
  }, [searchQuery, files]);

  async function loadFiles() {
    try {
      const data = await api.listFiles();
      setFiles(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error al cargar los archivos");
    } finally {
      setLoading(false);
    }
  }

  async function handleUpload() {
    if (!selectedFile || !remotePath) {
      toast.error("Selecciona un archivo e ingresa una ruta remota");
      return;
    }

    setUploading(true);
    setUploadProgress(0);

    try {
      await api.uploadFile(selectedFile, remotePath, (progress) => {
        setUploadProgress(progress);
      });

      toast.success("Archivo subido correctamente");
      setUploadDialogOpen(false);
      setSelectedFile(null);
      setRemotePath("");
      setUploadProgress(0);
      loadFiles();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Error al subir el archivo");
    } finally {
      setUploading(false);
    }
  }

  async function handleDownload(file: FileMetadata) {
    try {
      toast.info("Descargando archivo...");
      
      const blob = await api.downloadFile(file.path, (progress) => {
        // Could show progress in toast
      });

      // Trigger download
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.path.split("/").pop() || "download";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      toast.success("Archivo descargado correctamente");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Error al descargar el archivo");
    }
  }

  async function handleDelete(file: FileMetadata) {
    if (!confirm(`¿Deseas eliminar el archivo en la ruta ${file.path}?`)) return;

    try {
      await api.deleteFile(file.path, false);
      toast.success("Archivo eliminado");
      loadFiles();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Error al borrar el archivo");
    }
  }

  function showFileInfo(file: FileMetadata) {
    setSelectedFileInfo(file);
    setInfoDialogOpen(true);
  }

  return (
    <DashboardLayout>
      <div className="p-8">
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">Archivos</h1>
            <p className="text-muted-foreground mt-2">
              Administra tus archivos distribuidos
            </p>
          </div>
          <Button onClick={() => setUploadDialogOpen(true)}>
            <Upload className="h-4 w-4 mr-2" />
            Subir Archivo
          </Button>
        </div>

        {error && (
          <Alert variant="destructive" className="mb-6">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Search */}
        <Card className="mb-6">
          <CardContent className="pt-6">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar archivo por nombre..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
          </CardContent>
        </Card>

        {/* Files Table */}
        <Card>
          <CardHeader>
            <CardTitle className="text-foreground">
              Archivos ({filteredFiles.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : filteredFiles.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                {searchQuery ? "No se encontraron archivos que coincidan con tu búsqueda" : "Aún no se han subido archivos"}
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Ruta</TableHead>
                    <TableHead>Tamaño</TableHead>
                    <TableHead>Chunks</TableHead>
                    <TableHead>Creado</TableHead>
                    <TableHead className="text-right">Acciones</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredFiles.map((file) => (
                    <TableRow key={file.file_id}>
                      <TableCell className="font-medium">
                        <div className="flex items-center gap-2">
                          <FileText className="h-4 w-4 text-primary" />
                          {file.path}
                        </div>
                      </TableCell>
                      <TableCell>{formatBytes(file.size)}</TableCell>
                      <TableCell>{file.chunks.length}</TableCell>
                      <TableCell>
                        {new Date(file.created_at).toLocaleString()}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => showFileInfo(file)}
                          >
                            <Info className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDownload(file)}
                          >
                            <Download className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDelete(file)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        {/* Upload Dialog */}
        <Dialog open={uploadDialogOpen} onOpenChange={setUploadDialogOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Subir Archivo</DialogTitle>
              <DialogDescription>
                Seleccione un archivo y especifica la ruta remota.
              </DialogDescription>
            </DialogHeader>

            <div className="space-y-4 py-4">
              <div>
                <label className="text-sm font-medium text-foreground mb-2 block">
                  Archivo Local
                </label>
                <Input
                  type="file"
                  onChange={(e) => {
                    setSelectedFile(e.target.files?.[0] || null)
                    if (e.target.files?.[0]) {
                      setRemotePath(e.target.files[0].name)
                    }
                  }}
                  disabled={uploading}
                />
              </div>

              <div>
                <label className="text-sm font-medium text-foreground mb-2 block">
                  Ruta Remota
                </label>
                <Input
                  placeholder="/path/to/file.txt"
                  value={remotePath}
                  onChange={(e) => setRemotePath(e.target.value)}
                  disabled={uploading}
                />
              </div>

              {uploading && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">Subiendo...</span>
                    <span className="text-foreground font-medium">
                      {uploadProgress.toFixed(0)}%
                    </span>
                  </div>
                  <div className="w-full bg-muted rounded-full h-2">
                    <div
                      className="bg-primary h-2 rounded-full transition-all"
                      style={{ width: `${uploadProgress}%` }}
                    />
                  </div>
                </div>
              )}
            </div>

            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => setUploadDialogOpen(false)}
                disabled={uploading}
              >
                Cancelar
              </Button>
              <Button onClick={handleUpload} disabled={uploading || !selectedFile || !remotePath}>
                {uploading ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Subiendo...
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4 mr-2" />
                    Subir
                  </>
                )}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* File Info Dialog */}
        <Dialog open={infoDialogOpen} onOpenChange={setInfoDialogOpen}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>Información del Archivo</DialogTitle>
            </DialogHeader>

            {selectedFileInfo && (
              <div className="space-y-4 py-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Ruta</p>
                    <p className="text-sm text-foreground">{selectedFileInfo.path}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Tamaño</p>
                    <p className="text-sm text-foreground">{formatBytes(selectedFileInfo.size)}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Creado</p>
                    <p className="text-sm text-foreground">
                      {new Date(selectedFileInfo.created_at).toLocaleString()}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Chunks</p>
                    <p className="text-sm text-foreground">{selectedFileInfo.chunks.length}</p>
                  </div>
                </div>

                <div>
                  <p className="text-sm font-medium text-muted-foreground mb-2">Chunks y Réplicas</p>
                  <div className="space-y-2 max-h-60 overflow-y-auto">
                    {selectedFileInfo.chunks.map((chunk, idx) => (
                      <div key={chunk.chunk_id} className="p-3 bg-muted rounded-lg">
                        <p className="text-sm font-medium text-foreground">
                          Chunk {idx} ({formatBytes(chunk.size)})
                        </p>
                        <p className="text-xs text-muted-foreground mt-1">
                          {chunk.replicas.length} réplicas
                        </p>
                        <div className="mt-2 space-y-1">
                          {chunk.replicas.map((replica, ridx) => (
                            <div key={ridx} className="text-xs text-muted-foreground">
                              • {replica.url} ({replica.state})
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  );
}
