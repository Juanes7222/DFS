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
import { Eye } from "lucide-react";
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

// Devuelve la extensión (formato) del archivo en mayúsculas, o '—' si no tiene extensión
function getFileFormat(path: string): string {

  // Toma la última parte después de '/'
  const base = path.split("/").pop() || path;
  const dotIndex = base.lastIndexOf(".");
  if (dotIndex === -1 || dotIndex === 0) return "—";
  const ext = base.slice(dotIndex + 1);

  // Evita devolver algo raro
  if (!ext || ext.includes("/")) return "—";
  return ext.toUpperCase();
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
  const [previewDialogOpen, setPreviewDialogOpen] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewBlobUrl, setPreviewBlobUrl] = useState<string | null>(null);
  const [previewMime, setPreviewMime] = useState<string | null>(null);
  const [previewText, setPreviewText] = useState<string | null>(null);

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
      loadFiles(); // Metadata siempre se consulta fresca
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
      loadFiles(); // Metadata siempre se consulta fresca
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Error al borrar el archivo");
    }
  }

  function showFileInfo(file: FileMetadata) {
    setSelectedFileInfo(file);
    setInfoDialogOpen(true);
  }

  function getMimeFromExtension(path: string): string | null {
    const ext = getFileFormat(path).toLowerCase();
    if (ext === "—") return null;
    const map: Record<string, string> = {
      png: "image/png",
      jpg: "image/jpeg",
      jpeg: "image/jpeg",
      gif: "image/gif",
      webp: "image/webp",
      svg: "image/svg+xml",
      pdf: "application/pdf",
      txt: "text/plain",
      md: "text/markdown",
      csv: "text/csv",
      json: "application/json",
      mp4: "video/mp4",
      mp3: "audio/mpeg",
    };
    return map[ext] || null;
  }

  async function handlePreview(file: FileMetadata) {
    setSelectedFileInfo(file);
    setPreviewDialogOpen(true);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewBlobUrl(null);
    setPreviewMime(null);
    setPreviewText(null);

    try {
      const blob = await api.downloadFile(file.path, (p) => {
        // Podría mostrar avances si se desea
      });

      const mimeFromBlob = blob.type || getMimeFromExtension(file.path) || "application/octet-stream";
      const url = URL.createObjectURL(blob);
      setPreviewBlobUrl(url);
      setPreviewMime(mimeFromBlob);

      // Si es similar a texto, simplemente lo carga
      if (mimeFromBlob.startsWith("text/") || mimeFromBlob === "application/json" || mimeFromBlob === "text/markdown") {
        try {
          const txt = await blob.text();
          setPreviewText(txt);
        } catch (e) {
          // Ignora el error de análisis del texto
        }
      }
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : "Error al cargar la vista previa");
    } finally {
      setPreviewLoading(false);
    }
  }

  useEffect(() => {
    if (!previewDialogOpen && previewBlobUrl) {
      try {
        URL.revokeObjectURL(previewBlobUrl);
      } catch (e) {
        // Ignorado
      }
      setPreviewBlobUrl(null);
      setPreviewMime(null);
      setPreviewText(null);
      setPreviewError(null);
    }
  }, [previewDialogOpen]);

  return (
    <DashboardLayout>
      <div className="p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
        <div className="mb-6 sm:mb-8 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl sm:text-3xl font-bold text-foreground">Archivos</h1>
            <p className="text-muted-foreground mt-2 text-sm sm:text-base">
              Administra tus archivos distribuidos
            </p>
          </div>
          <Button onClick={() => setUploadDialogOpen(true)} className="w-full sm:w-auto">
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

        {/* Barra de búsqueda */}
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

        {/* Tabla de archivos / Tarjetas */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base sm:text-lg text-foreground">
              Archivos ({filteredFiles.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : filteredFiles.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground text-sm">
                {searchQuery ? "No se encontraron archivos que coincidan con tu búsqueda" : "Aún no se han subido archivos"}
              </div>
            ) : (
              <>
                {/* Desktop Table */}
                <div className="hidden md:block overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Ruta</TableHead>
                        <TableHead>Formato</TableHead>
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
                              <FileText className="h-4 w-4 text-primary flex-shrink-0" />
                              <span className="truncate max-w-xs">{file.path}</span>
                            </div>
                          </TableCell>

                          {/* Formato */}
                          <TableCell>
                            <span className="text-xs font-medium text-muted-foreground">
                              {getFileFormat(file.path)}
                            </span>
                          </TableCell>

                          <TableCell>{formatBytes(file.size)}</TableCell>
                          <TableCell>{file.chunks.length}</TableCell>
                          <TableCell className="whitespace-nowrap">
                            {new Date(file.created_at).toLocaleString()}
                          </TableCell>
                          <TableCell className="text-right">
                            <div className="flex items-center justify-end gap-2">
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => handlePreview(file)}
                              >
                                <Eye className="h-4 w-4" />
                              </Button>
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
                </div>

                {/* Tarjetas */}
                <div className="md:hidden space-y-3">
                  {filteredFiles.map((file) => (
                    <div key={file.file_id} className="p-4 bg-muted rounded-lg space-y-3">
                      <div className="flex items-start gap-2">
                        <FileText className="h-5 w-5 text-primary flex-shrink-0 mt-0.5" />
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-foreground text-sm truncate">{file.path}</p>
                          <p className="text-xs text-muted-foreground mt-1">
                            {formatBytes(file.size)} • {file.chunks.length} chunks
                          </p>
                          <p className="text-xs text-muted-foreground">
                            {new Date(file.created_at).toLocaleDateString()}
                          </p>
                        </div>

                        {/* Mostrar formato en la tarjeta móvil */}
                        <div className="text-xs text-muted-foreground ml-3 flex-shrink-0">
                          {getFileFormat(file.path)}
                        </div>
                      </div>
                      
                      <div className="flex gap-2 pt-2 border-t border-border">
                        <Button
                          variant="outline"
                          size="sm"
                          className="flex-1"
                          onClick={() => handlePreview(file)}
                        >
                          <Eye className="h-4 w-4 mr-2" />
                          Vista
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="flex-1"
                          onClick={() => handleDownload(file)}
                        >
                          <Download className="h-4 w-4 mr-2" />
                          Descargar
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleDelete(file)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Dialog de actualización */}
        <Dialog open={uploadDialogOpen} onOpenChange={setUploadDialogOpen}>
          <DialogContent className="sm:max-w-md">
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

            <DialogFooter className="flex-col sm:flex-row gap-2">
              <Button
                variant="outline"
                onClick={() => setUploadDialogOpen(false)}
                disabled={uploading}
                className="w-full sm:w-auto"
              >
                Cancelar
              </Button>
              <Button 
                onClick={handleUpload} 
                disabled={uploading || !selectedFile || !remotePath}
                className="w-full sm:w-auto"
              >
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

        {/* Dialog de vista previa del archivo */}
        <Dialog open={previewDialogOpen} onOpenChange={setPreviewDialogOpen}>
          <DialogContent className="max-w-full sm:max-w-3xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Vista previa</DialogTitle>
            </DialogHeader>

            <div className="space-y-4 py-4">
              {previewLoading ? (
                <div className="flex flex-col items-center justify-center py-12">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                  <p className="text-sm text-muted-foreground mt-3">Cargando vista previa...</p>
                </div>
              ) : previewError ? (
                <Alert variant="destructive">
                  <AlertDescription>{previewError}</AlertDescription>
                </Alert>
              ) : previewBlobUrl && previewMime ? (
                <div>
                  {previewMime.startsWith("image/") && (
                    <div className="flex items-center justify-center">
                      <img src={previewBlobUrl} alt="preview" className="max-h-[70vh] w-auto max-w-full object-contain" />
                    </div>
                  )}

                  {previewMime === "application/pdf" && (
                    <iframe src={previewBlobUrl} title="pdf-preview" className="w-full h-[80vh] border-0" />
                  )}

                  {previewMime.startsWith("text/") || previewMime === "application/json" || previewText ? (
                    <div className="max-h-[70vh] overflow-auto bg-muted p-4 rounded">
                      <pre className="whitespace-pre-wrap text-sm text-foreground">{previewText}</pre>
                    </div>
                  ) : null}

                  {previewMime.startsWith("video/") && (
                    <video controls src={previewBlobUrl} className="w-full max-h-[70vh]" />
                  )}

                  {previewMime.startsWith("audio/") && (
                    <audio controls src={previewBlobUrl} className="w-full" />
                  )}

                  {/* si no es ninguno de los anteriores */}
                  {!previewMime.startsWith("image/") && previewMime !== "application/pdf" && !previewMime.startsWith("text/") && !previewMime.startsWith("video/") && !previewMime.startsWith("audio/") && (
                    <div className="p-4 bg-muted rounded">
                      <p className="text-sm text-muted-foreground">No es posible visualizar este formato de archivo.</p>
                      <p className="text-sm text-muted-foreground">Descarga el archivo para abrirlo localmente.</p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center text-sm text-muted-foreground">No hay vista previa disponible</div>
              )}
            </div>

            <DialogFooter className="flex-col sm:flex-row gap-2">
              <Button variant="outline" onClick={() => setPreviewDialogOpen(false)} className="w-full sm:w-auto">Cerrar</Button>
              <Button
                onClick={() => selectedFileInfo && handleDownload(selectedFileInfo)}
                className="w-full sm:w-auto"
              >
                Descargar
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Dialog de información del archivo */}
        <Dialog open={infoDialogOpen} onOpenChange={setInfoDialogOpen}>
          <DialogContent className="max-w-full sm:max-w-2xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Información del Archivo</DialogTitle>
            </DialogHeader>

            {selectedFileInfo && (
              <div className="space-y-4 py-4">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Ruta</p>
                    <p className="text-sm text-foreground break-all">{selectedFileInfo.path}</p>
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
                            <div key={ridx} className="text-xs text-muted-foreground break-all">
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
