/**
 * API Service para comunicación con el Metadata Service del DFS
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY = 1000; // 1 segundo

// Timeout configuration
const FETCH_TIMEOUT = 120000; // 2 minutos por chunk (para redes lentas/VPN)

// Cache configuration
// IMPORTANTE: Solo se cachean los BLOBS (archivos descargados) para previews rápidos
// La METADATA (FileMetadata) NUNCA se cachea - siempre se consulta fresca
// Razón: La metadata cambia constantemente (heartbeats, réplicas, replicación)
const CACHE_VERSION = "v1";
const CACHE_TTL = 8 * 60 * 1000; // 8 minutos
const CACHE_KEY_PREFIX = `dfs_cache_${CACHE_VERSION}_`;

// Compression configuration
const COMPRESSION_ENABLED = true;
const COMPRESSIBLE_EXTENSIONS = [
  'txt', 'csv', 'json', 'xml', 'html', 'css', 'js', 'ts', 'jsx', 'tsx',
  'md', 'log', 'yaml', 'yml', 'sql', 'py', 'java', 'c', 'cpp', 'h',
  'go', 'rs', 'rb', 'php', 'sh', 'bat', 'ps1', 'svg', 'ini', 'conf',
  'properties', 'toml', 'gradle', 'cmake', 'dockerfile'
];

// Streaming configuration - archivos que se benefician de carga progresiva
const STREAMABLE_EXTENSIONS = [
  'mp4', 'webm', 'mkv', 'avi', 'mov', // Videos
  'mp3', 'wav', 'ogg', 'flac', 'm4a', // Audio
  'pdf', // PDFs grandes
];

// Tamaño mínimo para considerar streaming (10MB)
const MIN_SIZE_FOR_STREAMING = 10 * 1024 * 1024;

// Tamaño máximo para cachear en memoria (50MB)
// Archivos más grandes no se cachean para evitar problemas de memoria
const MAX_CACHE_SIZE = 50 * 1024 * 1024;

function shouldCompress(filename: string): boolean {
  if (!COMPRESSION_ENABLED) return false;
  
  const ext = filename.split('.').pop()?.toLowerCase();
  return ext ? COMPRESSIBLE_EXTENSIONS.includes(ext) : false;
}

function shouldStream(filename: string, fileSize: number): boolean {
  const ext = filename.split('.').pop()?.toLowerCase();
  return (ext && STREAMABLE_EXTENSIONS.includes(ext) && fileSize >= MIN_SIZE_FOR_STREAMING) || false;
}

async function compressBlob(blob: Blob): Promise<Blob> {
  const stream = blob.stream();
  const compressedStream = stream.pipeThrough(
    new CompressionStream('gzip')
  );
  return new Response(compressedStream).blob();
}

async function decompressBlob(blob: Blob): Promise<Blob> {
  const stream = blob.stream();
  const decompressedStream = stream.pipeThrough(
    new DecompressionStream('gzip')
  );
  return new Response(decompressedStream).blob();
}
interface CacheEntry<T> {
  data: T;
  timestamp: number;
}

class MetadataCache {
  private cache = new Map<string, CacheEntry<any>>();

  set<T>(key: string, data: T): void {
    this.cache.set(CACHE_KEY_PREFIX + key, {
      data,
      timestamp: Date.now(),
    });
  }

  get<T>(key: string): T | null {
    const entry = this.cache.get(CACHE_KEY_PREFIX + key);
    if (!entry) return null;

    // Check TTL
    if (Date.now() - entry.timestamp > CACHE_TTL) {
      this.cache.delete(CACHE_KEY_PREFIX + key);
      return null;
    }

    return entry.data as T;
  }

  invalidate(key: string): void {
    this.cache.delete(CACHE_KEY_PREFIX + key);
  }

  invalidateAll(): void {
    this.cache.clear();
  }

  invalidatePattern(pattern: string): void {
    const regex = new RegExp(pattern);
    const keysToDelete: string[] = [];
    this.cache.forEach((_, key) => {
      if (regex.test(key)) {
        keysToDelete.push(key);
      }
    });
    keysToDelete.forEach(key => this.cache.delete(key));
  }
}

const metadataCache = new MetadataCache();

async function withRetry<T>(
  fn: () => Promise<T>,
  retries = MAX_RETRIES,
  delay = INITIAL_RETRY_DELAY,
  chunkInfo?: string
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    if (retries === 0) {
      console.error(`Max retries reached${chunkInfo ? ` for ${chunkInfo}` : ''}`);
      throw error;
    }

    const nextDelay = delay * 2; // Exponential backoff
    console.warn(
      `Retry ${MAX_RETRIES - retries + 1}/${MAX_RETRIES}${chunkInfo ? ` for ${chunkInfo}` : ''} ` +
      `after ${delay}ms. Error: ${error}`
    );

    await new Promise(resolve => setTimeout(resolve, delay));
    return withRetry(fn, retries - 1, nextDelay, chunkInfo);
  }
}

async function fetchWithTimeout(
  url: string,
  options?: RequestInit,
  timeoutMs = FETCH_TIMEOUT
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeoutMs}ms`);
    }
    throw error;
  }
}

export class FileExistsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FileExistsError';
  }
}

export interface FileMetadata {
  file_id: string;
  path: string;
  size: number;
  created_at: string;
  modified_at: string;
  chunks: ChunkEntry[];
  is_deleted: boolean;
  compressed?: boolean; // Flag para indicar si el archivo está comprimido
}

export interface ChunkEntry {
  chunk_id: string;
  seq_index: number;
  size: number;
  checksum: string | null;
  replicas: ReplicaInfo[];
}

export interface ReplicaInfo {
  node_id: string;
  url: string;
  state: string;
  last_heartbeat: string | null;
  checksum_verified: boolean;
}

export interface NodeInfo {
  node_id: string;
  host: string;
  port: number;
  rack: string | null;
  free_space: number;
  total_space: number;
  chunk_count: number;
  last_heartbeat: string;
  state: string;
}

export interface HealthResponse {
  status: string;
  timestamp: string;
  details?: {
    total_nodes: number;
    active_nodes: number;
    replication_factor: number;
  };
}

class APIService {
  private baseURL: string;

  constructor(baseURL: string) {
    this.baseURL = baseURL;
  }

  private async request<T>(
    endpoint: string,
    options?: RequestInit
  ): Promise<T> {
    const response = await fetch(`${this.baseURL}${endpoint}`, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...options?.headers,
      },
    });

    if (!response.ok) {
      const error = await response.text();
      
      // Detectar conflicto 409 (archivo ya existe)
      if (response.status === 409) {
        throw new FileExistsError(error);
      }
      
      throw new Error(`API Error: ${response.status} - ${error}`);
    }

    return response.json();
  }

  // Health
  async getHealth(): Promise<HealthResponse> {
    return this.request<HealthResponse>("/api/v1/health");
  }

  // Files
  async listFiles(prefix?: string): Promise<FileMetadata[]> {
    // Siempre consultar metadata fresca - no usar caché
    // La metadata cambia frecuentemente (heartbeats, replicación, etc.)
    const params = prefix ? `?prefix=${encodeURIComponent(prefix)}` : "";
    const files = await this.request<FileMetadata[]>(`/api/v1/files${params}`);
    
    console.log(`Files metadata loaded: ${files.length} files`);
    
    return files;
  }

  async getFile(path: string): Promise<FileMetadata> {
    // La metadata cambia frecuentemente (heartbeats, replicación, etc.)
    const file = await this.request<FileMetadata>(
      `/api/v1/files/${encodeURIComponent(path)}`
    );
    
    console.log(`File metadata loaded: ${path}`);
    
    return file;
  }

  async deleteFile(path: string, permanent: boolean = false): Promise<void> {
    await this.request(
      `/api/v1/files/${encodeURIComponent(path)}?permanent=${permanent}`,
      {
        method: "DELETE",
      }
    );
    
    // Solo invalidar blob cache (metadata siempre se consulta fresca)
    metadataCache.invalidate(`blob_${path}`);
    console.log(`File deleted, blob cache invalidated: ${path}`);
  }

  // En api.ts - Upload via Proxy

  async uploadFile(
    file: File,
    remotePath: string,
    onProgress?: (progress: number) => void,
    overwrite: boolean = false
  ): Promise<void> {
    // Determinar si debemos comprimir
    const needsCompression = shouldCompress(file.name);
    let fileToUpload: Blob = file;
    let originalSize = file.size;
    
    if (needsCompression) {
      console.log(`Compressing file before upload: ${file.name}`);
      const startTime = Date.now();
      fileToUpload = await compressBlob(file);
      const compressionTime = Date.now() - startTime;
      const compressionRatio = ((1 - fileToUpload.size / file.size) * 100).toFixed(1);
      console.log(
        `Compressed: ${originalSize} → ${fileToUpload.size} bytes ` +
        `(${compressionRatio}% reduction) in ${compressionTime}ms`
      );
    }
    
    // Si es sobrescritura, invalidar caché del blob existente ANTES de subir
    if (overwrite) {
      metadataCache.invalidate(`blob_${remotePath}`);
      console.log(`Cache invalidated for overwrite: blob_${remotePath}`);
    }
    
    // 1. Init upload - El servidor decide el chunk_size
    const initResponse = await this.request<{
      file_id: string;
      chunk_size: number;
      chunks: Array<{
        chunk_id: string;
        size: number;
        targets: string[];
      }>;
    }>("/api/v1/files/upload-init", {
      method: "POST",
      body: JSON.stringify({
        path: remotePath,
        size: fileToUpload.size, // Tamaño después de compresión
        compressed: needsCompression,
        original_size: needsCompression ? originalSize : undefined,
        overwrite: overwrite,
      }),
    });

    const { file_id, chunks, chunk_size } = initResponse;

    console.log(`Server determined chunk size: ${chunk_size} bytes`);
    console.log(`Total chunks to upload: ${chunks.length}`);

    const CONCURRENT_UPLOADS = 6;
    const commitData: Array<{
      chunk_id: string;
      checksum: string;
      nodes: string[];
    }> = [];
    let completedChunks = 0;

    const uploadChunk = async (index: number) => {
      const chunk = chunks[index];
      const start = index * chunk_size;
      const end = Math.min(start + chunk_size, fileToUpload.size);
      const chunkBlob = fileToUpload.slice(start, end);

      const arrayBuffer = await chunkBlob.arrayBuffer();
      const hashBuffer = await crypto.subtle.digest("SHA-256", arrayBuffer);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const checksum = hashArray
        .map(b => b.toString(16).padStart(2, "0"))
        .join("");

      const proxyUrl = `${this.baseURL}/api/v1/proxy/chunks/${chunk.chunk_id}?target_nodes=${chunk.targets.join(",")}`;

      // Retry logic para la subida
      const result = await withRetry(
        async () => {
          console.log(`Uploading chunk ${index + 1}/${chunks.length} (${chunkBlob.size} bytes)`);

          const formData = new FormData();
          formData.append("file", chunkBlob);

          const response = await fetchWithTimeout(proxyUrl, {
            method: "PUT",
            body: formData,
          });

          if (!response.ok) {
            throw new Error(
              `Upload failed: ${response.status} ${response.statusText}`
            );
          }

          return response.json();
        },
        MAX_RETRIES,
        INITIAL_RETRY_DELAY,
        `chunk ${index + 1}/${chunks.length}`
      );

      const uploadedNodes = result.nodes || [];

      completedChunks++;
      if (onProgress) {
        onProgress((completedChunks / chunks.length) * 100);
      }

      return {
        chunk_id: chunk.chunk_id,
        checksum,
        nodes: uploadedNodes,
      };
    };

    // Upload chunks en batches paralelos
    try {
      for (let i = 0; i < chunks.length; i += CONCURRENT_UPLOADS) {
        const batchEnd = Math.min(i + CONCURRENT_UPLOADS, chunks.length);
        const batchIndices = Array.from(
          { length: batchEnd - i },
          (_, idx) => i + idx
        );

        console.log(`Starting parallel batch: chunks ${i + 1}-${batchEnd}`);

        // Subir batch de chunks en paralelo
        const batchResults = await Promise.all(
          batchIndices.map(idx => uploadChunk(idx))
        );

        commitData.push(...batchResults);
      }
    } catch (error) {
      console.error(`Error during parallel upload:`, error);
      throw new Error(`Failed to upload file: ${error}`);
    }

    // 3. Commit
    await this.request("/api/v1/files/commit", {
      method: "POST",
      body: JSON.stringify({
        file_id,
        chunks: commitData,
      }),
    });
    
    console.log(`File uploaded successfully: ${remotePath}`);
  }

  async downloadFile(
    path: string,
    onProgress?: (progress: number) => void
  ): Promise<Blob> {
    // Check blob cache first
    const cacheKey = `blob_${path}`;
    const cachedBlob = metadataCache.get<Blob>(cacheKey);
    if (cachedBlob) {
      console.log(`Blob cache hit: ${path}`);
      if (onProgress) onProgress(100);
      return cachedBlob;
    }

    // Get file metadata
    const metadata = await this.getFile(path);

    // Determinar si usar descarga optimizada para streaming
    const useStreamingDownload = shouldStream(path, metadata.size);
    
    if (useStreamingDownload) {
      console.log(`Using streaming download for: ${path} (${metadata.size} bytes)`);
      // Para archivos streamables grandes, usar descarga secuencial con menos concurrencia
      // esto permite reproducción mientras se descarga
      return this.downloadFileStreaming(path, metadata, onProgress);
    }

    console.log(`Downloading file: ${metadata.chunks.length} chunks`);

    // Download chunks in parallel (max 10 concurrent para máxima velocidad)
    const CONCURRENT_DOWNLOADS = 10;
    const chunkBlobs: Blob[] = new Array(metadata.chunks.length);
    let completedChunks = 0;

    // Función para descargar un chunk individual con retry
    const downloadChunk = async (index: number) => {
      const chunk = metadata.chunks[index];
      const proxyUrl = `${this.baseURL}/api/v1/proxy/chunks/${chunk.chunk_id}?file_path=${encodeURIComponent(path)}`;

      // Retry logic para la descarga
      const chunkBlob = await withRetry(
        async () => {
          console.log(`Downloading chunk ${index + 1}/${metadata.chunks.length}`);

          const response = await fetchWithTimeout(proxyUrl);
          
          if (!response.ok) {
            throw new Error(
              `Download failed: ${response.status} ${response.statusText}`
            );
          }

          return response.blob();
        },
        MAX_RETRIES,
        INITIAL_RETRY_DELAY,
        `chunk ${index + 1}/${metadata.chunks.length}`
      );

      chunkBlobs[index] = chunkBlob; // Mantener orden correcto

      completedChunks++;
      if (onProgress) {
        onProgress((completedChunks / metadata.chunks.length) * 100);
      }
    };

    // Download chunks en batches paralelos
    try {
      for (let i = 0; i < metadata.chunks.length; i += CONCURRENT_DOWNLOADS) {
        const batchEnd = Math.min(i + CONCURRENT_DOWNLOADS, metadata.chunks.length);
        const batchIndices = Array.from(
          { length: batchEnd - i },
          (_, idx) => i + idx
        );

        console.log(`Downloading parallel batch: chunks ${i + 1}-${batchEnd}`);

        // Descargar batch en paralelo
        await Promise.all(
          batchIndices.map(idx => downloadChunk(idx))
        );
      }
    } catch (error) {
      console.error(`Error during parallel download:`, error);
      throw new Error(`Failed to download file: ${error}`);
    }

    console.log(`Download complete, combining ${chunkBlobs.length} chunks`);

    // Combine chunks en el orden correcto
    let finalBlob = new Blob(chunkBlobs);
    
    // Descomprimir si es necesario
    if (metadata.compressed) {
      console.log(`🗜️ Decompressing file: ${path}`);
      const startTime = Date.now();
      const compressedSize = finalBlob.size;
      finalBlob = await decompressBlob(finalBlob);
      const decompressionTime = Date.now() - startTime;
      console.log(
        `Decompressed: ${compressedSize} → ${finalBlob.size} bytes ` +
        `in ${decompressionTime}ms`
      );
    }
    
    // Store in cache for future previews (si no es muy grande)
    if (finalBlob.size <= MAX_CACHE_SIZE) {
      metadataCache.set(cacheKey, finalBlob);
      console.log(`Blob cached: ${path} (${finalBlob.size} bytes)`);
    } else {
      console.log(`File too large to cache: ${path} (${finalBlob.size} bytes)`);
    }
    
    return finalBlob;
  }

  /**
   * Descarga optimizada para archivos grandes streamables (video, audio, PDF)
   * Usa menos concurrencia para permitir reproducción progresiva
   */
  private async downloadFileStreaming(
    path: string,
    metadata: FileMetadata,
    onProgress?: (progress: number) => void
  ): Promise<Blob> {
    const cacheKey = `blob_${path}`;
    
    // Usar solo 3 descargas concurrentes para archivos streamables
    // Esto reduce presión de memoria y permite reproducción más rápida
    const CONCURRENT_DOWNLOADS = 3;
    const chunkBlobs: Blob[] = new Array(metadata.chunks.length);
    let completedChunks = 0;

    const downloadChunk = async (index: number) => {
      const chunk = metadata.chunks[index];
      const proxyUrl = `${this.baseURL}/api/v1/proxy/chunks/${chunk.chunk_id}?file_path=${encodeURIComponent(path)}`;

      const chunkBlob = await withRetry(
        async () => {
          const response = await fetchWithTimeout(proxyUrl);
          
          if (!response.ok) {
            throw new Error(
              `Download failed: ${response.status} ${response.statusText}`
            );
          }

          return response.blob();
        },
        MAX_RETRIES,
        INITIAL_RETRY_DELAY,
        `chunk ${index + 1}/${metadata.chunks.length}`
      );

      chunkBlobs[index] = chunkBlob;

      completedChunks++;
      if (onProgress) {
        onProgress((completedChunks / metadata.chunks.length) * 100);
      }
    };

    // Descargar en batches más pequeños para streaming
    for (let i = 0; i < metadata.chunks.length; i += CONCURRENT_DOWNLOADS) {
      const batchEnd = Math.min(i + CONCURRENT_DOWNLOADS, metadata.chunks.length);
      const batchIndices = Array.from(
        { length: batchEnd - i },
        (_, idx) => i + idx
      );

      await Promise.all(
        batchIndices.map(idx => downloadChunk(idx))
      );
    }

    const finalBlob = new Blob(chunkBlobs);
    
    // Cache para archivos streamables también (si no son demasiado grandes)
    if (finalBlob.size <= MAX_CACHE_SIZE) {
      metadataCache.set(cacheKey, finalBlob);
      console.log(`Streaming file cached: ${path} (${finalBlob.size} bytes)`);
    } else {
      console.log(`File too large to cache: ${path} (${finalBlob.size} bytes)`);
    }
    
    return finalBlob;
  }

  // Nodes
  async listNodes(): Promise<NodeInfo[]> {
    return this.request<NodeInfo[]>("/api/v1/nodes");
  }

  async getNode(nodeId: string): Promise<NodeInfo> {
    return this.request<NodeInfo>(`/api/v1/nodes/${nodeId}`);
  }

  // Cache management
  clearCache(): void {
    metadataCache.invalidateAll();
    console.log("Cache cleared completely");
  }

  getCacheStats(): { size: number; keys: string[] } {
    const keys: string[] = [];
    metadataCache['cache'].forEach((_, key) => {
      keys.push(key);
    });
    return {
      size: keys.length,
      keys: keys
    };
  }
}

export const api = new APIService(API_BASE_URL);
