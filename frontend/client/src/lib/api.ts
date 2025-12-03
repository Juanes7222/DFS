/**
 * API Service para comunicación con el Metadata Service del DFS
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY = 1000; // 1 segundo

/**
 * Ejecuta una función con retry exponencial backoff
 * @param fn Función a ejecutar
 * @param retries Número de reintentos restantes
 * @param delay Delay actual en ms
 */
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
      console.error(`❌ Max retries reached${chunkInfo ? ` for ${chunkInfo}` : ''}`);
      throw error;
    }

    const nextDelay = delay * 2; // Exponential backoff
    console.warn(
      `⚠️ Retry ${MAX_RETRIES - retries + 1}/${MAX_RETRIES}${chunkInfo ? ` for ${chunkInfo}` : ''} ` +
      `after ${delay}ms. Error: ${error}`
    );

    await new Promise(resolve => setTimeout(resolve, delay));
    return withRetry(fn, retries - 1, nextDelay, chunkInfo);
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
    const params = prefix ? `?prefix=${encodeURIComponent(prefix)}` : "";
    return this.request<FileMetadata[]>(`/api/v1/files${params}`);
  }

  async getFile(path: string): Promise<FileMetadata> {
    return this.request<FileMetadata>(
      `/api/v1/files/${encodeURIComponent(path)}`
    );
  }

  async deleteFile(path: string, permanent: boolean = false): Promise<void> {
    await this.request(
      `/api/v1/files/${encodeURIComponent(path)}?permanent=${permanent}`,
      {
        method: "DELETE",
      }
    );
  }

  // En api.ts - Upload via Proxy

  async uploadFile(
    file: File,
    remotePath: string,
    onProgress?: (progress: number) => void
  ): Promise<void> {
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
        size: file.size,
      }),
    });

    const { file_id, chunks, chunk_size } = initResponse;

    console.log(`Server determined chunk size: ${chunk_size} bytes`);
    console.log(`Total chunks to upload: ${chunks.length}`);

    // 2. Upload chunks in parallel (max 3 concurrent)
    const CONCURRENT_UPLOADS = 3;
    const commitData: Array<{
      chunk_id: string;
      checksum: string;
      nodes: string[];
    }> = [];
    let completedChunks = 0;

    // Función para subir un chunk individual con retry
    const uploadChunk = async (index: number) => {
      const chunk = chunks[index];
      const start = index * chunk_size;
      const end = Math.min(start + chunk_size, file.size);
      const chunkBlob = file.slice(start, end);

      // Calculate checksum (fuera del retry para no recalcular)
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

          const response = await fetch(proxyUrl, {
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
  }

  async downloadFile(
    path: string,
    onProgress?: (progress: number) => void
  ): Promise<Blob> {
    // Get file metadata
    const metadata = await this.getFile(path);

    console.log(`Downloading file: ${metadata.chunks.length} chunks`);

    // Download chunks in parallel (max 5 concurrent)
    const CONCURRENT_DOWNLOADS = 8;
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

          const response = await fetch(proxyUrl);
          
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
    return new Blob(chunkBlobs);
  }

  // Nodes
  async listNodes(): Promise<NodeInfo[]> {
    return this.request<NodeInfo[]>("/api/v1/nodes");
  }

  async getNode(nodeId: string): Promise<NodeInfo> {
    return this.request<NodeInfo>(`/api/v1/nodes/${nodeId}`);
  }
}

export const api = new APIService(API_BASE_URL);
