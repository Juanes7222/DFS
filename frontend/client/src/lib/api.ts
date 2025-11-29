/**
 * API Service para comunicación con el Metadata Service del DFS
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

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
    // 1. Init upload
    const initResponse = await this.request<{
      file_id: string;
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
        chunk_size: 64 * 1024 * 1024, // 64MB
      }),
    });

    const { file_id, chunks } = initResponse;
    const chunkSize = 64 * 1024 * 1024;

    // 2. Upload chunks via metadata service proxy
    const commitData: Array<{
      chunk_id: string;
      checksum: string;
      nodes: string[];
    }> = [];

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      const chunkBlob = file.slice(start, end);

      // Calculate checksum
      const arrayBuffer = await chunkBlob.arrayBuffer();
      const hashBuffer = await crypto.subtle.digest("SHA-256", arrayBuffer);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const checksum = hashArray
        .map(b => b.toString(16).padStart(2, "0"))
        .join("");

      try {
        const formData = new FormData();
        formData.append("file", chunkBlob);

        // Upload via proxy endpoint (no need to access DataNodes directly)
        const proxyUrl = `${this.baseURL}/api/v1/proxy/chunks/${chunk.chunk_id}?target_nodes=${chunk.targets.join(",")}`;

        console.log(`📤 Uploading chunk ${i + 1}/${chunks.length} via proxy`);

        const response = await fetch(proxyUrl, {
          method: "PUT",
          body: formData,
        });

        if (!response.ok) {
          throw new Error(
            `Upload failed: ${response.status} ${response.statusText}`
          );
        }

        const result = await response.json();
        const uploadedNodes = result.nodes || [];

        commitData.push({
          chunk_id: chunk.chunk_id,
          checksum,
          nodes: uploadedNodes,
        });

        if (onProgress) {
          onProgress(((i + 1) / chunks.length) * 100);
        }
      } catch (error) {
        console.error(`Error uploading chunk ${i}:`, error);
        throw new Error(`Failed to upload chunk ${i}: ${error}`);
      }
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

    // Download chunks via proxy
    const chunkBlobs: Blob[] = [];

    for (let i = 0; i < metadata.chunks.length; i++) {
      const chunk = metadata.chunks[i];

      try {
        // Download via proxy endpoint (no need to access DataNodes directly)
        const proxyUrl = `${this.baseURL}/api/v1/proxy/chunks/${chunk.chunk_id}?file_path=${encodeURIComponent(path)}`;
        
        console.log(`📥 Downloading chunk ${i + 1}/${metadata.chunks.length} via proxy`);

        const response = await fetch(proxyUrl);
        
        if (!response.ok) {
          throw new Error(
            `Download failed: ${response.status} ${response.statusText}`
          );
        }

        const chunkBlob = await response.blob();
        chunkBlobs.push(chunkBlob);

        if (onProgress) {
          onProgress(((i + 1) / metadata.chunks.length) * 100);
        }
      } catch (error) {
        console.error(`Error downloading chunk ${i}:`, error);
        throw new Error(`Failed to download chunk ${i}: ${error}`);
      }
    }

    // Combine chunks
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
