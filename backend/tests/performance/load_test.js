/**
 * Performance test para DFS usando k6
 * 
 * Ejecutar:
 *   k6 run load_test.js
 * 
 * Con opciones:
 *   k6 run --vus 10 --duration 30s load_test.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Métricas personalizadas
const errorRate = new Rate('errors');
const uploadDuration = new Trend('upload_duration');
const downloadDuration = new Trend('download_duration');

// Configuración del test
export const options = {
  stages: [
    { duration: '30s', target: 10 },  // Ramp up a 10 usuarios
    { duration: '1m', target: 10 },   // Mantener 10 usuarios
    { duration: '30s', target: 50 },  // Ramp up a 50 usuarios
    { duration: '2m', target: 50 },   // Mantener 50 usuarios
    { duration: '30s', target: 0 },   // Ramp down
  ],
  thresholds: {
    'http_req_duration': ['p(95)<2000'], // 95% de requests < 2s
    'errors': ['rate<0.1'],              // Tasa de error < 10%
    'http_req_failed': ['rate<0.1'],     // Tasa de fallos < 10%
  },
};

// URL base del DFS
const BASE_URL = __ENV.DFS_URL || 'http://localhost:8000';

// Datos de prueba
const testData = 'x'.repeat(1024 * 1024); // 1MB de datos

/**
 * Setup: Se ejecuta una vez al inicio
 */
export function setup() {
  console.log('Iniciando performance test...');
  
  // Verificar que el servicio está disponible
  const res = http.get(`${BASE_URL}/api/v1/health`);
  check(res, {
    'servicio disponible': (r) => r.status === 200,
  });
  
  return { baseUrl: BASE_URL };
}

/**
 * Test principal: Se ejecuta por cada VU (virtual user)
 */
export default function(data) {
  const baseUrl = data.baseUrl;
  
  // Test 1: Health check
  testHealthCheck(baseUrl);
  sleep(1);
  
  // Test 2: Listar archivos
  testListFiles(baseUrl);
  sleep(1);
  
  // Test 3: Upload de archivo
  const filePath = testUpload(baseUrl);
  sleep(2);
  
  // Test 4: Download de archivo
  if (filePath) {
    testDownload(baseUrl, filePath);
    sleep(1);
    
    // Test 5: Eliminar archivo
    testDelete(baseUrl, filePath);
  }
  
  sleep(2);
}

/**
 * Teardown: Se ejecuta una vez al final
 */
export function teardown(data) {
  console.log('Performance test completado');
}

// ============================================================================
// FUNCIONES DE TEST
// ============================================================================

function testHealthCheck(baseUrl) {
  const res = http.get(`${baseUrl}/api/v1/health`);
  
  const success = check(res, {
    'health check status 200': (r) => r.status === 200,
    'health check response time < 200ms': (r) => r.timings.duration < 200,
  });
  
  errorRate.add(!success);
}

function testListFiles(baseUrl) {
  const res = http.get(`${baseUrl}/api/v1/files`);
  
  const success = check(res, {
    'list files status 200': (r) => r.status === 200,
    'list files is array': (r) => Array.isArray(JSON.parse(r.body)),
    'list files response time < 500ms': (r) => r.timings.duration < 500,
  });
  
  errorRate.add(!success);
}

function testUpload(baseUrl) {
  const filePath = `/test/file_${__VU}_${Date.now()}.txt`;
  
  // Paso 1: Iniciar upload
  const initStart = Date.now();
  const initRes = http.post(
    `${baseUrl}/api/v1/files/upload-init`,
    JSON.stringify({
      path: filePath,
      size: testData.length,
      chunk_size: 1048576
    }),
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );
  
  const initSuccess = check(initRes, {
    'upload init status 200': (r) => r.status === 200,
    'upload init has file_id': (r) => JSON.parse(r.body).file_id !== undefined,
  });
  
  if (!initSuccess) {
    errorRate.add(true);
    return null;
  }
  
  const initData = JSON.parse(initRes.body);
  
  // Paso 2: Subir chunks (simulado - en k6 no podemos hacer multipart fácilmente)
  // En un test real, subirías los chunks a los DataNodes
  
  // Paso 3: Commit
  const commitRes = http.post(
    `${baseUrl}/api/v1/files/commit`,
    JSON.stringify({
      file_id: initData.file_id,
      chunks: initData.chunks.map(c => ({
        chunk_id: c.chunk_id,
        checksum: 'dummy_checksum',
        nodes: c.targets.map(t => `node-${t}`)
      }))
    }),
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );
  
  const commitSuccess = check(commitRes, {
    'upload commit status 200': (r) => r.status === 200,
  });
  
  const duration = Date.now() - initStart;
  uploadDuration.add(duration);
  errorRate.add(!commitSuccess);
  
  return commitSuccess ? filePath : null;
}

function testDownload(baseUrl, filePath) {
  const start = Date.now();
  
  // Obtener metadata del archivo
  const encodedPath = encodeURIComponent(filePath);
  const res = http.get(`${baseUrl}/api/v1/files/${encodedPath}`);
  
  const success = check(res, {
    'download metadata status 200': (r) => r.status === 200,
    'download has chunks': (r) => JSON.parse(r.body).chunks.length > 0,
  });
  
  const duration = Date.now() - start;
  downloadDuration.add(duration);
  errorRate.add(!success);
}

function testDelete(baseUrl, filePath) {
  const encodedPath = encodeURIComponent(filePath);
  const res = http.del(`${baseUrl}/api/v1/files/${encodedPath}`);
  
  const success = check(res, {
    'delete status 200': (r) => r.status === 200,
  });
  
  errorRate.add(!success);
}

// ============================================================================
// ESCENARIOS ADICIONALES
// ============================================================================

/**
 * Test de carga constante
 */
export function constantLoad() {
  // 100 usuarios durante 5 minutos
  return {
    executor: 'constant-vus',
    vus: 100,
    duration: '5m',
  };
}

/**
 * Test de spike
 */
export function spikeTest() {
  return {
    executor: 'ramping-vus',
    stages: [
      { duration: '10s', target: 10 },
      { duration: '10s', target: 100 },  // Spike
      { duration: '30s', target: 100 },
      { duration: '10s', target: 10 },
    ],
  };
}

/**
 * Test de stress
 */
export function stressTest() {
  return {
    executor: 'ramping-vus',
    stages: [
      { duration: '1m', target: 50 },
      { duration: '2m', target: 100 },
      { duration: '2m', target: 200 },
      { duration: '2m', target: 300 },
      { duration: '1m', target: 0 },
    ],
  };
}
