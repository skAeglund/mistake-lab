// MistakeLab Service Worker — v1
// Strategy: network-first (always fetch fresh, cache as offline fallback)

const CACHE_NAME = 'mistakelab-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((names) =>
      Promise.all(
        names.filter((n) => n !== CACHE_NAME).map((n) => caches.delete(n))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;

  // Only cache GET requests for same-origin resources
  if (req.method !== 'GET' || !req.url.startsWith(self.location.origin)) {
    return;
  }

  // Skip caching the large ONNX model and WASM files
  if (req.url.endsWith('.onnx') || req.url.endsWith('.wasm')) {
    return;
  }

  event.respondWith(
    fetch(req)
      .then((response) => {
        const clone = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, clone));
        return response;
      })
      .catch(() => caches.match(req))
  );
});
