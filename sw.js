// MistakeLab Service Worker — v2
//
// Strategy:
//   - App shell (HTML, manifest)            → network-first, cache fallback
//       (fresh deploys arrive online; offline still works)
//   - Engine binaries (Stockfish .js/.wasm) → precached at install, cache-first
//       (rarely change — CACHE_NAME bump invalidates)
//   - Maia ONNX + move-mapping JSONs        → cache-first, lazy-populated
//       (43 MB is too large to precache; populated on first online use)
//   - Versioned CDN deps (jQuery, chess.js, chessboard.js, onnxruntime-web)
//                                           → cache-first
//       (URLs include version, naturally immutable)
//   - lichess1.org piece SVGs, Google Fonts → cache-first
//       (immutable per URL)
//   - Anything else cross-origin            → bypass SW (browser handles)
//
// CACHE_NAME bump (v1 → v2) wipes the old cache on activation. Existing tabs
// running v1 keep working until reload; on reload they receive v2.

const CACHE_NAME = 'mistakelab-v2';

// Critical app shell — precached at install. Failure here is logged but doesn't
// block activation; missing entries fall back to lazy runtime caching.
const PRECACHE = [
  './',
  './index.html',
  './manifest.json',
  './engine/stockfish-18-lite-single.js',
  './engine/stockfish-18-lite-single.wasm',
];

// Cross-origin URL patterns we cache opportunistically. Anything cross-origin
// not matching these is bypassed entirely (no SW interference).
const RUNTIME_CACHE_PATTERNS = [
  /^https:\/\/cdnjs\.cloudflare\.com\/ajax\/libs\/jquery\//,
  /^https:\/\/cdnjs\.cloudflare\.com\/ajax\/libs\/chess\.js\//,
  /^https:\/\/cdnjs\.cloudflare\.com\/ajax\/libs\/chessboard-js\//,
  /^https:\/\/cdn\.jsdelivr\.net\/npm\/onnxruntime-web@/,
  /^https:\/\/lichess1\.org\/assets\/piece\//,
  /^https:\/\/fonts\.googleapis\.com\//,
  /^https:\/\/fonts\.gstatic\.com\//,
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE).catch((err) => {
        // Non-fatal: a single missing entry shouldn't block the SW activating.
        // Files we couldn't precache will lazy-cache on first runtime fetch.
        console.warn('[SW] precache failed (will lazy-populate):', err);
      }))
      .then(() => self.skipWaiting())
  );
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
  if (req.method !== 'GET') return;

  const url = req.url;
  const isSameOrigin = url.startsWith(self.location.origin);
  const isRuntimeCacheable =
    !isSameOrigin && RUNTIME_CACHE_PATTERNS.some((re) => re.test(url));

  // Cross-origin URL we don't recognize → let the browser handle it.
  if (!isSameOrigin && !isRuntimeCacheable) return;

  // Cache-first for immutable assets:
  //   - cross-origin runtime-cacheable URLs (version-pinned)
  //   - same-origin engine binaries (rarely change; CACHE_NAME-bump invalidates)
  //   - same-origin Maia ONNX + JSON mapping files
  const isImmutable =
    isRuntimeCacheable ||
    /\/engine\/stockfish-18-lite-single\.(js|wasm)$/.test(url) ||
    /\/maia\//.test(url) ||
    url.endsWith('.onnx');

  if (isImmutable) {
    event.respondWith(
      caches.match(req).then((cached) => {
        if (cached) return cached;
        return fetch(req).then((res) => {
          // Cache successful responses + opaque cross-origin no-CORS responses.
          // (Opaque responses can be served back even though we can't read them.)
          if (res && (res.ok || res.type === 'opaque')) {
            const clone = res.clone();
            caches.open(CACHE_NAME)
              .then((cache) => cache.put(req, clone))
              .catch(() => {});  // quota / size errors are non-fatal
          }
          return res;
        });
      })
    );
    return;
  }

  // Network-first for everything else (HTML, manifest, JSON) — fresh deploys
  // arrive online; cache fills the gap when offline.
  event.respondWith(
    fetch(req)
      .then((res) => {
        if (res && res.ok) {
          const clone = res.clone();
          caches.open(CACHE_NAME)
            .then((cache) => cache.put(req, clone))
            .catch(() => {});
        }
        return res;
      })
      .catch(() => caches.match(req))
  );
});
