// Set locateFile synchronously before anything else runs
self.locateFile = (file) => file.endsWith('.wasm') ? self._wasmBlobUrl : file;

async function init() {
  const base = self.location.href.replace(/\/[^/]+$/, '/');
  const resp = await fetch(base + 'stockfish-18-single.wasm.gz');
  const stream = resp.body.pipeThrough(new DecompressionStream('gzip'));
  const buffer = await new Response(stream).arrayBuffer();
  const blob = new Blob([buffer], { type: 'application/wasm' });
  self._wasmBlobUrl = URL.createObjectURL(blob);

  importScripts('stockfish-18-single.js');
}

init();