async function init() {
  const base = self.location.href.replace(/\/[^/]+$/, '/');
  const resp = await fetch(base + 'stockfish-18-single.wasm.gz');
  const stream = resp.body.pipeThrough(new DecompressionStream('gzip'));
  const buffer = await new Response(stream).arrayBuffer();
  const blob = new Blob([buffer], { type: 'application/wasm' });
  const wasmUrl = URL.createObjectURL(blob);

  self.locateFile = (file) => file.endsWith('.wasm') ? wasmUrl : file;

  importScripts('stockfish-18-single.js');
}

init();