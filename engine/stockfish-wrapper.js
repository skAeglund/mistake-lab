async function init() {
  const resp = await fetch('engine/stockfish-18-single.wasm.gz');
  const stream = resp.body.pipeThrough(new DecompressionStream('gzip'));
  const buffer = await new Response(stream).arrayBuffer();
  const blob = new Blob([buffer], { type: 'application/wasm' });
  const wasmUrl = URL.createObjectURL(blob);

  self.locateFile = (file) => file.includes('.wasm') ? wasmUrl : file;

  importScripts('stockfish-18-single.js');
}

init();