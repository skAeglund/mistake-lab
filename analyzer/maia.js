// ═══════════════════════════════════════════════════════════════════════════
// Maia 3 ONNX Engine for Node.js — human-like move prediction
// ═══════════════════════════════════════════════════════════════════════════
//
// Ported from MistakeLab's browser-side Maia integration (index.html).
// Uses onnxruntime-web (WASM backend) which works in both browser and Node.js.

const ort = require('onnxruntime-web');
const { Chess } = require('chess.js');
const fs = require('fs');
const path = require('path');

class MaiaEngine {
  constructor() {
    this.session = null;
    this.movesForward = null;  // UCI string → index
    this.movesReverse = null;  // index → UCI string
    this.elo = 1900;
  }

  async init(modelPath, movesDir) {
    // Load ONNX model
    this.session = await ort.InferenceSession.create(modelPath, {
      executionProviders: ['wasm'],
    });

    // Load move mappings
    const fwdPath = path.join(movesDir, 'all_moves_maia3.json');
    const revPath = path.join(movesDir, 'all_moves_maia3_reversed.json');
    this.movesForward = JSON.parse(fs.readFileSync(fwdPath, 'utf8'));
    this.movesReverse = JSON.parse(fs.readFileSync(revPath, 'utf8'));
  }

  /**
   * Predict the most likely human move (argmax) for the given FEN.
   * Returns a UCI string (e.g. "e2e4") or null.
   */
  async predict(fen) {
    if (!this.session) return null;

    const isBlack = fen.split(' ')[1] === 'b';
    const workFen = isBlack ? mirrorFEN(fen) : fen;

    // Encode board as 64×12 tensor
    const boardTokens = encodeBoard(workFen);

    // Build legal moves mask
    const chess = new Chess(workFen);
    const legalMoves = chess.moves({ verbose: true });
    const legalMask = new Float32Array(4352);
    for (const m of legalMoves) {
      const uci = m.from + m.to + (m.promotion || '');
      const idx = this.movesForward[uci];
      if (idx !== undefined) legalMask[idx] = 1.0;
    }

    // Build input tensors
    const feeds = {
      tokens: new ort.Tensor('float32', boardTokens, [1, 64, 12]),
      elo_self: new ort.Tensor('float32', Float32Array.from([this.elo]), [1]),
      elo_oppo: new ort.Tensor('float32', Float32Array.from([this.elo]), [1]),
    };

    const result = await this.session.run(feeds);

    // Find policy output (logits_move)
    const logits = result.logits_move?.data;
    if (!logits || logits.length < 4352) return null;

    // Argmax over legal moves
    let bestIdx = -1, bestVal = -Infinity;
    for (let i = 0; i < logits.length; i++) {
      if (legalMask[i] > 0 && logits[i] > bestVal) {
        bestVal = logits[i];
        bestIdx = i;
      }
    }

    if (bestIdx < 0) return null;

    let bestUci = this.movesReverse[String(bestIdx)];
    if (isBlack) bestUci = mirrorMove(bestUci);

    return bestUci;
  }

  release() {
    if (this.session) {
      this.session.release();
      this.session = null;
    }
  }
}

// ─── Board encoding: 64 squares × 12 piece channels ───
function encodeBoard(fen) {
  const piecePlacement = fen.split(' ')[0];
  const pieceTypes = ['P','N','B','R','Q','K','p','n','b','r','q','k'];
  const tensor = new Float32Array(64 * 12);
  const rows = piecePlacement.split('/');
  for (let rank = 0; rank < 8; rank++) {
    const row = 7 - rank;
    let file = 0;
    for (const ch of rows[rank]) {
      const n = parseInt(ch);
      if (isNaN(n)) {
        const idx = pieceTypes.indexOf(ch);
        if (idx >= 0) tensor[(row * 8 + file) * 12 + idx] = 1.0;
        file++;
      } else {
        file += n;
      }
    }
  }
  return tensor;
}

// ─── FEN mirroring (always present position from white's perspective) ───
function mirrorSquare(sq) {
  return sq[0] + String(9 - parseInt(sq[1]));
}

function mirrorMove(uci) {
  const from = uci.substring(0, 2);
  const to = uci.substring(2, 4);
  const promo = uci.length > 4 ? uci.substring(4) : '';
  return mirrorSquare(from) + mirrorSquare(to) + promo;
}

function swapColorsInRank(rank) {
  let result = '';
  for (const ch of rank) {
    if (/[A-Z]/.test(ch)) result += ch.toLowerCase();
    else if (/[a-z]/.test(ch)) result += ch.toUpperCase();
    else result += ch;
  }
  return result;
}

function swapCastlingRights(castling) {
  if (castling === '-') return '-';
  let out = '';
  if (castling.includes('k')) out += 'K';
  if (castling.includes('q')) out += 'Q';
  if (castling.includes('K')) out += 'k';
  if (castling.includes('Q')) out += 'q';
  return out || '-';
}

function mirrorFEN(fen) {
  const [pos, color, castling, ep, half, full] = fen.split(' ');
  const ranks = pos.split('/');
  const mirrored = ranks.slice().reverse().map(r => swapColorsInRank(r)).join('/');
  const mirColor = color === 'w' ? 'b' : 'w';
  const mirCastling = swapCastlingRights(castling);
  const mirEp = ep !== '-' ? mirrorSquare(ep) : '-';
  return `${mirrored} ${mirColor} ${mirCastling} ${mirEp} ${half} ${full}`;
}

module.exports = { MaiaEngine };
