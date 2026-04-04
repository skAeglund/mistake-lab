#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// MistakeLab Analyzer — Batch Stockfish analysis for unanalyzed games
// ═══════════════════════════════════════════════════════════════════════════
//
// Fetches games from Lichess or Chess.com, evaluates every position with
// native Stockfish, extracts mistakes, and saves results to a GitHub Gist
// (and local file) so MistakeLab can use them on next launch.
//
// Usage:
//   cd analyzer
//   npm install
//   node analyze.js --username <lichess_name> --stockfish <path_to_binary> [options]
//
// Options:
//   --username    Lichess or Chess.com username (required)
//   --stockfish   Path to native Stockfish binary (required)
//   --platform    "lichess" (default) or "chesscom"
//   --depth       Analysis depth per position (default: 18)
//   --threads     Total Stockfish threads (default: half your CPUs)
//   --hash        Total Stockfish hash table in MB (default: 256)
//   --workers     Parallel Stockfish instances (default: 1, threads/hash split evenly)
//   --max         Max games to fetch (default: 500)
//   --gist-token  GitHub personal access token (gist scope)
//   --gist-id     Existing Gist ID for MistakeLab sync
//   --save-every  Save progress every N games (default: 5)
//   --output      Local output file (default: analyzed_games.json)
//   --rated-only  Only analyze rated games (default: true)
//   --time-controls Comma-separated: bullet,blitz,rapid,classical (default: all)
//   --scan-tactics Scan existing analyzed games for tactical puzzles (no fetch)
//   --tactic-depth MultiPV analysis depth for tactic detection (default: 20)
//   --rescan       Re-evaluate 'found' flags on existing tactics (no Stockfish needed)
//
// Examples:
//   node analyze.js --username DrNykterstein --stockfish ./stockfish
//   node analyze.js --username DrNykterstein --stockfish "C:\stockfish\stockfish.exe" --depth 20 --threads 8
//   node analyze.js --username magnus --platform chesscom --stockfish ./stockfish
//   node analyze.js --username user1 --stockfish ./stockfish --gist-token ghp_xxx --gist-id abc123
//   node analyze.js --scan-tactics --stockfish ./stockfish --gist-token ghp_xxx --gist-id abc123
//   node analyze.js --rescan --gist-token ghp_xxx --gist-id abc123
//
// Download Stockfish: https://stockfishchess.org/download/
// ═══════════════════════════════════════════════════════════════════════════

const { spawn } = require('child_process');
const { createInterface } = require('readline');
const { Chess } = require('chess.js');
const fs = require('fs');
const path = require('path');
const os = require('os');
const https = require('https');

// ─── Arg parsing ───
function parseArgs() {
  const args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) {
      const key = argv[i].slice(2);
      const val = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : 'true';
      args[key] = val;
      if (val !== 'true') i++;
    }
  }
  return args;
}

const args = parseArgs();
const CONFIG = {
  username:      args.username || '',
  stockfishPath: args.stockfish || '',
  platform:      args.platform || 'lichess',
  depth:         parseInt(args.depth) || 18,
  threads:       parseInt(args.threads) || Math.max(1, Math.floor(os.cpus().length / 2)),
  hash:          parseInt(args.hash) || 256,
  workers:       parseInt(args.workers) || 1,
  maxGames:      parseInt(args.max) || 500,
  gistToken:     args['gist-token'] || process.env.MISTAKELAB_GIST_TOKEN || '',
  gistId:        args['gist-id'] || process.env.MISTAKELAB_GIST_ID || '',
  saveEvery:     parseInt(args['save-every']) || 5,
  outputFile:    args.output || 'analyzed_games.json',
  ratedOnly:     args['rated-only'] !== 'false',
  timeControls:  args['time-controls'] ? args['time-controls'].split(',') : null,
  // Tactic scanning
  scanTactics:   args['scan-tactics'] === 'true' || args['scan-tactics'] === true,
  tacticDepth:   parseInt(args['tactic-depth']) || 20,
  rescan:        args['rescan'] === 'true' || args['rescan'] === true,
};

if (CONFIG.rescan) {
  // Rescan doesn't need Stockfish — just re-evaluates found flags
} else if (CONFIG.scanTactics) {
  if (!CONFIG.stockfishPath) {
    console.error('Usage: node analyze.js --scan-tactics --stockfish <path> [--gist-token X --gist-id Y]');
    process.exit(1);
  }
} else if (!CONFIG.username || !CONFIG.stockfishPath) {
  console.error('Usage: node analyze.js --username <n> --stockfish <path> [options]');
  console.error('       node analyze.js --scan-tactics --stockfish <path> [options]');
  console.error('       node analyze.js --rescan [--gist-token X --gist-id Y]');
  console.error('Run with --help for all options.');
  process.exit(1);
}

// ─── Logging ───
let gistUsernames = []; // Tracks usernames from Gist (preserved across platforms)
const START_TIME = Date.now();
function log(msg) {
  const elapsed = ((Date.now() - START_TIME) / 1000).toFixed(1);
  console.log(`[${elapsed}s] ${msg}`);
}
function logError(msg) {
  const elapsed = ((Date.now() - START_TIME) / 1000).toFixed(1);
  console.error(`[${elapsed}s] ERROR: ${msg}`);
}

// ─── HTTP helper (works in Node 18+ with global fetch, falls back to https) ───
function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': 'MistakeLab-Analyzer/1.0',
        ...headers,
      },
    };
    const req = https.request(options, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        // Follow redirect
        httpGet(res.headers.location, headers).then(resolve).catch(reject);
        return;
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} from ${parsedUrl.hostname}${parsedUrl.pathname}: ${data.slice(0, 200)}`));
        } else {
          resolve(data);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('Request timeout')); });
    req.end();
  });
}

function httpPatch(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
    const options = {
      hostname: parsedUrl.hostname,
      port: 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'PATCH',
      headers: {
        'User-Agent': 'MistakeLab-Analyzer/1.0',
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        ...headers,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
        else resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('Request timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

function httpPost(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
    const options = {
      hostname: parsedUrl.hostname,
      port: 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'POST',
      headers: {
        'User-Agent': 'MistakeLab-Analyzer/1.0',
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        ...headers,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
        else resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('Request timeout')); });
    req.write(bodyStr);
    req.end();
  });
}


// ═══════════════════════════════════════════════════════════════════════════
// Stockfish Engine Wrapper
// ═══════════════════════════════════════════════════════════════════════════

class StockfishEngine {
  constructor(sfPath) {
    this.sfPath = sfPath;
    this.proc = null;
    this.rl = null;
    this.lineHandlers = [];
    this.positionsEvaluated = 0;
  }

  async start(threads, hash) {
    this.proc = spawn(this.sfPath, [], { stdio: ['pipe', 'pipe', 'pipe'] });

    this.proc.on('error', (err) => {
      logError(`Stockfish process error: ${err.message}`);
      logError('Make sure the path to Stockfish is correct and the binary is executable.');
      process.exit(1);
    });

    this.proc.stderr.on('data', (data) => {
      // SF sometimes writes to stderr — ignore unless debugging
    });

    this.rl = createInterface({ input: this.proc.stdout, crlfDelay: Infinity });
    this.rl.on('line', (line) => {
      for (const handler of this.lineHandlers) handler(line);
    });

    // UCI init
    this._send('uci');
    await this._waitFor(line => line === 'uciok');

    this._send(`setoption name Threads value ${threads}`);
    this._send(`setoption name Hash value ${hash}`);
    this._send('isready');
    await this._waitFor(line => line === 'readyok');

    log(`Stockfish ready (threads=${threads}, hash=${hash}MB)`);
  }

  _send(cmd) {
    if (this.proc && !this.proc.killed) {
      this.proc.stdin.write(cmd + '\n');
    }
  }

  _waitFor(predicate) {
    return new Promise((resolve) => {
      const handler = (line) => {
        if (predicate(line)) {
          this.lineHandlers = this.lineHandlers.filter(h => h !== handler);
          resolve(line);
        }
      };
      this.lineHandlers.push(handler);
    });
  }

  /**
   * Signal a new game to Stockfish (clears hash). Call once per game, not per position.
   */
  async newGame() {
    this._send('ucinewgame');
    this._send('isready');
    await this._waitFor(line => line === 'readyok');
  }

  /**
   * Evaluate a position. Returns { eval: cp } or { mate: N } from WHITE's perspective.
   */
  async evaluate(fen, depth) {
    this._send(`position fen ${fen}`);
    this._send(`go depth ${depth}`);

    let bestCp = 0;
    let bestMate = null;
    let bestDepth = 0;

    return new Promise((resolve) => {
      const handler = (line) => {
        if (line.startsWith('info') && line.includes(' pv ')) {
          const mpvM = line.match(/multipv (\d+)/);
          const pv = parseInt(mpvM?.[1] || '1');
          if (pv !== 1) return; // only care about best line

          const dM = line.match(/\bdepth (\d+)/);
          const d = parseInt(dM?.[1] || '0');

          const cpM = line.match(/score cp (-?\d+)/);
          const mateM = line.match(/score mate (-?\d+)/);

          if (d >= bestDepth) {
            bestDepth = d;
            if (cpM) { bestCp = parseInt(cpM[1]); bestMate = null; }
            if (mateM) { bestMate = parseInt(mateM[1]); bestCp = 0; }
          }
        }

        if (line.startsWith('bestmove')) {
          this.lineHandlers = this.lineHandlers.filter(h => h !== handler);
          this.positionsEvaluated++;

          // Convert from side-to-move perspective to white's perspective
          const sideToMove = fen.split(' ')[1];
          const flip = sideToMove === 'b' ? -1 : 1;

          if (bestMate !== null) {
            resolve({ mate: bestMate * flip });
          } else {
            resolve({ eval: bestCp * flip });
          }
        }
      };
      this.lineHandlers.push(handler);
    });
  }

  /**
   * Evaluate a position with MultiPV. Returns array of { eval/mate, move (uci) }
   * from WHITE's perspective, ordered best → worst.
   */
  async evaluateMultiPV(fen, depth, numPVs = 2) {
    this._send(`setoption name MultiPV value ${numPVs}`);
    this._send(`position fen ${fen}`);
    this._send(`go depth ${depth}`);

    // Track best result per PV line at the highest depth seen
    const pvResults = {}; // pv# → { cp, mate, move, depth }

    return new Promise((resolve) => {
      const handler = (line) => {
        if (line.startsWith('info') && line.includes(' pv ')) {
          const mpvM = line.match(/multipv (\d+)/);
          const pvNum = parseInt(mpvM?.[1] || '1');
          if (pvNum > numPVs) return;

          const dM = line.match(/\bdepth (\d+)/);
          const d = parseInt(dM?.[1] || '0');

          const cpM = line.match(/score cp (-?\d+)/);
          const mateM = line.match(/score mate (-?\d+)/);

          // Extract first move of PV line
          const pvMovesM = line.match(/ pv (.+)/);
          const firstMove = pvMovesM ? pvMovesM[1].split(' ')[0] : null;

          const prev = pvResults[pvNum];
          if (!prev || d >= prev.depth) {
            pvResults[pvNum] = {
              cp: cpM ? parseInt(cpM[1]) : 0,
              mate: mateM ? parseInt(mateM[1]) : null,
              move: firstMove,
              depth: d,
            };
          }
        }

        if (line.startsWith('bestmove')) {
          this.lineHandlers = this.lineHandlers.filter(h => h !== handler);
          this.positionsEvaluated++;

          // Reset MultiPV to 1 for normal operation
          this._send('setoption name MultiPV value 1');

          // Convert from side-to-move perspective to white's perspective
          const sideToMove = fen.split(' ')[1];
          const flip = sideToMove === 'b' ? -1 : 1;

          const results = [];
          for (let i = 1; i <= numPVs; i++) {
            const r = pvResults[i];
            if (!r) break; // fewer legal moves than numPVs
            if (r.mate !== null) {
              results.push({ mate: r.mate * flip, move: r.move });
            } else {
              results.push({ eval: r.cp * flip, move: r.move });
            }
          }

          resolve(results);
        }
      };
      this.lineHandlers.push(handler);
    });
  }

  quit() {
    if (this.proc && !this.proc.killed) {
      this._send('quit');
      setTimeout(() => {
        if (this.proc && !this.proc.killed) this.proc.kill();
      }, 2000);
    }
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// Lichess Game Fetcher
// ═══════════════════════════════════════════════════════════════════════════

async function fetchLichessGames(username, maxGames) {
  log(`Fetching up to ${maxGames} games from Lichess for "${username}"…`);

  const params = new URLSearchParams({
    max: String(maxGames),
    moves: 'true',
    pgnInJson: 'true',
    opening: 'true',
    clocks: 'false',
    evals: 'true',       // include server evals if available (we'll use them)
    rated: CONFIG.ratedOnly ? 'true' : '',
  });
  // Remove empty params
  for (const [k, v] of [...params.entries()]) { if (!v) params.delete(k); }

  if (CONFIG.timeControls) {
    params.set('perfType', CONFIG.timeControls.join(','));
  }

  const url = `https://lichess.org/api/games/user/${encodeURIComponent(username)}?${params}`;

  const text = await httpGet(url, { 'Accept': 'application/x-ndjson' });
  const lines = text.trim().split('\n').filter(l => l.trim());
  const games = lines.map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);

  log(`Fetched ${games.length} games from Lichess`);
  return games;
}


// ═══════════════════════════════════════════════════════════════════════════
// Chess.com Game Fetcher
// ═══════════════════════════════════════════════════════════════════════════

async function fetchChesscomGames(username, maxGames) {
  log(`Fetching games from Chess.com for "${username}"…`);

  // Get monthly archive list
  const archivesText = await httpGet(
    `https://api.chess.com/pub/player/${encodeURIComponent(username.toLowerCase())}/games/archives`
  );
  const { archives } = JSON.parse(archivesText);
  if (!archives || archives.length === 0) {
    log('No game archives found on Chess.com');
    return [];
  }

  // Fetch from newest to oldest
  const allGames = [];
  for (let i = archives.length - 1; i >= 0 && allGames.length < maxGames; i--) {
    const url = archives[i];
    log(`  Fetching archive: ${url.split('/').slice(-2).join('/')}`);

    try {
      const text = await httpGet(url);
      const { games } = JSON.parse(text);
      if (!games) continue;

      for (const g of games) {
        if (allGames.length >= maxGames) break;
        const normalized = normalizeChesscomGame(g, username);
        if (normalized) allGames.push(normalized);
      }
    } catch (err) {
      logError(`Failed to fetch archive ${url}: ${err.message}`);
    }

    // Chess.com rate limiting — pause between archive requests
    await sleep(500);
  }

  log(`Fetched ${allGames.length} games from Chess.com`);
  return allGames;
}

/**
 * Convert a Chess.com game object into MistakeLab's expected format.
 */
function normalizeChesscomGame(g, username) {
  // Skip non-standard games
  if (!g.pgn) return null;
  const rules = g.rules || 'chess';
  if (rules !== 'chess') return null;

  // Parse PGN to extract moves
  const chess = new Chess();
  let loaded = false;
  try { loaded = chess.load_pgn(g.pgn); } catch { return null; }
  if (!loaded) return null;

  const history = chess.history();
  if (history.length < 6) return null; // skip very short games

  // Extract PGN headers
  const headers = {};
  const headerRegex = /\[(\w+)\s+"([^"]*)"\]/g;
  let hm;
  while ((hm = headerRegex.exec(g.pgn)) !== null) {
    headers[hm[1]] = hm[2];
  }

  // Filter by time control
  const timeClass = g.time_class || inferTimeClass(headers.TimeControl);
  if (CONFIG.ratedOnly && !g.rated) return null;
  if (CONFIG.timeControls && !CONFIG.timeControls.includes(timeClass)) return null;

  // Build game object in MistakeLab format
  const isWhite = (headers.White || '').toLowerCase() === username.toLowerCase();
  const gameUrl = g.url || '';
  const gameId = 'chesscom_' + (gameUrl.match(/\/(\d+)$/)?.[1] || String(g.end_time || Date.now()));

  return {
    id: gameId,
    moves: history.join(' '),
    analysis: null, // will be filled by analyzer
    players: {
      white: { user: { name: headers.White || 'Unknown', id: (headers.White || 'unknown').toLowerCase() } },
      black: { user: { name: headers.Black || 'Unknown', id: (headers.Black || 'unknown').toLowerCase() } },
    },
    opening: { name: headers.ECOUrl ? decodeChesscomOpening(headers.ECOUrl) : (headers.Opening || 'Unknown') },
    createdAt: g.end_time ? g.end_time * 1000 : Date.now(),
    speed: timeClass,
    pgn: g.pgn,
    _source: 'chesscom',
    _playerColor: isWhite ? 'white' : 'black',
  };
}

function decodeChesscomOpening(url) {
  // "https://www.chess.com/openings/Sicilian-Defense-2...d6" → "Sicilian Defense 2...d6"
  const name = url.split('/').pop() || '';
  return decodeURIComponent(name).replace(/-/g, ' ');
}

function inferTimeClass(tc) {
  if (!tc) return 'rapid';
  const parts = tc.split('+');
  const base = parseInt(parts[0]) || 600;
  const inc = parseInt(parts[1]) || 0;
  const total = base + 40 * inc;
  if (total < 180) return 'bullet';
  if (total < 600) return 'blitz';
  if (total < 1800) return 'rapid';
  return 'classical';
}


// ═══════════════════════════════════════════════════════════════════════════
// Game Analyzer
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Analyze a single game: replay all moves, evaluate each position.
 * Returns the game object with an `analysis` array added.
 */
async function analyzeGame(pool, game, depth) {
  const moves = game.moves.split(' ').filter(Boolean);
  if (moves.length < 4) return null; // too short

  // Pre-compute all FENs
  const chess = new Chess();
  const fens = [];
  for (let i = 0; i < moves.length; i++) {
    const result = chess.move(moves[i], { sloppy: true });
    if (!result) {
      logError(`  Invalid move "${moves[i]}" at ply ${i + 1}, stopping analysis`);
      break;
    }
    fens.push(chess.fen());
  }

  // Signal new game to all workers
  await Promise.all(pool.map(sf => sf.newGame()));

  // Evaluate positions in parallel using a shared queue
  const results = new Array(fens.length);
  let nextIdx = 0;
  let cacheHitCount = 0;

  async function workerLoop(sf) {
    while (nextIdx < fens.length) {
      const idx = nextIdx++;

      // Check position cache first
      const cached = getCachedEval(fens[idx], depth);
      if (cached) {
        results[idx] = cached;
        cacheHitCount++;
        continue;
      }

      const posStart = Date.now();
      posCacheMisses++;
      results[idx] = await sf.evaluate(fens[idx], depth);
      setCachedEval(fens[idx], results[idx], depth);
      const elapsed = ((Date.now() - posStart) / 1000).toFixed(1);
      const ev = results[idx];
      const evStr = ev.mate !== undefined ? `M${ev.mate}` : `${ev.eval}cp`;
      log(`  ply ${idx + 1}/${fens.length}: ${moves[idx]} → ${evStr} (${elapsed}s)`);
    }
  }

  await Promise.all(pool.map(sf => workerLoop(sf)));

  if (cacheHitCount > 0) {
    log(`  ${cacheHitCount}/${fens.length} positions from cache`);
  }

  return { ...game, analysis: results };
}


// ═══════════════════════════════════════════════════════════════════════════
// Win% and Mistake Detection (for progress reporting)
// ═══════════════════════════════════════════════════════════════════════════

function cpToWinPct(cp) {
  return 100 / (1 + Math.pow(10, -cp / 400));
}

function evalToCp(evalObj, perspective) {
  let cp;
  if (evalObj.eval !== undefined) cp = evalObj.eval;
  else if (evalObj.mate !== undefined) cp = evalObj.mate > 0 ? 10000 : -10000;
  else return 0;
  return perspective === 'white' ? cp : -cp;
}

function countMistakes(game, username) {
  const moves = game.moves.split(' ').filter(Boolean);
  const analysis = game.analysis || [];
  let playerColor;
  if (game._playerColor) {
    playerColor = game._playerColor;
  } else {
    const isWhite = game.players?.white?.user?.name?.toLowerCase() === username.toLowerCase()
                 || game.players?.white?.user?.id?.toLowerCase() === username.toLowerCase();
    playerColor = isWhite ? 'white' : 'black';
  }

  let mistakes = 0, blunders = 0;
  for (let i = 1; i < moves.length && i < analysis.length; i++) {
    const ply = i + 1;
    const isOurMove = (playerColor === 'white' && ply % 2 === 1) ||
                      (playerColor === 'black' && ply % 2 === 0);
    if (!isOurMove) continue;

    const cpBefore = evalToCp(analysis[i - 1], playerColor);
    const cpAfter = evalToCp(analysis[i], playerColor);
    const wpBefore = cpToWinPct(cpBefore);
    const wpAfter = cpToWinPct(cpAfter);
    const wpDrop = wpBefore - wpAfter;

    if (wpDrop > 10) {
      mistakes++;
      if (wpDrop >= 15) blunders++;
    }
  }
  return { mistakes, blunders };
}


// ═══════════════════════════════════════════════════════════════════════════
// Tactic Scanner
// ═══════════════════════════════════════════════════════════════════════════

// Thresholds for tactic detection
const TACTIC_FIRST_MOVE_THRESHOLD = 0.30;  // win% gap for the first move of a tactic (0-1 scale)
const TACTIC_CONTINUATION_THRESHOLD = 0.20; // relaxed win% gap for subsequent moves in the chain
const TACTIC_MIN_USER_MOVES = 2;           // minimum solver moves for a tactic
const TACTIC_MIN_OPP_CP_LOSS = 100;        // opponent must have lost ≥100cp to create a tactic opportunity
const TACTIC_MIN_PLY = 6;                  // skip very early opening moves
const TACTIC_MIN_REMAINING = 2;            // need at least 2 plies remaining in game
const TACTIC_OPP_CANDIDATES = 3;           // number of opponent responses to try when building chains
const TACTIC_OPP_WP_CAP = 0.15;            // max win% gap (from opponent's view) an alternative response can be worse than best

/**
 * Convert eval object to win probability (0-1 scale) from the given perspective.
 */
function evalToWinProb(evalObj, perspective) {
  const cp = evalToCp(evalObj, perspective);
  return cpToWinPct(cp) / 100; // 0-1 scale
}

/**
 * Check if a position has a unique best move by comparing MultiPV results.
 * Returns { unique: bool, gap: number, bestMove: uci, bestEval: obj, secondEval: obj }
 */
function checkUniqueness(mpvResults, perspective, threshold) {
  if (!mpvResults || mpvResults.length < 1) {
    return { unique: false, gap: 0 };
  }
  // Only one legal move — trivially unique (but not interesting as a puzzle move
  // unless it's part of a longer chain)
  if (mpvResults.length === 1) {
    return {
      unique: true,
      gap: 1.0,
      forcedOnly: true,
      bestMove: mpvResults[0].move,
      bestEval: mpvResults[0],
      secondEval: null,
    };
  }

  const bestWp = evalToWinProb(mpvResults[0], perspective);
  const secondWp = evalToWinProb(mpvResults[1], perspective);
  const gap = bestWp - secondWp;

  return {
    unique: gap >= threshold,
    gap: Math.round(gap * 1000) / 1000,
    forcedOnly: false,
    bestMove: mpvResults[0].move,
    bestEval: mpvResults[0],
    secondEval: mpvResults[1],
  };
}

/**
 * Scan a single game for tactics. Requires Stockfish engine for MultiPV analysis.
 * Returns an array of tactic objects to store on the game.
 */
async function scanGameForTactics(sf, game, tacticDepth) {
  const moves = game.moves ? game.moves.split(' ').filter(Boolean) : [];
  const analysis = game.analysis || [];
  if (moves.length < 10 || analysis.length < 10) return [];

  const playerColor = game._playerColor || 'white';

  // Rebuild all FENs
  const chess = new Chess(game.initialFen || undefined);
  const fens = [chess.fen()]; // fens[0] = starting position
  const moveResults = [];     // moveResults[i] = chess.js move result for moves[i]
  for (let i = 0; i < moves.length; i++) {
    const result = chess.move(moves[i], { sloppy: true });
    if (!result) break;
    fens.push(chess.fen());
    moveResults.push(result);
  }

  // Identify candidate positions (user's turn, passes pre-filter)
  const candidates = [];
  for (let i = 0; i < moves.length; i++) {
    const ply = i + 1; // 1-indexed
    const isOurMove = (playerColor === 'white' && ply % 2 === 1) ||
                      (playerColor === 'black' && ply % 2 === 0);
    if (!isOurMove) continue;

    // Pre-filter checks
    if (ply < TACTIC_MIN_PLY) continue;
    if (moves.length - i < TACTIC_MIN_REMAINING) continue;

    // Pre-filter: the opponent's preceding move must have lost ≥100cp.
    // This means a tactic opportunity was created. We compare the eval
    // BEFORE the opponent's move (= after OUR previous move) to the eval
    // AFTER the opponent's move (= before OUR current move).
    // For the very first candidate (i <= 1), skip — no opponent move to evaluate.
    if (i >= 2 && analysis[i - 2] && analysis[i - 1]) {
      const cpBeforeOppMove = evalToCp(analysis[i - 2], playerColor);
      const cpAfterOppMove = evalToCp(analysis[i - 1], playerColor);
      const oppCpLoss = cpAfterOppMove - cpBeforeOppMove; // positive = opponent lost cp (good for us)
      // Also allow positions where a mate appeared (opponent blundered into mate)
      const mateAppeared = analysis[i - 1].mate !== undefined && analysis[i - 2].mate === undefined;
      if (oppCpLoss < TACTIC_MIN_OPP_CP_LOSS && !mateAppeared) continue;
    }

    // fenBefore this move = fens[i] (the position after i moves have been played,
    // i.e. position before moves[i])
    candidates.push({
      moveIdx: i,
      ply: ply,
      fen: fens[i],
    });
  }

  if (candidates.length === 0) return [];

  // Run MultiPV on all candidates
  const tactics = [];
  const alreadyCovered = new Set(); // plies already part of a found tactic

  for (const cand of candidates) {
    if (alreadyCovered.has(cand.ply)) continue;

    // MultiPV check at candidate position (first move uses stricter threshold)
    const mpv = await sf.evaluateMultiPV(cand.fen, tacticDepth, 2);
    const uniq = checkUniqueness(mpv, playerColor, TACTIC_FIRST_MOVE_THRESHOLD);

    if (!uniq.unique) continue;
    // Skip if the only reason it's "unique" is there's literally one legal move
    // (a single forced move isn't the start of an interesting tactic)
    if (uniq.forcedOnly) continue;

    // Chain walk: try to extend this into a multi-move tactic.
    // For opponent responses, we try the top N moves and pick the one
    // that produces the longest chain (best puzzle).
    const bestChain = await buildBestTacticChain(
      sf, cand, mpv, uniq, playerColor, tacticDepth
    );

    if (!bestChain || bestChain.length === 0) continue;

    // Count user moves in the chain
    const userMoveCount = bestChain.filter(m => m.isUser).length;
    if (userMoveCount < TACTIC_MIN_USER_MOVES) continue;

    // Calculate eval swing
    const evalBefore = cand.moveIdx > 0 ? analysis[cand.moveIdx - 1] : { eval: 0 };
    // Get eval of final position
    const finalFen = bestChain[bestChain.length - 1].fen;
    const finalEval = await sf.evaluate(finalFen, tacticDepth);

    const wpBefore = cpToWinPct(evalToCp(evalBefore, playerColor));
    const wpAfter = cpToWinPct(evalToCp(finalEval, playerColor));
    const wpSwing = wpAfter - wpBefore;

    // Check if the user actually played this tactic in the game.
    // Compare each move in sequence — if the opponent deviates, the game
    // diverged but the user still "found" the tactic (played correctly up
    // to that point). Only mark found=false if a USER move differs.
    let found = true;
    let gameIdx = cand.moveIdx;
    for (const tm of bestChain) {
      if (gameIdx >= moves.length) { found = false; break; }
      const gameMoveResult = moveResults[gameIdx];
      if (!gameMoveResult) { found = false; break; }
      // Compare UCI: build UCI from the game's move result
      const gameUci = gameMoveResult.from + gameMoveResult.to + (gameMoveResult.promotion || '');
      if (gameUci !== tm.uci) {
        if (tm.isUser) { found = false; }
        break; // game diverged from tactic line — stop comparing
      }
      gameIdx++;
    }

    tactics.push({
      startPly: cand.ply,
      fenBefore: cand.fen,
      playerColor: playerColor,
      moves: bestChain,
      evalBefore: evalBefore,
      evalAfter: finalEval,
      wpSwing: Math.round(wpSwing * 10) / 10,
      found: found,
    });

    // Mark covered plies so we don't start overlapping tactics
    for (let p = cand.ply; p < cand.ply + bestChain.length; p++) {
      alreadyCovered.add(p);
    }
  }

  return tactics;
}

/**
 * Build the best tactic chain from a candidate position by trying multiple
 * opponent responses at each step and picking the one that yields the longest chain.
 */
async function buildBestTacticChain(sf, cand, initialMpv, initialUniq, playerColor, tacticDepth) {
  return await walkChain(sf, cand.fen, cand.moveIdx, initialMpv, initialUniq, playerColor, tacticDepth, true);
}

/**
 * Recursively walk a tactic chain. Returns an array of tactic moves.
 * isFirstMove: true for the initial candidate position (uses stricter threshold).
 */
async function walkChain(sf, startFen, startMoveIdx, initialMpv, initialUniq, playerColor, tacticDepth, isFirstMove) {
  const tacticMoves = [];
  let currentFen = startFen;
  const currentChess = new Chess(currentFen);
  let moveIdx = startMoveIdx;
  let isFirst = isFirstMove;
  let reuseInitial = !!(initialMpv && initialUniq); // reuse initialMpv/initialUniq for the first iteration

  while (true) {
    // --- User's move (must be unique) ---
    const threshold = isFirst ? TACTIC_FIRST_MOVE_THRESHOLD : TACTIC_CONTINUATION_THRESHOLD;
    const userMpv = reuseInitial
      ? initialMpv
      : await sf.evaluateMultiPV(currentFen, tacticDepth, 2);
    const userUniq = reuseInitial
      ? initialUniq
      : checkUniqueness(userMpv, playerColor, threshold);

    reuseInitial = false;
    isFirst = false;

    if (!userUniq.unique) break;

    // Apply user's best move
    const userMoveUci = userUniq.bestMove;
    const userMove = currentChess.move(userMoveUci, { sloppy: true });
    if (!userMove) break;

    tacticMoves.push({
      uci: userMoveUci,
      san: userMove.san,
      fen: currentChess.fen(),
      isUser: true,
    });

    // Check for game-ending position
    if (currentChess.in_checkmate() || currentChess.in_stalemate() ||
        currentChess.in_draw()) {
      break; // tactic ends with checkmate/draw — that's fine
    }

    // --- Opponent's response: try top N moves, pick the one yielding the longest chain ---
    const oppMpv = await sf.evaluateMultiPV(currentChess.fen(), tacticDepth, TACTIC_OPP_CANDIDATES);
    if (!oppMpv || oppMpv.length === 0) break;

    // Compute win probability cap: opponent responses can't be more than TACTIC_OPP_WP_CAP
    // worse (from the opponent's perspective) than the best response
    const opponentColor = playerColor === 'white' ? 'black' : 'white';
    const bestOppWp = evalToWinProb(oppMpv[0], opponentColor);

    let bestOppResult = null; // { oppMove, oppSan, oppFen, continuation }

    for (let oi = 0; oi < Math.min(oppMpv.length, TACTIC_OPP_CANDIDATES); oi++) {
      // Skip opponent responses that are unrealistically bad (no human would play this)
      if (oi > 0) {
        const thisOppWp = evalToWinProb(oppMpv[oi], opponentColor);
        if (bestOppWp - thisOppWp > TACTIC_OPP_WP_CAP) continue;
      }

      const oppMoveUci = oppMpv[oi].move;

      // Test this opponent move in a scratch chess instance
      const scratchChess = new Chess(currentChess.fen());
      const oppMove = scratchChess.move(oppMoveUci, { sloppy: true });
      if (!oppMove) continue;

      // Check if game ends after opponent's move
      if (scratchChess.in_checkmate() || scratchChess.in_stalemate() ||
          scratchChess.in_draw()) {
        // Opponent's move ends the game — this is a valid (short) continuation
        if (!bestOppResult || 0 > bestOppResult.continuation.length) {
          bestOppResult = {
            oppMoveUci, oppSan: oppMove.san, oppFen: scratchChess.fen(),
            continuation: [], gameOver: true,
          };
        }
        continue;
      }

      // Check if there's a unique user response after this opponent move
      const nextUserMpv = await sf.evaluateMultiPV(scratchChess.fen(), tacticDepth, 2);
      const nextUserUniq = checkUniqueness(nextUserMpv, playerColor, TACTIC_CONTINUATION_THRESHOLD);

      if (!nextUserUniq.unique) {
        // This opponent response kills the tactic — try next opponent move
        // But if no better result found yet, record it as a dead end with 0 continuation
        if (!bestOppResult) {
          bestOppResult = {
            oppMoveUci, oppSan: oppMove.san, oppFen: scratchChess.fen(),
            continuation: [], gameOver: false, deadEnd: true,
          };
        }
        continue;
      }

      // This opponent response leads to a unique continuation — extend the chain
      // Recursively walk from here (no initial reuse, not first move)
      const subChain = await walkChain(
        sf, scratchChess.fen(), moveIdx + 2, null, null, playerColor, tacticDepth, false
      );

      if (!bestOppResult || subChain.length > bestOppResult.continuation.length) {
        bestOppResult = {
          oppMoveUci, oppSan: oppMove.san, oppFen: scratchChess.fen(),
          continuation: subChain, gameOver: false,
        };
      }
    }

    // No valid opponent response found at all
    if (!bestOppResult) break;

    // If all opponent responses killed the tactic, stop here (user's move is the last)
    if (bestOppResult.deadEnd && bestOppResult.continuation.length === 0) break;

    // Apply the best opponent response
    // (need to apply it on our actual currentChess instance)
    const appliedOpp = currentChess.move(bestOppResult.oppMoveUci, { sloppy: true });
    if (!appliedOpp) break;

    tacticMoves.push({
      uci: bestOppResult.oppMoveUci,
      san: bestOppResult.oppSan,
      fen: currentChess.fen(),
      isUser: false,
    });

    if (bestOppResult.gameOver) break;

    // Append the sub-chain continuation
    if (bestOppResult.continuation.length > 0) {
      tacticMoves.push(...bestOppResult.continuation);
      break; // sub-chain already walked to completion
    }

    // If continuation is empty but not a dead end, the sub-chain found no further moves
    break;
  }

  // Safety: limit tactic length to 14 moves (7 user moves)
  if (tacticMoves.length > 14) {
    return tacticMoves.slice(0, 14);
  }

  return tacticMoves;
}

/**
 * Re-evaluate the 'found' flag on an existing tactic without re-scanning.
 * Returns the corrected found value.
 */
function rescanTacticFound(game, tactic) {
  const moves = game.moves ? game.moves.split(' ').filter(Boolean) : [];
  if (!tactic.moves || tactic.moves.length === 0) return false;

  // Replay the game to get chess.js move results
  const chess = new Chess(game.initialFen || undefined);
  const moveResults = [];
  for (let i = 0; i < moves.length; i++) {
    const result = chess.move(moves[i], { sloppy: true });
    if (!result) break;
    moveResults.push(result);
  }

  // Compare tactic moves against actual game moves
  const startIdx = tactic.startPly - 1; // convert 1-indexed ply to 0-indexed
  let found = true;
  let gameIdx = startIdx;
  for (const tm of tactic.moves) {
    if (gameIdx >= moveResults.length) { found = false; break; }
    const gameMoveResult = moveResults[gameIdx];
    if (!gameMoveResult) { found = false; break; }
    const gameUci = gameMoveResult.from + gameMoveResult.to + (gameMoveResult.promotion || '');
    if (gameUci !== tm.uci) {
      if (tm.isUser) { found = false; }
      break; // game diverged — stop comparing
    }
    gameIdx++;
  }
  return found;
}


// ═══════════════════════════════════════════════════════════════════════════
// Gist Sync
// ═══════════════════════════════════════════════════════════════════════════

const GIST_GAMES_FILENAME = 'mistakelab_games.json';

async function readGistGames(token, gistId) {
  if (!token || !gistId) return null;
  log('Reading existing games from Gist…');
  try {
    const text = await httpGet(`https://api.github.com/gists/${gistId}`, {
      'Authorization': `token ${token}`,
    });
    const gist = JSON.parse(text);
    const file = gist.files?.[GIST_GAMES_FILENAME];
    if (!file) return null;
    // GitHub API truncates large files — fall back to raw_url
    let content = file.content;
    if (file.truncated || !content) {
      if (!file.raw_url) return null;
      log('File truncated by API, fetching via raw_url…');
      content = await httpGet(file.raw_url, { 'Authorization': `token ${token}` });
    }
    return JSON.parse(content);
  } catch (err) {
    logError(`Gist read failed: ${err.message}`);
    return null;
  }
}

async function writeGistGames(token, gistId, cacheObj, description) {
  if (!token || !gistId) return;
  log('Writing games to Gist…');
  try {
    await httpPatch(`https://api.github.com/gists/${gistId}`, {
      description: description || `MistakeLab analyzer — ${cacheObj.games?.length || 0} games`,
      files: { [GIST_GAMES_FILENAME]: { content: JSON.stringify(cacheObj) } }
    }, {
      'Authorization': `token ${token}`,
    });
    log('Gist updated successfully');
  } catch (err) {
    logError(`Gist write failed: ${err.message}`);
  }
}

function buildGameCacheObj(username, games, existingUsernames) {
  const sorted = games.slice().sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  const newest = sorted[0]?.createdAt || 0;
  // Merge username into the usernames set (preserves names from other platforms)
  const names = new Set((existingUsernames || []).map(u => u.toLowerCase()));
  if (username) names.add(username.toLowerCase());
  return {
    version: 2,
    usernames: [...names],
    games: sorted,
    lastGameCreatedAt: newest,
  };
}

// Extract usernames from a cache object (v1 compat: old single 'username' field)
function cacheUsernames(cache) {
  if (!cache) return [];
  if (Array.isArray(cache.usernames)) return cache.usernames;
  if (cache.username) return [cache.username];
  return [];
}


// ═══════════════════════════════════════════════════════════════════════════
// Local File I/O
// ═══════════════════════════════════════════════════════════════════════════

function loadLocalCache(filepath) {
  try {
    if (fs.existsSync(filepath)) {
      const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
      if (data && Array.isArray(data.games)) return data;
    }
  } catch {}
  return null;
}

function saveLocalCache(filepath, cacheObj) {
  try {
    fs.writeFileSync(filepath, JSON.stringify(cacheObj, null, 2), 'utf8');
  } catch (err) {
    logError(`Failed to save local file: ${err.message}`);
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// Position Eval Cache
// ═══════════════════════════════════════════════════════════════════════════

// Cache key: FEN without halfmove clock and fullmove number
function fenCacheKey(fen) {
  const parts = fen.split(' ');
  return parts.slice(0, 4).join(' '); // placement + side + castling + en passant
}

let posCache = {};       // key → { eval/mate, depth }
let posCacheHits = 0;
let posCacheMisses = 0;
const POS_CACHE_FILE = 'eval_cache.json';

function loadPosCache() {
  try {
    const filepath = path.join(path.dirname(CONFIG.outputFile), POS_CACHE_FILE);
    if (fs.existsSync(filepath)) {
      posCache = JSON.parse(fs.readFileSync(filepath, 'utf8'));
      const count = Object.keys(posCache).length;
      log(`Position cache loaded: ${count} entries`);
      return;
    }
  } catch {}
  posCache = {};
}

function savePosCache() {
  try {
    const filepath = path.join(path.dirname(CONFIG.outputFile), POS_CACHE_FILE);
    fs.writeFileSync(filepath, JSON.stringify(posCache), 'utf8');
  } catch (err) {
    logError(`Failed to save position cache: ${err.message}`);
  }
}

function getCachedEval(fen, minDepth) {
  const key = fenCacheKey(fen);
  const entry = posCache[key];
  if (entry && entry.depth >= minDepth) {
    posCacheHits++;
    if (entry.mate !== undefined) return { mate: entry.mate };
    return { eval: entry.eval };
  }
  return null;
}

function setCachedEval(fen, evalResult, depth) {
  const key = fenCacheKey(fen);
  const existing = posCache[key];
  // Only store if deeper or equal to existing
  if (!existing || depth >= existing.depth) {
    if (evalResult.mate !== undefined) {
      posCache[key] = { mate: evalResult.mate, depth };
    } else {
      posCache[key] = { eval: evalResult.eval, depth };
    }
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// Utilities
// ═══════════════════════════════════════════════════════════════════════════

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formatDuration(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}


// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  // Compute per-worker resources
  const numWorkers = CONFIG.workers;
  const threadsPerWorker = Math.max(1, Math.floor(CONFIG.threads / numWorkers));
  const hashPerWorker = Math.max(16, Math.floor(CONFIG.hash / numWorkers));

  log('═══════════════════════════════════════════════════');
  log(`  MistakeLab ${CONFIG.rescan ? 'Tactic Rescan' : CONFIG.scanTactics ? 'Tactic Scanner' : 'Analyzer'}`);
  log('═══════════════════════════════════════════════════');
  if (!CONFIG.scanTactics && !CONFIG.rescan) {
    log(`Username:    ${CONFIG.username}`);
    log(`Platform:    ${CONFIG.platform}`);
  }
  if (!CONFIG.rescan) {
    log(`Stockfish:   ${CONFIG.stockfishPath}`);
    log(`Depth:       ${CONFIG.scanTactics ? CONFIG.tacticDepth + ' (tactic MultiPV)' : CONFIG.depth}`);
    log(`Workers:     ${numWorkers} × ${threadsPerWorker} threads, ${hashPerWorker}MB hash each`);
  }
  if (!CONFIG.scanTactics && !CONFIG.rescan) log(`Max games:   ${CONFIG.maxGames}`);
  log(`Gist sync:   ${CONFIG.gistToken && CONFIG.gistId ? 'enabled' : 'disabled'}`);
  log(`Output file: ${CONFIG.outputFile}`);
  log('');

  // ── Phase 1: Start Stockfish worker pool (skip for rescan) ──
  const pool = [];
  if (!CONFIG.rescan) {
    for (let w = 0; w < numWorkers; w++) {
      const sf = new StockfishEngine(CONFIG.stockfishPath);
      await sf.start(threadsPerWorker, hashPerWorker);
      pool.push(sf);
    }
    log(`${numWorkers} Stockfish worker(s) ready`);
  }

  // Load position eval cache
  if (!CONFIG.rescan) loadPosCache();

  function quitAll() { for (const sf of pool) sf.quit(); }

  // ── Phase 2: Load existing analyzed games ──
  let existingGames = [];
  const existingIds = new Set();

  // Load from local file
  const localCache = loadLocalCache(CONFIG.outputFile);
  if (localCache) {
    existingGames = localCache.games;
    log(`Loaded ${existingGames.length} games from local cache`);
  }

  // Load from Gist
  if (CONFIG.gistToken && CONFIG.gistId) {
    const gistCache = await readGistGames(CONFIG.gistToken, CONFIG.gistId);
    if (gistCache && Array.isArray(gistCache.games)) {
      gistUsernames = cacheUsernames(gistCache);
      // Merge: gist games + local games, dedup by ID
      const byId = new Map();
      for (const g of existingGames) byId.set(g.id, g);
      for (const g of gistCache.games) byId.set(g.id, g);
      existingGames = [...byId.values()];
      log(`After merging with Gist: ${existingGames.length} total games`);
    }
  }

  for (const g of existingGames) {
    if (g.analysis && g.analysis.length > 0) existingIds.add(g.id);
    // Stamp player color on older games that don't have it yet
    if (g.analysis && g.analysis.length > 0 && !g._playerColor && CONFIG.username) {
      const isW = g.players?.white?.user?.name?.toLowerCase() === CONFIG.username.toLowerCase()
               || g.players?.white?.user?.id?.toLowerCase() === CONFIG.username.toLowerCase();
      g._playerColor = isW ? 'white' : 'black';
    }
  }
  log(`${existingIds.size} games already have analysis`);

  // Seed position cache from existing analyzed games (skip for rescan)
  if (!CONFIG.rescan) {
    const preSeedCount = Object.keys(posCache).length;
    let seededPositions = 0;
    for (const g of existingGames) {
      if (!g.analysis || !g.moves) continue;
      const moves = g.moves.split(' ').filter(Boolean);
      const analysis = g.analysis;
      const chess = new Chess();
      for (let i = 0; i < moves.length && i < analysis.length; i++) {
        const result = chess.move(moves[i], { sloppy: true });
        if (!result) break;
        const ev = analysis[i];
        if (ev && (ev.eval !== undefined || ev.mate !== undefined)) {
          setCachedEval(chess.fen(), ev, CONFIG.depth);
          seededPositions++;
        }
      }
    }
    const newEntries = Object.keys(posCache).length - preSeedCount;
    if (newEntries > 0) {
      log(`Seeded ${newEntries} new positions into eval cache from existing games (${seededPositions} total scanned)`);
    }
  }

  // ── Rescan mode: re-evaluate 'found' flags on existing tactics, no Stockfish needed ──
  if (CONFIG.rescan) {
    const gamesWithTactics = existingGames.filter(g => Array.isArray(g.tactics) && g.tactics.length > 0);
    log('');
    log('═══════════════════════════════════════════════════');
    log('  Tactic Rescan (found flags only)');
    log('═══════════════════════════════════════════════════');
    log(`Games with tactics: ${gamesWithTactics.length}`);

    let totalTactics = 0;
    let changed = 0;
    for (const game of gamesWithTactics) {
      const gameUrl = game.id.startsWith('chesscom_')
        ? `https://www.chess.com/game/live/${game.id.replace('chesscom_', '')}`
        : `https://lichess.org/${game.id}`;
      for (const t of game.tactics) {
        totalTactics++;
        const oldFound = t.found;
        t.found = rescanTacticFound(game, t);
        if (oldFound !== t.found) {
          changed++;
          const userMoves = t.moves.filter(m => m.isUser).map(m => m.san).join(' → ');
          log(`  ${oldFound ? '✓→✗' : '✗→✓'} Move ${Math.ceil(t.startPly / 2)}: ${userMoves}`);
          log(`    ${gameUrl}`);
        }
      }
    }

    log('');
    log(`Rescanned ${totalTactics} tactics across ${gamesWithTactics.length} games`);
    log(`Changed: ${changed} found flags`);

    if (changed === 0) {
      log('Nothing to update.');
      quitAll();
      return;
    }

    // Save locally always
    saveLocalCache(CONFIG.outputFile, buildGameCacheObj(CONFIG.username, existingGames, gistUsernames));
    log(`Saved to ${CONFIG.outputFile}`);

    // Ask for confirmation before Gist write
    if (CONFIG.gistToken && CONFIG.gistId) {
      const rl = createInterface({ input: process.stdin, output: process.stdout });
      const answer = await new Promise(resolve => {
        rl.question(`\nPush ${changed} changes to Gist? (y/N) `, resolve);
      });
      rl.close();
      if (answer.trim().toLowerCase() === 'y') {
        const cacheObj = buildGameCacheObj(CONFIG.username, existingGames, gistUsernames);
        await writeGistGames(CONFIG.gistToken, CONFIG.gistId, cacheObj,
          `MistakeLab rescan — ${changed} found flags updated`);
      } else {
        log('Skipped Gist write.');
      }
    }

    quitAll();
    return;
  }

  // ── Scan-tactics mode: scan existing games for tactics, then exit ──
  if (CONFIG.scanTactics) {
    const analyzed = existingGames.filter(g => g.analysis && g.analysis.length > 0);
    log('');
    log('═══════════════════════════════════════════════════');
    log('  Tactic Scanner');
    log('═══════════════════════════════════════════════════');
    log(`Games to scan:   ${analyzed.length}`);
    log(`Tactic depth:    ${CONFIG.tacticDepth} (MultiPV up to ${TACTIC_OPP_CANDIDATES})`);
    log(`Uniqueness:      ${TACTIC_FIRST_MOVE_THRESHOLD * 100}% first move / ${TACTIC_CONTINUATION_THRESHOLD * 100}% continuation`);
    log(`Opponent cap:    ${TACTIC_OPP_WP_CAP * 100}% max win% gap from best response`);
    log(`Pre-filter:      opponent must lose ≥${TACTIC_MIN_OPP_CP_LOSS}cp`);
    log(`Min user moves:  ${TACTIC_MIN_USER_MOVES}`);
    log('');

    let totalTactics = 0;
    let gamesWithTactics = 0;
    let gamesScanned = 0;

    // Graceful shutdown for tactic scanning
    let shuttingDown = false;
    process.on('SIGINT', async () => {
      if (shuttingDown) { process.exit(1); }
      shuttingDown = true;
      log('\nInterrupted — saving progress…');
      try {
        await saveAll(existingGames, undefined, `MistakeLab tactic scan — ${gamesScanned} games scanned, ${totalTactics} tactics found`);
      } catch (err) {
        logError(`Save failed: ${err.message}`);
      }
      quitAll();
      process.exit(0);
    });

    // Filter to scannable games
    const toScan = [];
    for (let i = 0; i < analyzed.length; i++) {
      const game = analyzed[i];
      if (game._tacticsScanned) continue;
      if (!game._playerColor) {
        log(`Skipping (no player color): ${game.id}`);
        continue;
      }
      toScan.push({ game, originalIdx: i, totalCount: analyzed.length });
    }
    log(`${toScan.length} games to scan (${analyzed.length - toScan.length} already scanned)\n`);

    // Shared queue index for parallel workers
    let nextScanIdx = 0;

    async function tacticWorkerLoop(sf, workerId) {
      while (nextScanIdx < toScan.length && !shuttingDown) {
        const idx = nextScanIdx++;
        if (idx >= toScan.length) break;
        const { game, originalIdx, totalCount } = toScan[idx];

        const opening = game.opening?.name || 'Unknown';
        const moveCount = game.moves ? game.moves.split(' ').length : 0;
        const gameUrl = game.id.startsWith('chesscom_')
          ? `https://www.chess.com/game/live/${game.id.replace('chesscom_', '')}`
          : `https://lichess.org/${game.id}`;
        log(`[${idx + 1}/${toScan.length}] W${workerId}: ${opening} (${moveCount} moves)`);
        log(`  ${gameUrl}`);

        const gameStart = Date.now();
        try {
          await sf.newGame();
          const tactics = await scanGameForTactics(sf, game, CONFIG.tacticDepth);
          game.tactics = tactics;
          game._tacticsScanned = true;

          const duration = ((Date.now() - gameStart) / 1000).toFixed(1);
          gamesScanned++;

          if (tactics.length > 0) {
            gamesWithTactics++;
            totalTactics += tactics.length;
            for (const t of tactics) {
              const userMoves = t.moves.filter(m => m.isUser).length;
              const label = t.found ? '✓ found' : '✗ missed';
              const moveNum = Math.ceil(t.startPly / 2);
              const solution = t.moves.filter(m => m.isUser).map(m => m.san).join(' → ');
              log(`  ⚡ ${userMoves}-move tactic at move ${moveNum} (${label}, wp swing ${t.wpSwing > 0 ? '+' : ''}${t.wpSwing}%): ${solution}`);
            }
          }
          log(`  ${tactics.length} tactic(s) — ${duration}s`);
        } catch (err) {
          logError(`  Failed to scan game ${game.id}: ${err.message}`);
          continue;
        }

        // Periodic save
        if (gamesScanned > 0 && gamesScanned % CONFIG.saveEvery === 0) {
          log(`Saving progress (${gamesScanned} games scanned)…`);
          await saveAll(existingGames, undefined, `MistakeLab tactic scan — ${gamesScanned} games scanned, ${totalTactics} tactics found`);
        }
      }
    }

    // Launch parallel workers
    await Promise.all(pool.map((sf, i) => tacticWorkerLoop(sf, i + 1)));

    // Final save and summary
    log('');
    log('═══════════════════════════════════════════════════');
    log('  Tactic Scan Complete');
    log('═══════════════════════════════════════════════════');
    log(`Games scanned:      ${gamesScanned}`);
    log(`Games with tactics: ${gamesWithTactics}`);
    log(`Total tactics found:${totalTactics}`);
    log(`Total time:         ${formatDuration((Date.now() - START_TIME) / 1000)}`);

    await saveAll(existingGames, undefined, `MistakeLab tactic scan — ${gamesScanned} games scanned, ${totalTactics} tactics found`);
    quitAll();
    log('Done!');
    return;
  }

  // ── Phase 3: Fetch games ──
  let fetchedGames;
  if (CONFIG.platform === 'chesscom') {
    fetchedGames = await fetchChesscomGames(CONFIG.username, CONFIG.maxGames);
  } else {
    fetchedGames = await fetchLichessGames(CONFIG.username, CONFIG.maxGames);
  }

  // Filter out already-analyzed games and games with server analysis
  const toAnalyze = fetchedGames.filter(g => {
    if (!g.moves || g.moves.split(' ').length < 4) return false;
    if (existingIds.has(g.id)) return false;
    // If Lichess already provided analysis, keep it but don't re-analyze
    if (g.analysis && g.analysis.length > 0) {
      existingGames.push(g);
      existingIds.add(g.id);
      return false;
    }
    return true;
  });

  log(`${toAnalyze.length} games need local analysis`);

  if (toAnalyze.length === 0) {
    log('Nothing to do! All games are already analyzed.');
    quitAll();
    // Still save merged data (may have new _playerColor stamps)
    await saveAll(existingGames, undefined, `MistakeLab analyzer — no new games, merged data save`);
    return;
  }

  // Estimate time
  const avgMoves = toAnalyze.reduce((sum, g) => sum + g.moves.split(' ').length, 0) / toAnalyze.length;
  const estSecondsPerPos = 0.3; // rough estimate for native SF at depth 18
  const estTotal = toAnalyze.length * avgMoves * estSecondsPerPos;
  log(`Estimated time: ${formatDuration(estTotal)} (${Math.round(avgMoves)} avg moves/game, ~${estSecondsPerPos}s/position)`);
  log('');

  // ── Phase 4: Analyze games ──
  const analyzedGames = [...existingGames];
  let gamesAnalyzed = 0;
  let totalPositions = 0;
  const gameTimings = [];

  // Graceful shutdown
  let shuttingDown = false;
  process.on('SIGINT', async () => {
    if (shuttingDown) { process.exit(1); }
    shuttingDown = true;
    log('\nInterrupted — saving progress…');
    try {
      await saveAll(analyzedGames, gamesAnalyzed, `MistakeLab analyzer — ${gamesAnalyzed} games analyzed`);
      savePosCache();
    } catch (err) {
      logError(`Save failed: ${err.message}`);
    }
    quitAll();
    process.exit(0);
  });

  for (let i = 0; i < toAnalyze.length; i++) {
    if (shuttingDown) break;
    const game = toAnalyze[i];
    const moveCount = game.moves.split(' ').length;
    const opening = game.opening?.name || 'Unknown';
    const date = game.createdAt ? new Date(game.createdAt).toLocaleDateString() : '?';

    log(`[${i + 1}/${toAnalyze.length}] Analyzing: ${opening} (${date}, ${moveCount} moves, ${game.speed || '?'})…`);

    const gameStart = Date.now();
    try {
      const analyzed = await analyzeGame(pool, game, CONFIG.depth);
      if (analyzed) {
        // Stamp player color so MistakeLab knows which side we played
        const isWhite = analyzed.players?.white?.user?.name?.toLowerCase() === CONFIG.username.toLowerCase()
                     || analyzed.players?.white?.user?.id?.toLowerCase() === CONFIG.username.toLowerCase();
        analyzed._playerColor = isWhite ? 'white' : 'black';

        const { mistakes, blunders } = countMistakes(analyzed, CONFIG.username);
        const duration = (Date.now() - gameStart) / 1000;
        gameTimings.push(duration);
        totalPositions += moveCount;
        gamesAnalyzed++;

        analyzedGames.push(analyzed);
        existingIds.add(analyzed.id);

        const avgTime = gameTimings.reduce((a, b) => a + b, 0) / gameTimings.length;
        const remaining = (toAnalyze.length - i - 1) * avgTime;

        log(`  ✓ ${mistakes} mistakes (${blunders} blunders) — ${duration.toFixed(1)}s — ETA: ${formatDuration(remaining)}`);
      }
    } catch (err) {
      logError(`  Failed to analyze game ${game.id}: ${err.message}`);
      // Try to continue with next game
      continue;
    }

    // Periodic save
    if (gamesAnalyzed > 0 && gamesAnalyzed % CONFIG.saveEvery === 0) {
      log(`Saving progress (${gamesAnalyzed} games analyzed so far)…`);
      await saveAll(analyzedGames, gamesAnalyzed, `MistakeLab analyzer — ${gamesAnalyzed} games analyzed`);
      savePosCache();
    }
  }

  // ── Phase 5: Final save ──
  log('');
  log('═══════════════════════════════════════════════════');
  log('  Analysis Complete');
  log('═══════════════════════════════════════════════════');
  log(`Games analyzed:    ${gamesAnalyzed}`);
  log(`Total positions:   ${totalPositions}`);
  log(`Cache hits:        ${posCacheHits} (${totalPositions > 0 ? Math.round(100 * posCacheHits / (posCacheHits + posCacheMisses)) : 0}%)`);
  log(`Positions/sec:     ${(pool.reduce((s, w) => s + w.positionsEvaluated, 0) / ((Date.now() - START_TIME) / 1000)).toFixed(1)}`);
  log(`Total time:        ${formatDuration((Date.now() - START_TIME) / 1000)}`);
  log(`Total games in DB: ${analyzedGames.length}`);

  await saveAll(analyzedGames, gamesAnalyzed, `MistakeLab analyzer — ${gamesAnalyzed} games analyzed`);
  savePosCache();
  log(`Eval cache: ${Object.keys(posCache).length} entries saved`);
  quitAll();
  log('Done!');
}

async function saveAll(games, newlyAnalyzed, description) {
  const cacheObj = buildGameCacheObj(CONFIG.username, games, gistUsernames);

  // Save locally (synchronous — always completes)
  saveLocalCache(CONFIG.outputFile, cacheObj);
  const newStr = newlyAnalyzed !== undefined ? ` (${newlyAnalyzed} newly analyzed this session)` : '';
  log(`Saved ${games.length} games to ${CONFIG.outputFile}${newStr}`);

  // Save to Gist (await so it completes before process exits)
  if (CONFIG.gistToken && CONFIG.gistId) {
    await writeGistGames(CONFIG.gistToken, CONFIG.gistId, cacheObj, description);
  }
}

// ─── Run ───
main().catch(err => {
  logError(err.message);
  process.exit(1);
});
