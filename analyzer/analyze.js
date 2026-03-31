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
//   --threads     Stockfish threads (default: half your CPUs)
//   --hash        Stockfish hash table in MB (default: 256)
//   --max         Max games to fetch (default: 500)
//   --gist-token  GitHub personal access token (gist scope)
//   --gist-id     Existing Gist ID for MistakeLab sync
//   --save-every  Save progress every N games (default: 5)
//   --output      Local output file (default: analyzed_games.json)
//   --rated-only  Only analyze rated games (default: true)
//   --time-controls Comma-separated: bullet,blitz,rapid,classical (default: all)
//
// Examples:
//   node analyze.js --username DrNykterstein --stockfish ./stockfish
//   node analyze.js --username DrNykterstein --stockfish "C:\stockfish\stockfish.exe" --depth 20 --threads 8
//   node analyze.js --username magnus --platform chesscom --stockfish ./stockfish
//   node analyze.js --username user1 --stockfish ./stockfish --gist-token ghp_xxx --gist-id abc123
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
  maxGames:      parseInt(args.max) || 500,
  gistToken:     args['gist-token'] || process.env.MISTAKELAB_GIST_TOKEN || '',
  gistId:        args['gist-id'] || process.env.MISTAKELAB_GIST_ID || '',
  saveEvery:     parseInt(args['save-every']) || 5,
  outputFile:    args.output || 'analyzed_games.json',
  ratedOnly:     args['rated-only'] !== 'false',
  timeControls:  args['time-controls'] ? args['time-controls'].split(',') : null,
};

if (!CONFIG.username || !CONFIG.stockfishPath) {
  console.error('Usage: node analyze.js --username <name> --stockfish <path> [options]');
  console.error('Run with --help for all options.');
  process.exit(1);
}

// ─── Logging ───
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
   * Evaluate a position. Returns { eval: cp } or { mate: N } from WHITE's perspective.
   */
  async evaluate(fen, depth) {
    this._send('ucinewgame');
    this._send('isready');
    await this._waitFor(line => line === 'readyok');

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
async function analyzeGame(sf, game, depth) {
  const moves = game.moves.split(' ').filter(Boolean);
  if (moves.length < 4) return null; // too short

  const analysis = [];
  const chess = new Chess();

  for (let i = 0; i < moves.length; i++) {
    const san = moves[i];
    const result = chess.move(san, { sloppy: true });
    if (!result) {
      logError(`  Invalid move "${san}" at ply ${i + 1}, stopping analysis`);
      break;
    }

    // Evaluate the position after this move
    const fen = chess.fen();
    const evalResult = await sf.evaluate(fen, depth);
    analysis.push(evalResult);
  }

  return { ...game, analysis };
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
    return JSON.parse(file.content);
  } catch (err) {
    logError(`Gist read failed: ${err.message}`);
    return null;
  }
}

async function writeGistGames(token, gistId, cacheObj) {
  if (!token || !gistId) return;
  log('Writing games to Gist…');
  try {
    await httpPatch(`https://api.github.com/gists/${gistId}`, {
      files: { [GIST_GAMES_FILENAME]: { content: JSON.stringify(cacheObj) } }
    }, {
      'Authorization': `token ${token}`,
    });
    log('Gist updated successfully');
  } catch (err) {
    logError(`Gist write failed: ${err.message}`);
  }
}

function buildGameCacheObj(username, games) {
  const sorted = games.slice().sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  const newest = sorted[0]?.createdAt || 0;
  return {
    version: 1,
    username: username.toLowerCase(),
    games: sorted,
    lastGameCreatedAt: newest,
  };
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
  log('═══════════════════════════════════════════════════');
  log('  MistakeLab Analyzer');
  log('═══════════════════════════════════════════════════');
  log(`Username:    ${CONFIG.username}`);
  log(`Platform:    ${CONFIG.platform}`);
  log(`Stockfish:   ${CONFIG.stockfishPath}`);
  log(`Depth:       ${CONFIG.depth}`);
  log(`Threads:     ${CONFIG.threads}`);
  log(`Hash:        ${CONFIG.hash}MB`);
  log(`Max games:   ${CONFIG.maxGames}`);
  log(`Gist sync:   ${CONFIG.gistToken && CONFIG.gistId ? 'enabled' : 'disabled'}`);
  log(`Output file: ${CONFIG.outputFile}`);
  log('');

  // ── Phase 1: Start Stockfish ──
  const sf = new StockfishEngine(CONFIG.stockfishPath);
  await sf.start(CONFIG.threads, CONFIG.hash);

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
    if (g.analysis && g.analysis.length > 0 && !g._playerColor) {
      const isW = g.players?.white?.user?.name?.toLowerCase() === CONFIG.username.toLowerCase()
               || g.players?.white?.user?.id?.toLowerCase() === CONFIG.username.toLowerCase();
      g._playerColor = isW ? 'white' : 'black';
    }
  }
  log(`${existingIds.size} games already have analysis — will be skipped`);

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
    sf.quit();
    // Still save merged data (may have new _playerColor stamps)
    await saveAll(existingGames);
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
      await saveAll(analyzedGames, gamesAnalyzed);
    } catch (err) {
      logError(`Save failed: ${err.message}`);
    }
    sf.quit();
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
      const analyzed = await analyzeGame(sf, game, CONFIG.depth);
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
      await saveAll(analyzedGames, gamesAnalyzed);
    }
  }

  // ── Phase 5: Final save ──
  log('');
  log('═══════════════════════════════════════════════════');
  log('  Analysis Complete');
  log('═══════════════════════════════════════════════════');
  log(`Games analyzed:    ${gamesAnalyzed}`);
  log(`Total positions:   ${totalPositions}`);
  log(`Positions/sec:     ${(sf.positionsEvaluated / ((Date.now() - START_TIME) / 1000)).toFixed(1)}`);
  log(`Total time:        ${formatDuration((Date.now() - START_TIME) / 1000)}`);
  log(`Total games in DB: ${analyzedGames.length}`);

  await saveAll(analyzedGames, gamesAnalyzed);
  sf.quit();
  log('Done!');
}

async function saveAll(games, newlyAnalyzed) {
  const cacheObj = buildGameCacheObj(CONFIG.username, games);

  // Save locally (synchronous — always completes)
  saveLocalCache(CONFIG.outputFile, cacheObj);
  const newStr = newlyAnalyzed !== undefined ? ` (${newlyAnalyzed} newly analyzed this session)` : '';
  log(`Saved ${games.length} games to ${CONFIG.outputFile}${newStr}`);

  // Save to Gist (await so it completes before process exits)
  if (CONFIG.gistToken && CONFIG.gistId) {
    await writeGistGames(CONFIG.gistToken, CONFIG.gistId, cacheObj);
  }
}

// ─── Run ───
main().catch(err => {
  logError(err.message);
  process.exit(1);
});
