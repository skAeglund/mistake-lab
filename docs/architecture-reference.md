MistakeLab Architecture Reference

This is the deep-dive companion to the short always-on project instructions.
Workflow basics (clone/PAT/commit discipline), the 8 top-level mode flags, the
GitHub Issues trigger condition, and the Stockfish `stop` constraint are NOT
repeated here — they live in the always-on core and apply regardless of what
this document says. Everything below is organized by subsystem under
─── SECTION ─── headers; search by subsystem name or function name.

─── TECH STACK & FILE STRUCTURE ───

Single index.html (~27,600 lines, ~1.23MB), vanilla JS, no build tools.
- CDN deps: jQuery 3.7.1, chess.js 0.10.3, chessboard.js 1.0.0
- onnxruntime-web 1.21.0 (dynamically loaded, not in HTML)
- Stockfish 18 WASM lite-single as two Web Workers: main sfWorker + eval evalSfWorker
- Maia 3 ONNX (~43.5MB) via onnxruntime-web
- Piece images from lichess1.org (cburnett SVG)
- Analyzer uses chess.js 0.13.4 (Node.js) — a different version than the browser

File structure:
mistake-lab/
├── index.html           Single-file app (~27,600 lines, ~1.23MB)
├── bookmarklet.html     Chessable bookmarklet helper
├── manifest.json        NO orientation field → respects system orientation lock
├── sw.js                Service Worker v2 ('mistakelab-v2'). App shell network-first;
│                          engine binaries, Maia ONNX, move-mapping JSONs, versioned
│                          CDN deps, lichess1.org pieces, Google Fonts all cache-first.
├── testing.html
├── docs/architecture-reference.md   This file
├── docs/issues/          Images for GitHub issue bodies (committed, not pasted)
├── engine/stockfish-18-lite-single.{js,wasm}
├── maia/{maia3_simplified.onnx, all_moves_maia3.json, all_moves_maia3_reversed.json}
└── analyzer/{analyze.js, maia.js, package.json, package-lock.json}

GitHub: https://github.com/skAeglund/mistake-lab (public repo)

─── GITHUB ISSUES WORKFLOW (full mechanics) ───

Gate condition (in the always-on core): only run this when the person explicitly
  asks to work from the tracker.

Claude has READ access only (via HTML scrape — api.github.com is firewalled in the
  sandbox, github.com is not). No commenting/closing/labeling; auto-close on push
  (via "closes #N" in the commit message) is the only write path.

List open issues:
  curl -sL -H "User-Agent: Mozilla/5.0" https://github.com/skAeglund/mistake-lab/issues -o /tmp/issues.html
  Parse the SSR JSON blob at <script type="application/json" data-target="react-app.embeddedData">:
  payload.preloadedQueries[0].result.data.repository.search.edges → each node has number/state/titleHtml.
  Write the parser to a .py file rather than inlining in bash -c (quote-escaping breaks one-liners).

Fetch a specific issue:
  curl -sL -H "User-Agent: Mozilla/5.0" https://github.com/skAeglund/mistake-lab/issues/<N> -o /tmp/issueN.html
  Same SSR blob; body is at payload.structured_data.{headline, articleBody}.

If the person didn't name a specific issue, list the opens and ask which to work on.

Images in issues: (a) preferred — committed to docs/issues/<ID>.png and referenced
  as ![](docs/issues/<ID>.png) in the issue body, readable from the clone; (b) placed
  in project knowledge and referenced by filename; (c) pasted via GitHub's UI —
  NOT accessible (user-attachments 302s to a signed S3 URL outside the sandbox
  allowlist). Ask for (a) or (b) if diagnosis needs an image.

Commit message MUST include "closes #N" (or "fixes #N") to auto-close on push:
  "<Short title>, closes #N

   <root cause / fix / scope>"
The PAT needs BOTH Contents:read-write AND Issues:read-write scopes — without the
  latter, "closes #N" only creates a reference event, doesn't flip issue state, and
  this can't be retroactively triggered after the fact if discovered later.

Before committing: re-read the issue body and confirm the specific symptom is
  resolved (not just "probably fixed"), then run the usual JS/CSS validation and
  buildVersion update.

Batching: default is one issue per commit. OK to batch independent trivial UI fixes
  (label/visibility toggles, no shared state) — list all in the message: "closes #11,
  closes #12". NEVER batch anything touching continuation/SRS/tactic/repertoire flow
  or eval-worker state. Propose a grouping for approval; don't pre-decide to batch.

Claude can draft text for a new issue but cannot file it — say so and wait for Anders
  to file it via the app or web UI.

─── ITEM TYPES ───

allMistakes[] holds five item types, processed by the same SRS system:
  type: 'mistake'     — single-move, win% drop > 10%
  type: 'tactic'      — multi-move from game.tactics OR saved practice tactics
                         (includes analyzer/scanner-detected tactics AND user-authored
                         "sequences" converted from mistakes via Analysis Mode —
                         see SEQUENCE CONVERSION; sequences carry `_isSequence: true`)
  type: 'advantage'   — blown winning edge (silent continuation drill)
  type: 'repertoire'  — deviation from loaded Lichess study
  type: 'practice'    — synthetic item for filter continuation practice (NOT in SRS queue)

posId(m):
  type='repertoire' → `r_${m.positionKey}`
  type='tactic'     → `${m.gameId}_t${m.movePly}`
  type='advantage'  → `${m.gameId}_a${m.movePly}`
  type='mistake'    → `${m.gameId}_${m.movePly}`  (NO prefix between id and ply)

Left panel tabs: Games / Repertoire / Review.
Severity chips skip tactics/advantage. Type chips: Mistakes / ⚡ Tactics / ♛ Convert.
Filters: color, severity, platform, type, speed, rated, hideTimeTrouble. Multi-select
Sets via toggleFilter(). State persisted in 'mistakelab_filters'.
Speed chips (5): bullet / blitz / rapid / classical / correspondence (✉️, also covers
  MistakeLab Game Review history entries — see GAME REVIEW HISTORY).

The Games tab is a CHRONOLOGICAL MERGE of fetched Lichess/chess.com games AND
  MistakeLab Game Review history snapshots, the latter styled with `.review-hist-card`
  (purple left bar). `reviewPassesGameFilters()` applies color/speed filtering to
  review entries (they have no platform, always the 'correspondence' speed bucket).

`window._deferredEngineLine` (see ENGINE LINES) is a stashed-payload pattern, NOT
  a mode flag — gated by `deferredLineBranchReady()`, which checks all eight mode
  flags plus several transient UI states (browsingLine, engineLine, contLineBrowsing,
  contLineReviewMode, repertoireDrillMode, repContActive, repLeadUpActive,
  noteJumpedAway, an open settings panel).

─── EXTRACTION & CACHES ───

Per-game fingerprint = { tacticsLen, hasClocks, analysisLen }. Changes bust cache for that game.

Mistakes cache (localStorage, key `mistakelab_mistakes_<username>`):
  MISTAKES_CACHE_VERSION = 8
  MISTAKES_CACHE_PARAMS = { wpDropThreshold: 10, advantageThreshold: 300, advantageMinMoves: 2 }
  Schema: { version, params, knownUsernamesSig, games: {gameId → fingerprint},
            mistakes: [...sans .game],
            advantageCandidates: {gameId → {peakCp, peakFen, peakPly} | null} }
  Slimmed to ~100KB — does NOT store fenIndex/moveStatsIndex/PMI (lazy-built).
  knownUsernamesSig = sorted lowercase usernames joined by '|'. Cache invalidates
    when sig differs (cross-platform username added → playerColor may flip).

FenData cache (IndexedDB — localStorage has ~5MB quota, ~2.86MB on Brave):
  DB name: 'mistakelab', version 1 (FEN_DB_VERSION=1), store: 'fendata'
  Key = gameId, value = { moveStats: [{posKey, moveUci, moveSan}], finalFen }
  Uses getAll()/getAllKeys() — NOT cursor (50× faster on mobile)
  startFenDataPreload() at showMainApp; opening explorer waits UNCONDITIONALLY
  for preload (chess.js on main thread blocks IDB result delivery if raced).
  Never bump FEN_DB_VERSION casually — a version-bump open BLOCKS (onblocked) if a
  second tab still holds a v1 connection, breaking the preload wait. This is exactly
  why Game Review history uses its own separate IndexedDB — see GAME REVIEW HISTORY.

Games cache: 'mistakelab_games_<username>', v2 schema with usernames: [] array.
Progress/Gist cache: 'mistakelab_progress_cache' for airplane mode.
Progress dirty sentinel: 'mistakelab_progress_dirty' — set on every debounced
  mutation, cleared after a successful syncToGist; survives tab kills.
Eval cache: 'mistakelab_eval_cache' (LS) + Gist file 'mistakelab_evals.json'.
  EVAL_CACHE_MIN_DEPTH=20, EVAL_CACHE_SYNC_THRESHOLD=10, EVAL_CACHE_SYNC_DEBOUNCE=15000.
  evalCacheKey = fenPositionKey(fen) — first 4 FEN fields, strip clocks AND
    non-pseudo-legal EP (transposition-aware).
  evalCacheShouldOverwrite: deeper-AND-at-least-as-wide wins; deeper-but-narrower
    is REJECTED (preserves PV2/PV3 for Great/Miss classification).

Startup (cache-first path, extractAllMistakes → showMainApp):
  1. If localGames + progress in localStorage: render immediately, then background
     sync Gist (merges fresh remote into local via mergeGistData, not overwrite).
     Re-extract only if gamesDigest changed AND currentMistakeIdx < 0.
  2. Cold start (no local cache): block on Gist fetch, then render.
  3. loadFromGist() must complete before extractAllMistakes() so gistData.positions
     (invalidations, invalidatedLines) is populated first.

Date filter (Settings → Training → Date filter, `mistakelab_date_filter` LS key):
  dateFilterValue = 'all' | rolling preset '3m'|'6m'|'12m'|'24m' (recalculated from
  today via getDateFilterCutoffMs on every load) | custom 'YYYY-MM-DD'.
  applyDateFilterToGames(games) filters by game.createdAt and is called at all 5
  `allGames = ` assignment sites inside fetchGames (fast-path render, phase-2
  background-sync-changed, Gist cold-start, standalone-cache-render, standalone-
  post-fetch-merge) — always AFTER saveGamesCache/buildGameCacheObj is given the
  FULL unfiltered set, so the localStorage games cache and Gist never shrink; only
  the in-memory allGames used for extraction/display is trimmed. This means
  extractAllMistakes and detectRepertoireDeviations (both iterate allGames) only
  process games newer than the cutoff — the actual load-time win, since the Gist
  fetch itself is one monolithic JSON regardless of the filter. Switching back to
  'all' restores everything with no re-fetch needed. Takes effect on next load
  (restart), matching the Start View setting's pattern — no mid-session re-extract.
  Game Review history entries respect the same cutoff but ONLY at render time,
  inside renderGameList's reviewHistory loop (checked against s.ts) — reviewHistory
  itself is never trimmed, so syncReviewHistoryToGist's read-merge-write still
  operates on the full set and can't reintroduce/lose entries across the toggle.

Startup order in showMainApp() (appShown guard prevents double init):
  loadContSettings → loadVoiceSettings → initStockfish → prefetchMaia (I/O only)
  → migrateOldLocalStorageFenCache → migrateLegacyFenDataPosKeys
  → navigator.storage.persist() → startFenDataPreload
  → loadRepertoireTrie (then detectRepertoireDeviations + updateRepertoireBadge)
  → check for interrupted cont session (≥ 2 user moves) → enterStartView()

Advantage candidates: O(games + mistakes) via pre-indexed Map<gameId, mistakes[]>.
Advantage detection: ply > 15, cpBefore ≥ 300 for ≥ 2 consecutive user moves.
Peak selection excludes plies overlapping with tactics. Note: this exclusion only
  works for analyzer-scanned games — unscanned games can produce false AC peaks.

practiceMistakes & practiceTactics merge into allMistakes AFTER analyzer-derived
  mistakes/tactics, BEFORE the repertoire-trie filter pass — so saved practice items
  matching the trie also get filtered out. Sequences merge through this same path.

Shared helpers worth knowing: ensureGistData() (lazy-init gistData), lsSetSafe(key,
  value, ctx) (LS write with quota-failure surfacing), invalidateOpeningIndices()
  (resets fen/move indices, bumps openingBuildSession), flushPendingSyncs()
  (pagehide/beforeunload flush, dual-fire + bfcache guarded).

Test injections still present (remove before final release):
  Test game: id '_test_clocks_6sS121tG' (Zukertort Sicilian Invitation, with clocks)
  Test tactic: id '_test_bxf7' Bxf7+ puzzle with TWO alt lines (Kxf7→Ne6 / Kxf7→Qf3#)

─── START VIEW (startup destination setting) ───

`startView` — persisted setting ('mistakelab_start_view') controlling what's shown
  on load; four legal values, 'games' fallback if unset/invalid.
`enterStartView()` — the single startup dispatcher (replaces a hard-coded
  enterOpeningFilter() call). Settings modal: "Start view" dropdown, first row of
  the Training section.
`showNeutralStartBoard()` — desktop neutral board render for non-Opening-Filter start views.

Color-filter / orientation decoupling: the color filter is only force-synced to
  board orientation (`syncColorFilterToOrientation`) when an ACTUAL position filter
  is active in the opening explorer — never on bare entry. Forced syncs never call
  saveFilters(), so localStorage stays the source of truth for the user's real chip
  selection. Navigating back to the start position or clearing the filter calls
  `restorePersistedColorFilter()`. If the persisted color filter is exactly
  black-only, the board starts flipped on fresh explorer entry.

─── MOVE EVALUATION & SRS ───

winPct(cp) = 100 / (1 + 10^(−cp/400)). Mistake threshold: win% drop > 10%.

SRS grades (srsRecorded guards retry re-grading):
  Mistakes (firstAttemptFailed always → Again):
    isExactBest or wpDrop ≤ 2  → Easy (4)
    wpDrop ≤ 5                 → Good (3)
    wpDrop ≤ 10                → Hard (2)
    wpDrop > 10                → Again (1)  (recorded immediately on attempt 1)
    hintUsed                   → Again (1) override
  Tactics (including sequences):
    any wrong → Again (1); hint only → Hard (2); perfect → Easy (4)
  Repertoire (time-based, currentAttempts ≤ 1 && !hintUsed):
    < 3s & correct → Easy (4); < 15s & correct → Good (3);
    ≥ 15s & correct → Hard (2); any wrong or hint → Again (1)
  Skip → Again (1) unless already graded or synthetic (_practice/_resumed);
    surfaced via toast (feedback container is cleared by the navigation).

isExactBest: move.san === bestMoveSan OR userUci === bestMoveUci OR wpDrop < 0.5.

FSRS-5 (fsrs_review(card, grade, now)):
  - Due-date boundaries use the LOCAL calendar day, not UTC.
  - fsrs_review treats an unparseable c.lastReview as "no prior review" (elapsed=0)
    rather than propagating NaN and silently dropping the card from the queue.
  - w[17]/w[18] are reserved for the (unimplemented) short-term scheduler.
  - Synthetic in-flight items (practiceContItem, resumed practice) are excluded
    from the review queue.

srsRecorded resume semantics: cont-session persistence stores srsRecordedAtSave
  (= srsRecorded at save time) and itemType. On resume, srsRecorded =
  session.srsRecordedAtSave if defined, else true. Advantage items default to
  false on resume (others true) — a blanket true default would make resumed
  advantage drills silently never grade.

Six-tier classification (classifyWithContext):
  Base (classifyWpDrop): best / excellent / good / inaccuracy / mistake / blunder
  Great: best/excellent AND pvGap ≥ 15% AND bestCp < 1000
  Miss:  inaccuracy AND bestCp ≥ 300 AND pvGap ≥ 10%
  Book override: any move matching repertoireTrie at preFen → 'book' (applied in
    both live evaluateMove and silent eval re-classification)

MOVE_CLASSIFICATION map (symbol / color):
  great '!' #5b9bd5 (blue)        best '★' #62cf8e (green)
  excellent '!' #96c84a (lime)    good '✓' #96bc4b (lime)
  book '📖' #8b7dd8 (purple)      miss '✕' #e04040 (red)
  inaccuracy '?!' #e8a62d (yel)   mistake '?' #e07a3a (orange)
  blunder '??' #ef5f5f (red)

cpToWinPct(cp): win%-based helper also used by sequence validation (see below) —
  keeps the "good enough" bar for a sequence alt-line consistent with what would
  count as a mistake in a live drill.

─── SEQUENCE CONVERSION (mistake → drillable multi-move sequence) ───

Lets a mistake be turned into a multi-move drillable tactic by building lines out
in Analysis Mode, independent of analyzer/scanner-detected tactics. Two entry
points feed the same save function but route differently:
  - Plain mistake review → "💾 Sequence" (`seqSaveContext = {kind:'mistake',
    mistakeIdx}`): saving REPLACES (invalidates) the original single-move mistake.
  - Game Review key move → "🔍 Sequence" (`seqSaveContext = {kind:'review',
    playerColor, wpDrop}`, opts.seqReview to enterAnalysisMode): plain additional
    save, nothing invalidated; tree root pins to the position BEFORE the key move.

Authoring surface = the analysis tree: first-child chain from root = main line;
  every other root-to-leaf path = an alt line. `analysisTreeToSequenceLines
  (playerColor)` walks the tree at SAVE time (not prompt time, since the user may
  keep exploring while the dialog is open), re-annotating each user move with its
  PV eval (`seqPvsForFen`, side-to-move POV).

Analysis-tree root selection (enterAnalysisMode):
  - Sequence-from-review: root = seqReview.rootFen; the played move is seeded as
    line move 1 (removable via 🗑/Delete).
  - Plain mistake entry (no review/tactic/continuation context): root = m.fenBefore
    (the PRE-mistake position — required for the pre-mistake position to be
    reachable/saveable; it's opponent-to-move at the post-move FEN otherwise).
    Why the pre-mistake position matters / why it's the tree root: without this,
    the analysis tree would be rooted at the post-move FEN, which is opponent-to-
    move — the pre-mistake position itself would never appear in the tree at all,
    so there'd be no node to save a sequence FROM. Rooting at m.fenBefore is what
    makes the pre-mistake position reachable and therefore savable as line move 1.
  - Game review / full-game entry: whole game seeded.
`analysisDeleteCurrent()` (🗑 button + Delete key): removes the current node and
  its subtree, lands on the parent, disabled at root.

Validation:
  - Threshold is a 5%-win-chance-drop via cpToWinPct (matching classifyWpDrop's own
    "good" cutoff), NOT a flat centipawn gap. No won-position exemption.
  - "Find another way" is optional and eval-gated, not mandatory.
  - `classifyRemainingTacticAlts(invLines)` classifies each not-yet-solved alt line:
      • mandatory — divergence-from-solved is at an OPPONENT move, or a user move
        with no proven sibling yet.
      • optional — divergence at a USER move with a proven sibling AND the branch's
        stored eval is strictly higher than the best proven sibling → surfaced as
        "💡 Find a stronger move (+Ncp)" on the completion card
        (`loadOptionalTacticAlt`); does not gate the SRS grade.
      • excluded — user-divergent with an equal/better/unmeasured proven sibling;
        nothing left to prove there.
  - `tryTacticLineSwitch` (called from evaluateTacticMove): a played move that
    doesn't match the active line but IS the expected first move of another
    unsolved sibling line is silently accepted, swapping the active line. No-op
    for ordinary analyzer/detected tactics (which only diverge at opponent moves).

Saved shape (gistData.practiceTactics[], via confirmSaveSequence — parallel to but
  a DIFFERENT function than savePracticeTactic used by Detected Tactics):
  { type:'tactic', gameId:`_practice_tactic_<ts>_<rand>`, movePly, fenBefore,
    playerColor, moveNumber, tacticMoves (=lines[0]), tacticAltLines (=lines.slice(1)),
    maiaOpponentMoves:null, wpSwing, wpDrop, found:false, evalBefore:{eval:cp},
    evalAfter:null, _opening, _createdAt, _source:'converted-mistake'|'game-review',
    _isSequence:true, _chainSig, [_sourceGameId, _sourcePosId if from a mistake] }
  Dedup by fenBefore + _chainSig.

─── PRE-ANALYSIS & ADAPTIVE ANALYSIS ───

preAnalysis: Stockfish go depth 30 MultiPV 3 when mistake loads.
  waitForPreAnalysis(minDepth=16) min before evaluating user's move.
  preAnalysisSession tokens invalidate stale handlers.
  stopPreAnalysis() tears down (NO stop command). Skipped for tactics.

sfAnalyzeAdaptive(fen, {minDepth=16, maxDepth=22, stableCount=2, onUpdate=null}):
  depthAdj() applies mobile offset to minDepth/maxDepth.
  Exits early on stable-for-N consecutive depths ≥ minDepth.
  onUpdate(depth, uci, cp, moves) for live analysisArrow.
  Stable-exit does not detach the listener before bestmove (sfSearchActive can't
  latch true permanently). Init failures terminate zombie workers on handshake
  timeout. handleSfCrash nulls both preAnalysisListener and analysisListener.

─── EVAL BAR (DEDICATED WORKER) ───

evalSfWorker + evalSfSearchActive + evalSfSession + evalSfPhase + evalSfPhase2PollTimer.
Two-phase: EVAL_WORKER_PHASE1_DEPTH=16 (on position change), EVAL_WORKER_PHASE2_DEPTH=30
  (after main worker idle, via evalWorkerWaitForIdle). Hash = max(16, sfHashMB/4).
  150ms debounce. evalSfPhase: 0=idle, 1=quick, 2=deep. evalSfResult={fen,cpWhite,depth}.
Eval worker pauses when the tab is hidden (visibilitychange) and resumes on return.

Visibility:
  Continuation: gated on continuationEvalBarOn (transient per-puzzle flag, starts
    OFF every puzzle, user-togglable; INDEPENDENT of persisted evalBarEnabled).
    Always reset to false in retryPosition / loadPosition.
  Outside continuation: evalBarEnabled && evalBarRevealed && !tacticMode &&
    currentMistakeIdx >= 0 && (preAnalysis || hasEngineLineEval). Hidden in tactic mode.
afterMoveCpWhite takes priority over evalSfResult whenever non-null.
Cancel = terminate + reinit (NEVER send stop).

─── ENGINE LINES ───

buildEngineLine(baseFen, userMove, contUci, cpWhite, opts?) → engineLine:
  { baseFen, moves: [{uci,san,fen,isUser}], currentIdx: -1,
    alternatives: [{branchIdx, moves, cpWhite}], activeAlt: -1,
    extension: null, cpWhite }
  (wrongMove flag added externally by callers; no userMove key — moves[0] is the user's.)
  `opts` supports a pinned start index + animation control, used when the line is
  started by moving an opponent piece directly (see below) rather than via the
  "Show engine line" button.

ENGINE_LINE_PLY = 6 (extended on demand via prefetchLineExtension).
Auto-advance after build: idx 1 (opponent's first reply), deferred 250ms re-highlight.

Branching: user plays a move at any position → alternatives[]. activeAlt: -1 main,
  0+ alt. Nav back past branchIdx returns to main. Max depth 2 (alt's alt replaces
  the continuation).

PGN-style right-panel rendering with parentheses. Click panel moves = navigate.
browsingLine = true at non-terminal. Arrow keys navigate lines only.

afterMoveCpWhite updated by handleEvalResult / goToLineMove / branch analysis.
Always priority over preAnalysis.bestCp.

renderEngineLine's "Back" button is CONTEXT-AWARE: when window._reviewBestLine is
  set it labels "← Back to review" and calls exitReviewBestLine(moveIdx) (restores
  the key-move review card); otherwise "← Back" / exitEngineLine. Step buttons and
  hint text carry .engine-line-step-btn / .engine-line-step-hint so mobile CSS
  hides only those, never the whole nav row (mobile has no keyboard Escape).
  exitReviewBestLine clears analysisArrow and terminate+reinits the worker if a
  branch analysis is in flight.

Copy Line as PGN: buildPgnMoveText(baseFen, sans) + copyLineToClipboard(text, btnEl)
  power a 📋 button on all six navigable line surfaces (engineLine, contLine,
  tacticLine, repContLine, repLeadUpLine, analysisTree). Clipboard API with
  execCommand fallback, ✓/✕ flash.

Start an engine line by moving a piece FOR THE OPPONENT: after a good move, instead
  of pressing "Show engine line" the user can pick up an OPPONENT piece and drop it
  to branch a line at that reply — the engine's own top reply stays the main line,
  the user's chosen reply becomes an analyzed alternative via handleEngineLineBranch.
  - `showContinuationOptions(m, move, contUci)` stashes `window._deferredEngineLine
    = {baseFen, move, contUci, cpWhite, postFen}` — postFen is the board position
    (opponent to move) at that moment.
  - `deferredLineBranchReady()` — true only when a payload exists AND none of the
    eight mode flags/transient states (browsingLine, engineLine, contLineBrowsing,
    contLineReviewMode, repertoireDrillMode, repContActive, repLeadUpActive,
    noteJumpedAway, open settings panel) are active AND
    `gameEngine.fen() === d.postFen`. That FEN check is the drift-guard:
    onRevealBestMove resets gameEngine to the PRE-move FEN, which fails the check
    and disables the affordance (correctly — it's the user's move again there).
  - `startEngineLineWithOpponentMove(source, target)` validates the drop against
    data.postFen first (illegal drop leaves state untouched), builds the line
    pinned at idx 0, routes into handleEngineLineBranch.
  - Hooked into onDragStart, onDrop (before gameEngine.move), and tap-to-move.
  - window._deferredEngineLine is nulled once consumed either way, so it can't
    linger as a zombie reference into the next puzzle.
  - KNOWN OPEN BUG (issue #23): pressing "Back" after starting a line this way
    returns to the position AFTER the new move, not before it.

─── BAD MOVE BEHAVIOR ───

Position stays on board. Engine line shown. User navigates back to retry.
"↺ Try again" resets (tryAgainFromLine / tryAgainFromContinuationLine).
After 3 wrong: revealBestMove. Again is recorded on attempt 1 (locked).
Arrow keys only for lines, not puzzle navigation.

─── CONTINUATION PRACTICE ───

After a good move: "Play Continuation" + "Show engine line" buttons, plus a hint
  line for the opponent-move-branch affordance (see ENGINE LINES).
Opponent source priority in advanceContinuation (standard + filter practice + advantage):
  1. Lichess Explorer DB (fetchExplorerMoves at https://explorer.lichess.ovh/lichess)
     - Requires lichessToken; 5s AbortController timeout
     - Filters by explorerRatings/explorerSpeeds, then explorerMinGames + explorerMinFreq
     - pickExplorerMove: WEIGHTED RANDOM by total games (NOT deterministic-best —
       two runs from the same FEN give different opponent lines)
     - explorerData=null → API failure (explorerApiFailed=true)
     - explorerData non-null but no candidates → genuine exhaustion (switchedToMaia=true)
  2. Maia 3 — if !maiaReady && _maiaCompilePromise, wait up to 5s via Promise.race
     with "Preparing opponent model…" feedback (suppressed in silent mode).
  3. Stockfish best move (final fallback).
lastOpponentSource: 'DB (N)' | 'Maia' | 'Stockfish' (indicator 📊/🧠/none).
explorerApiNotified: per-session flag; shows "📊 Database unavailable — using Maia"
  once per session on API failure. explorerNotifyMaia: shown when DB exhausts.

NOTE: Repertoire continuation does NOT use this priority chain — it uses
  getRepertoireContinuations() (trie-known opponent moves only).

contLine = { baseFen, moves: [{san,uci,fen,isUser}], currentIdx, [_tacticCandidates] }.
contLineBrowsing = true mid-line. Mid-line move truncates and resumes.
End-of-line action row: "Review Game" (gated on wasSilent for natural endings) +
  "♟ Analyze on Lichess" (POSTs a hidden form to https://lichess.org/paste).

Every finished continuation (natural ending, Stop & Review, advantage-drill end,
  TODO-list drill end) is also snapshotted into persistent Game Review HISTORY —
  see that section for storage/restore/UI. This is separate from (and feeds)
  contLineReviewMode, the live in-session review UI below.

contLineReviewMode ("Game Review", via "Stop & Review"):
  contLineReviewKeyMoves: indices of mistake/blunder moves.
  contLineReviewKeyIdx / contLineReviewRetryIdx (-1 = none).
  Interactive eval graph, accuracy %, key move walkthrough.
  "Show line" / "Show refutation" share window._reviewBestLine state.
  showReviewBestLine plays mv.bestMoveUci first, then analyzes (matches the arrow).
  stepReviewBestLine reads engineLine.currentIdx as source of truth.
  Repertoire deviations are a distinct key-move category (purple 📖) — see
    GAME REVIEW HISTORY for the in-line vs corrected distinction.
  "🔍 Sequence" on key-move/retry-result cards routes into SEQUENCE CONVERSION.

Continuation refutation: wrong move → contWrongMovePending=true, continuationMode
  temporarily false so engine line browsing works. tryAgainFromContinuationLine
  restores cont state and resumes pre-analysis.

silentContinuation = true for advantage + silent-game contexts; suppresses per-move
  feedback. SILENT_REVIEW_MIN_MOVES = 5 before "Stop & Review" appears (also the
  minimum for a Game Review history snapshot to be captured at all).
  silentEvalQueue processes deep (SILENT_EVAL_DEPTH=22) re-evals on both workers.
  After each silent eval classifies a user move, detectTacticCandidate runs to
  populate contLine._tacticCandidates (see DETECTED TACTICS). Queue-drain
  completion also triggers finalizeReviewSnapshotIfPending().

─── CONTINUATION SESSION PERSISTENCE ───

CONT_SESSION_KEY = 'mistakelab_cont_session', CONT_SESSION_MAX_AGE = 4h.
saveContSession: called on every move AND visibilitychange/pagehide.
Stores: contLine, currentSolveFen, orientation, silentContinuation, advantageMode,
        playerColor, itemType, itemPosId, srsRecordedAtSave, savedAt.
loadContSession + showContResumeBanner (≥ 2 moves required to prompt).
resumeContSession restores and synthetic-item-injects into filteredMistakes for
  practiceContItem persistence ('practice' itemType). srsRecorded restored type-aware.

─── GAME REVIEW HISTORY (persisted, cross-session, cross-device) ───

Lets a finished continuation's post-game review be reopened later from the Games tab.

Capture — six end-of-continuation hook sites: finishAdvantage, finishTodoDrill,
  stopAndReviewContinuation, user checkmate/draw, opponent checkmate/draw,
  turn-mismatch ending. Each calls `captureReviewSnapshot(source, outcome)`.
  - `buildReviewSnapshot` requires ≥ SILENT_REVIEW_MIN_MOVES (5) user moves or
    returns null.
  - Snapshot is SLIM: per-move `{san, uci, isUser, classification, wpDrop, cpLoss,
    bestCp, afterCp, bestMoveUci, repTried}` — no per-move FEN (rebuilt on restore
    from baseFen + UCIs) and no `_pvs`. `repTried` (the move first attempted before
    a corrected repertoire-deviation undo) is kept since it's real game history,
    not derivable from the trie.
  - Idempotent via `contLine._reviewSnapId` (stamped on first capture, reused on
    re-capture — a later finalization overwrites the same entry). `ts` (original
    end time) is preserved across re-captures; `updatedAt` bumps so merges prefer
    the freshest version. `finalCp` is pinned to the value at game end.
  - Provisional capture at the end-of-game hook; final capture on silent-eval
    queue drain (`finalizeReviewSnapshotIfPending`).

Storage — split between two systems:
  - Gist file `mistakelab_reviews.json` is the cross-device source of truth, kept
    SEPARATE from the progress Gist file (append-heavy/bulky; bundling it would
    weigh down every ordinary 3s progress read-merge-write).
  - Local mirror lives in its OWN IndexedDB database `mistakelab_reviews` (v1),
    store `reviews`, keyed by snapshot id (openReviewsDb / idbPutReviews /
    idbDeleteReviews / idbLoadAllReviews, standard getAll() batch read).
    DELIBERATELY not a new store in the 'mistakelab' DB — see EXTRACTION & CACHES
    for why bumping FEN_DB_VERSION is dangerous.
  - `mergeReviewHistories(a, b)`: union by id, higher updatedAt (fallback ts) wins,
    sorted ts desc, capped at REVIEW_HISTORY_CAP=150.
  - `syncReviewHistoryToGist` — like the eval cache (unlike progress) STILL WRITES
    on remote-read failure: a lost snapshot is recoverable via the next
    read-merge-write from either device, so this device keeps its own data rather
    than refusing to write.

Restore — `openHistoricalReview(snapId)` uses the same synthetic-practice-item
  pattern as resumeContSession to reconstruct a contLine from the slim snapshot
  and re-enter contLineReviewMode. Must hide `#emptyMsg` on entry (a stuck
  "Select a game to begin" overlay was a real bug here).

Games-tab UI — review history entries are merged into the chronological games
  list (see ITEM TYPES), not segregated. The "Correspondence" (✉️) speed chip
  covers both real correspondence/daily games and review-history entries.
  `reviewPassesGameFilters()` is the filter policy for review entries specifically.

Repertoire deviations as Game Review key moves: in-line deviations (an off-book
  move never corrected) and corrected deviations (the mid-practice "Ignore for
  this game?" banner triggered, then undone) are both flagged as
  `mv._repDeviation` (recomputed on every contLine build, never persisted as a raw
  boolean) and rendered with purple 📖 styling distinct from mistake cards, with a
  trie-match fast path in the retry flow. `pendingRepDevTried = {posKey, san,
  uci}` captures the first-tried move; `addContLineMove` stamps it onto
  `move.repTried` when the FEN posKey matches and the played UCI differs, then
  clears the pending marker.
  Corrected deviations land PRE-move in the walkthrough (moveIdx − 1 — the
  corrected version is what the user actually played and is the thing to retry).
  The answer (repTried) is hidden until solved (`revealed` param on
  `showReviewKeyMoveInfo`); the "you first tried X" context appears only after a
  successful retry.

─── ADVANTAGE MODE (type: 'advantage') ───

Silent continuation drill from peakFen. advantageMode=true, silentContinuation=true.
Loop tracked by advantageMinCpReached, advantageConsecutiveAbove1000.
CONT_ADVANTAGE_GOOD_ENOUGH = 600 cp.
finishAdvantage('victory'|'draw'|'collapse'|'interrupted') ends the drill — and is
  one of the six Game Review History capture hook sites.
Full post-game review on end (same as contLineReview).

─── TACTIC MODE ───

tacticMode=true routes all moves through evaluateTacticMove().
State: tacticMoves[], tacticStepIdx, tacticWrongAttempts, tacticHintUsed,
       tacticLine, tacticAltLines[], tacticAltIdx (-1 main), tacticSolvedLines[],
       tacticPrefixUserMoves, tacticCompleted.

Flow: correct → opponent auto-play (400ms) → next. Wrong → engine line → retry.
A move that doesn't match the active line but IS the expected first move of another
  unsolved sibling line is silently accepted via `tryTacticLineSwitch` (see
  SEQUENCE CONVERSION) and swaps the active line — a no-op for ordinary
  analyzer/detected tactics.
After the main line: advance through unsolved alts, skipping the common prefix via
  tacticSolvedLines. Remaining alts are classified mandatory vs. optional via
  `classifyRemainingTacticAlts` (see SEQUENCE CONVERSION) — mandatory drives
  "▶ Next line"; optional surfaces as "💡 Find a stronger move" without gating the grade.
Maia alt lines labeled "🧠 Opponent plays a common human mistake".

Per-line invalidation: invalidateLine(pid, fingerprint = uci moves joined).
tacticHasValidLines checks all lines against gistData.positions[pid].invalidatedLines.
All-invalid dialog: Convert to mistake OR Just remove.

Post-tactic: startTacticContinuation() starts fresh contLine from final pos.

Tactic constants (browser):
  TACTIC_MIN_USER_MOVES=2, TACTIC_MIN_OPP_CP_LOSS=100, TACTIC_MIN_PLY=6,
  TACTIC_MIN_REMAINING=2, TACTIC_OPP_WP_CAP=15 (percentage),
  TACTIC_MAX_ALT_LINES=3, TACTIC_MAX_ALT_DEPTH=3,
  TACTIC_FIRST_MOVE_THRESHOLD_WP=30, TACTIC_HASH_MB=32.
  (Analyzer mirrors these but TACTIC_OPP_WP_CAP=0.15 in fractional form.)

─── DETECTED TACTICS (post-game tactic discovery) ───

Phase 1 — detectTacticCandidate(moveIdx) runs after each silent eval classifies a
  user move. Pre-filters mirror analyzer/analyze.js exactly:
    - moveIdx ≥ 2: prev opp move's cpLoss ≥ TACTIC_MIN_OPP_CP_LOSS (skip for moveIdx 0/1)
    - ply on board ≥ TACTIC_MIN_PLY; remaining plies ≥ TACTIC_MIN_REMAINING
    - mv._pvs has ≥ 2 entries; PV gap ≥ TACTIC_FIRST_MOVE_THRESHOLD_WP (30%)
  Uses mv._pvs captured during silent eval — no new SF call.
  Pushes to contLine._tacticCandidates: { moveIdx, preFen, playerColor, oppCpLoss,
    gapWp, bestMoveUci, initialPvs, scanned, scanFailed, chainResult, userFound,
    detectedAt, _saved, _discarded }.

Phase 2 — background scanner runs walkTacticTree on unscanned candidates after
  game ends. tacticScanMaiaAlts toggle (default true, 'mistakelab_tactic_maia_alts')
  also generates Maia opponent alt lines. tacticScanBanner updates as it resolves.

UI — Detected Tactics Modal (openDetectedTacticsModal / renderDetectedTacticsList):
  ✓ Found / ✕ Missed badge, move-num SAN preview, line count, per-state actions
  (Try/Save/Discard → ✓ Saved → Undo discard).

Solve mode — tryDetectedTactic(listIdx) sets detectedTacticSolveMode=true, routes
  through tacticMode evaluation. Saves review snapshot in
  _detectedTacticReviewSnapshot for restore on exit. Escape → abandonDetectedTacticSolve.

Save flow — savePracticeTactic(cand), a DIFFERENT function from confirmSaveSequence
  (SEQUENCE CONVERSION) though both write to gistData.practiceTactics with similar
  shapes and the same fenBefore+chainSig dedup convention. Scanner-detected tactics
  never carry `_isSequence`; sequences always carry `_isSequence:true, found:false`.
  { type:'tactic', gameId:`_practice_tactic_<ts>_<rand>`, movePly, fenBefore,
    playerColor, moveNumber, tacticMoves, tacticAltLines, maiaOpponentMoves,
    wpSwing≈gapWp, wpDrop, found, evalBefore, evalAfter:null, _opening, _createdAt,
    _source, _chainSig }
  Injected into live filteredMistakes immediately; merges into allMistakes on next
  extraction with `_t<ply>` posId, same as analyzer-discovered tactics.

─── REPERTOIRE CHECK (type: 'repertoire') ───

OAuth: PKCE flow, scope 'study:read'. LICHESS_CLIENT_ID = 'mistakelab'.
  LICHESS_REDIRECT_URI = location.origin + location.pathname.
  Token at https://lichess.org/api/token. Stored in localStorage 'mistakelab_lichess_token'.
  Verifier 128 chars / state 32 chars in sessionStorage; cleared after callback.

REPERTOIRE_CACHE_KEY = 'mistakelab_repertoire', REPERTOIRE_CACHE_VERSION = 5.
  Per-study entry: { pgnHash, color, trie, comments, lastFetched }.
  pgnHash match → restore cached trie/comments without re-parsing.
  Fetch failure with cached entry → fall back to cache; without cache → skip study.

fetchStudyPGN: https://lichess.org/api/study/<id>.pgn?clocks=false&orientation=true
  Auth header if lichessToken present. Retries 3× with backoff on HTTP 429.

State: repertoireTrie (Map<posKey, Array<{san, uci, ...chapterMeta}>>) ← multi-value!
       repertoireDeviations[], repertoireGaps[],
       repertoireMode, repertoireDrillMode, repertoireDrillQueue,
       repertoireDrillFailed[], repertoireDrillStartTime.
Trie helpers: trieFirst(posKey) returns first entry; trieMatchesMove(posKey, san, uci)
  matches by uci OR san across all entries.
isRepertoireCorrectForItem(m, san, uci): checks m.repertoireMove first, then trie.
isRepertoireMove(fenBefore, san, uci): applies 'book' classification + filters out
  "deliberate prep" moves from extracted mistakes.

Review queue: isDue() + startRepertoireReview (due badge via updateRepertoireBadge,
  updates continuously, not gated on drill mode).

Repertoire continuation (repContActive, ungraded): repContMode 'random'|'all',
  repContPendingBranches, repContDepth, repContLine, repContBrowsing,
  repContFailedFens (Set), repContRetrying, repContRetryQueue.
  Uses getRepertoireContinuations() — ONLY trie-known opponent moves, no DB/Maia.

Retry failed moves: "↺ Retry N failed positions" → startRepContRetry replays
  silently to each failed position, prompts again.

Lead-up navigation: repLeadUpActive + repLeadUpLine + repLeadUpIdx.
  buildRepertoireLeadUp / buildLeadUpFromTrie / replayRepertoireBeginning /
  startRepertoireFromBeginning.

Studies storage: gistData.repertoire.studies = [{id, color, name}] (name from
  [ChapterName] → [StudyName] → [Event] precedence, bracket-safe header stripping).
Custom deviations: gistData.repertoire.customDeviations[]. addCustomDeviation /
  removeCustomDeviation / addCustomDeviationFromInput.
Dismissed: gistData.repertoire.dismissed[] (position keys) —
  dismissRepertoirePosition / dismissRepertoireOpening / restoreDismissedRepertoire.

PGN parsed via parseRepertoirePGN → tokenizePGN → walkPGNTokens. Comments stored
  as notes per position. Chessable bookmarklet helps extract course URLs.

─── REPERTOIRE VARIATION-CHECKLIST (TODO-LIST) ───

Per-study checklist of the most important variations to know, on the Repertoire tab.
Ranking mirrors the standalone prioritize_repertoire.py: TRIE-DRIVEN enumeration of
the covered tree, with the Lichess opening explorer scoring opponent nodes to set
how a fixed slot budget is APPORTIONED across the tree (proportional, NOT a ranking
cut). Absent-from-DB covered moves still enumerate with a tiny floor.

Generation — generateTodoVariations(studyId, color, opts) → { variations, gaps }:
  opts: targetPly (10), maxVariations (10; UI clamps 1–50), maxGaps (10),
        maxExplorerCalls (2500), maxNodes (20000), onProgress(callCount).
  TODO_RANK_RATINGS = [1600,1800,2000,2200,2500]; TODO_RANK_SPEEDS = blitz/rapid/
    classical/correspondence. Explorer calls memoized by fenPositionKey; 120ms
    politeness delay; keys BOTH uci and 's:'+SAN (castling O-O differs between
    chess.js e1g1 and explorer e1h1 — SAN matches either way).
  Transposed leaves deduped by leafKey (fenPositionKey of leaf).
  Variation shape: { leafKey, leafFen, lineUci, lineSan, cumProb, importance,
    openingName }. Gap shape: uncovered opponent moves (reachProb ≥ 0.08),
    collapsed by transposition, ranked, capped.

Persistence — gistData.repertoire.todoLists[] (one per study): { id:`todo_<studyId>`,
  studyId, name, color, targetPly, maxVariations, createdAt, variations[], gaps[] }.
  generateTodoListForStudy(studyId) regenerates via #todoPly/#todoCount inputs.
  Merged by id, local wins — these are regenerable definitions.

Completion is DERIVED, not stored per-variation: a "win" writes to
  gistData.practiceScoreboard[leafKey] tagged with the preset (recordPracticeResult
  meta.preset). todoCompletedPresets(leafKey) scans for result==='win' entries
  carrying a preset. Write key and read key are both fenPositionKey(leafFen).

Difficulty presets (REP_TODO_PRESETS, easy→medium→hard, REP_TODO_PRESET_ORDER):
  easy:   ratings [1600,1800],           maiaElo 1750, precision 0.65
  medium: ratings [1600,1800,2000,2200], maiaElo 2000, precision 0.75
  hard:   ratings [2000,2200,2500],      maiaElo 2150, precision 0.85
  Presets affect ONLY the opponent — never the ranking. Chips unlock progressively.
  Shared adjustable Maia ELO per preset: repTodoMaiaElo, persisted in
  'mistakelab_todo_maia' (1600–2200, step 25); precision stays preset-fixed.

Per-difficulty win-rate on chips: `TODO_WINRATE_WINDOW = 5` — window over the most
  recent attempts of each preset. `todoPresetStats(leafKey)` buckets
  practiceScoreboard[leafKey] by e.preset, takes slice(-5) of each bucket (entries
  are chronological, so this is the most-recent window), returns
  {easy:{n,wins,rate}, medium:…, hard:…} — rendered as wins/n + tooltip on each
  chip. `winRateColor(rate)` maps 0..1 to background color via piecewise HSL hue
  lerp: 0%→red(0°), 50%→orange(~30°), 75%→yellow-green(~78°), 100%→green(~122°),
  fixed 55%/45% S/L for legible white text. Never-attempted chips stay neutral.
  This is a display layer only — completion/unlock logic is unchanged by it.

Repertoire-tab fold/sort UX: checklist blocks render COLLAPSED by default with a
  clickable header showing a color-coded progress bar (winRateColor, across ALL
  variations/difficulties — 100% means every Easy/Medium/Hard chip is won).
  Controls (count/ply, Generate) live in the unfolded body; a block force-opens
  during generation. Checklists sort by `todoLastInteraction(studyId)`, combining
  the newest cross-device practiceScoreboard timestamp with a session-local touch
  stamp `window._todoTouch[studyId]` (set in startTodoFromList) so an abandoned
  drill (no scoreboard entry yet) still surfaces as recently touched. The Studies
  config section (Lichess connect + study list) collapses to a compact header once
  Lichess is connected and studies exist; `addRepertoireStudy` force-keeps it open
  mid-setup. All fold state is SESSION-LOCAL: `window._todoOpenStudies` (Set),
  `window._repConfigOpen` (bool) — not persisted.

Drill flow (repTodoActive=true, repTodoState={variation,preset,phase:'leadup'|'cont'}):
  1. Scripted LEAD-UP: user plays their own prep from the start; opponent on rails
     (buildTodoLeadUp / showTodoLeadUpPrompt / advanceTodoLeadUpAfterUser /
     evaluateTodoLeadUpMove). Deviation rejected, must retry.
     State: repTodoLeadUp[], repTodoLeadUpIdx.
  2. Free CONTINUATION from the leaf using advantage-mode machinery
     (startTodoContinuation → silent continuation, DB→Maia opponent at the preset,
     Claim-Victory at +1000×3, checkmate ends). applyTodoPreset sets
     explorerRatings/explorerSpeeds/maiaElo; restoreTodoContSettings restores after.
  3. finishTodoDrill(outcome): a win → recordPracticeResult('win', cp, {preset}) →
     checks the variation off for that preset. endTodoDrill tears down.
     abandonTodoDrill on Escape; replayLastTodoDrill re-runs. finishTodoDrill is
     also one of the six Game Review History capture hook sites.
  Voice mode supported in the play-out (toggleTodoVoice).

UI: renderTodoChecklistSection(studies) / renderTodoStudyBlock(s). Mini-board
  HOVER PREVIEW plays the line move-by-move as the cursor slides (todoShowPreview
  / todoPreviewCells / todoLinePreview / todoHidePreview / formatTodoLineSanHoverable)
  — a persistent diff-updated grid, anchored on first show. startTodoFromList
  (studyId, vIdx, preset) launches a drill and stamps the touch timestamp.

─── FILTER PRACTICE (type: 'practice') ───

"▶ Play" on opening filter panel. Uses current filter FEN as start.
practiceContItem: synthetic, pushed into filteredMistakes without applyFilters()
  (which would wipe it). playerColor = board orientation.
Opponent source: same DB → Maia → Stockfish chain as standard continuation.

Mid-practice repertoire check (ply < 20): if repertoireTrie has a move for the
  current position, user's move is silently checked. Deviation → banner
  "Ignore for this game?" → practiceRepIgnored=true on click. Auto-saves
  non-ignored deviations to gistData.repertoire.customDeviations.
  Hint returns trie move (not Stockfish) when trie has one for this position.

Scoreboard (recordPracticeResult(result, finalCpWhite, meta={})):
  appends to gistData.practiceScoreboard[posKey][]: { ts, result:'win'|'draw'|'loss',
  finalCp, moves, [preset] }. posKey = fenPositionKey(practiceContItem.fenBefore).
  meta.preset present only for TODO-list drills.

Save mistake (savePracticeMistake(moveIdx)) during post-game review, appends to
  gistData.practiceMistakes[]: { type:'mistake', gameId:`_practice_<ts>`, movePly,
  fenBefore, sanPlayed, userUci, cpLoss, cpBefore, cpAfter, wpDrop, playerColor,
  moveNumber, bestEval, _opening, _createdAt }. Dedup by fenBefore + sanPlayed.

Settings panel (⚙ next to ▶ Play): buildContSettingsHtml() — DB toggle, rating
  checkboxes, speed checkboxes, csMinGames/csMinFreq inputs, csNotifyMaia, Maia
  ELO/precision sliders. hasContSettings() skips the panel if already configured.
  Persisted in 'mistakelab_cont_settings'.

cleanupPracticeCont: lightweight exit that skips exitOpeningFilter() (which
  applyFilters-wipes the practice item).

─── IN-APP ANALYSIS MODE ───

analysisMode=true. analysisEngine (Chess instance), analysisNodes (id→node tree),
  analysisTree (root), analysisTreeCurrent, analysisPvs (MultiPV 3), cachedPvs per
  node for instant restore.
Arrows: teal=best, green=alts. Opacity/scale proportional to eval gap.
Keybinds: ←/→ analysisLineStep, F flip, N toggle note panel, Delete →
  analysisDeleteCurrent().
Auto-exits on loadPosition/enterOpeningFilter. Position Notes + Copy Line as PGN
  available. Notes track whatever position is on the board regardless of how it
  was reached. Divergent retry positions are preserved as sidelines in the tree.

Tree root selection — see SEQUENCE CONVERSION for the three-case logic.
🗑 Delete current move (analysisDeleteCurrent, also Delete key): see SEQUENCE CONVERSION.
"💾/🔍 Sequence" save flow — see SEQUENCE CONVERSION in full.

Maia predictions overlay (🧠 toggle): what a human at a given ELO would play,
  independent of, and alongside, the DB overlay below.
  - `analysisMaiaOn` + `analysisMaiaElo` persisted in 'mistakelab_analysis_maia'.
  - analysisMaiaElo: 1600–2200 step 25, default 2000, its OWN slider — independent
    of the continuation opponent's maiaElo.
  - `maiaPolicyTopMoves(fen, elo)` runs the full legal-move policy softmax at
    TEMPERATURE 1 (the raw distribution, not a sampled move) through the existing
    Maia ONNX session. SANs resolve on the ORIGINAL fen, not the mirrored one
    (avoids errors for black-to-move; maiaMirrorFEN still applies to board
    encoding internally).
  - Memoized: analysisMaiaCache, key fenPositionKey|elo, cap 300 entries.
  - Serialized via a promise chain + debounce so rapid ELO/nav changes can't
    trigger overlapping ONNX run() calls; session-token-guarded against stale results.
  - Probability badges on PV rows at ≥10% predicted share; if Maia's top move
    isn't in the top-3 PVs it renders as a clickable violet 4th row + board arrow.

Lichess DB overlay (📊 toggle): parallel to the Maia overlay, from actual DB play.
  - `analysisDbOn` + rating/speed filters persisted in 'mistakelab_analysis_db'.
  - Sample-size thresholds: below ANALYSIS_DB_MIN_GAMES(=5) → hidden; 5–24 → both
    DB and Maia badges show; ≥ ANALYSIS_DB_HIDE_MAIA_MIN(=25) → DB-only, Maia
    annotations suppressed at render time.
  - Up to the top-3 most-played DB moves not already in engine PVs render as
    clickable extra rows, each requiring ≥ ANALYSIS_DB_EXTRA_MIN(=15)% share.
  - Footer: "N games in database". Cache: 300 entries (fenPositionKey|ratings|speeds).
    Debounce: 300ms. Requires a castling-UCI normalization for correct DB lookups.
  - Layout: DB badges render right of Maia badges on PV rows; DB extra rows render
    above Maia's extra row.

─── POSITION NOTES ───

📝 btnNote in header; hidden when no position loaded.
gistData.notes[fenPositionKey] = { text, arrows, circles, updated } (updated drives
  mergeGistData newer-wins per note).
noteArrows/noteCircles rendered by renderArrows() only when note panel is OPEN —
  never auto-render on position load.
checkForNote / updateNoteButton / .has-note CSS class on btnNote.
showNoteArrows / hideNoteArrows / openNotePanel / enterNoteEditMode / saveNote /
  deleteNote route through debounceSyncToGist (not bypassing the dirty flag).
deleteNote shows an undo-toast (6s): restore writes the pre-delete snapshot back
  with a FRESH `updated` timestamp so merge keeps the restore over the tombstone
  cross-device. Restore preserves the original `source` field verbatim.
Auto-show note on correct/reveal (repertoire shows study PGN comments as notes).
Clickable variation lines in notes: getCurrentNoteFen() prioritizes gameEngine.fen()
  (post-move) then m.fenBefore, so a note at a post-move position validates
  variation moves with the right side to move. jumpBoardToNoteFen +
  noteActiveVarParent/noteActiveVarIdx + arrow-key nav scoped per .note-variation;
  resetNoteVariationState() at every panel.innerHTML rewrite site.

─── EVAL CACHE (CROSS-DEVICE) ───

evalCache: Map<fenKey, {depth, bestMove, bestCp, pvs, multiPv}>.
evalCacheKey delegates to fenPositionKey (transposition hits, EP-normalized).
evalCacheStore merges via evalCacheShouldOverwrite (deeper-AND-at-least-as-wide).
Partial hits used immediately, deepened in background.
Gist file: 'mistakelab_evals.json'. debounceEvalCacheSync: 15s debounce, shortened
  when EVAL_CACHE_SYNC_THRESHOLD(=10) dirty entries accumulate.
syncEvalCacheToGist is READ-MERGE-WRITE but, UNLIKE syncToGist, still writes on
  read-failure (eval entries are recomputable from FEN, so a blind write is
  acceptable) — the same convention Game Review History's sync uses.
Airplane mode: same debounce, writes to localStorage instead. flushPendingSyncs
  (pagehide/beforeunload) bypasses debounce → direct LS write. Airplane→online
  force-syncs pending dirty entries.

─── HINT SYSTEM ───

btnHint grayed (.hidden/.disabled), not display:none.
hintLevel 0→1: piece-to-move highlight. Mistakes wait depth 20 then stopPreAnalysis.
  Tactics (incl. sequences): tacticMoves[tacticStepIdx].uci.
  Repertoire (drill): m.repertoireMove.uci. Repertoire continuation: trieFirst(currentFen).
  Filter practice: trie move if available, else Stockfish. SKIPPED during
  contLineReviewRetryIdx >= 0 (Game Review retry uses engine best, not trie).
hintLevel 1→2: blue hintArrow. Origin keeps .square-hint.
hintUsed=true → SRS Again override. Reset in loadPosition, retryPosition, every
  evaluate* function entry, and at tryDetectedTactic.
H key respects .hidden/.disabled. In continuation, hints use currentSolveFen.

─── OPENING EXPLORER ───

gameFenIndex: Map<gameId, Set<positionKey>> — transposition-aware filter.
positionMoveIndex: Map<posKey, Map<moveSAN, MoveStats>>.
Both built LAZILY (ensurePMI/buildFenIndex → buildOpeningIndex) — 30 games per
  frame, progressive render as batches complete.
fenIndexGameCount set after allGames is fully finalized to prevent spurious rebuilds.
openingBuildSession: bumped on logout and in invalidateOpeningIndices() (called
  when allGames is replaced) — late builds with a stale snapshot get dropped.
Result bars chess-absolute (white-wins left). filterArrows frequency arrows
  (green); hover filter arrow (blue 0.8).
Opening filter: openingFilterMode, openingFilterFen, savedFilterOrientation.
"Done" opens left panel. Exits on Review/Repertoire tab entry.
Orientation syncs with color filter only when an actual position filter is active
  — see START VIEW for syncColorFilterToOrientation / restorePersistedColorFilter.
Entry point governed by enterStartView() / persisted startView — see START VIEW.
Mobile: layout class switched before board init.

lookupOpeningName(fen): iterates allGames via gameFenIndex (NOT filtered) — most-
  frequent opening name across games containing this position. Also reused by
  buildReviewSnapshot and confirmSaveSequence to label saved items.

─── BOARD ANNOTATIONS ───

Right-click drag → arrow (activeArrows Map "e2e4" → color).
Right-click tap → circle (activeCircles Map square → color).
Modifiers (getDrawColor): Alt→blue, Shift/Ctrl/Meta→red, none→green (yellow exists
  in DRAW_COLOR_MAP but isn't reachable from getDrawColor). currentDrawColor set on
  mousedown.
Ghost preview: ghostArrow, ghostCircle, ghostCircleWasColor (preserves prior color
  if user cancels mid-drag on an already-circled square).
Left-click clears all user annotations. Annotations cleared on loadPosition.
contextmenu suppressed on board.
Note: "frozen ghost arrow" during a held right-click drag and "right-click landing
  on the board border" are intentional Lichess-style behavior, not bugs.

─── PREMOVE ───

premoveData = { from, to, promotion }. Set during opponent's turn via setPremove.
Always queens when premoving a pawn to 1st/8th rank.
Cleared by left-click, right-click, or illegal-on-execute.
Executed in advanceContinuation after opponent plays.
isOpponentTurn(): reads gameEngine.turn() vs m.playerColor — NOT ply parity
  (fromPosition games invert parity). There is no isOurMove() function, only an
  inline boolean in extractMistakesForGame; the same FEN-not-parity rule applies
  anywhere turn-detection is needed.

─── TIME TROUBLE ───

Per-mistake: clockRemaining, moveTime, timeTrouble stamped during extraction.
timeTrouble = clockRemaining < 45 && moveTime < 10.
⏱ icon in review list + game card. "Hide time trouble" filter chip
  (hideTimeTrouble), persisted in mistakelab_filters.

─── CLOUD SYNC (Gist) ───

Files:
  mistakelab_progress.json  → gistData (progress + SRS + notes + repertoire meta)
  mistakelab_games.json     → analyzer output (v2 usernames[] array)
  mistakelab_evals.json     → evalCache
  mistakelab_reviews.json   → Game Review History snapshots (separate file/store —
                               see GAME REVIEW HISTORY)

gistData schema:
  positions[pid].{completed, lastSeen, srs, invalidated, invalidatedLines[]}
  notes[fenKey].{text, arrows, circles, updated}
  repertoire.studies[]            // [{id, color, name}]
  repertoire.customDeviations[]   // [{positionKey, ...}]
  repertoire.dismissed[]          // position keys
  repertoire.todoLists[]          // variation-checklist defs
  practiceScoreboard[posKey][]    // [{ts, result, finalCp, moves, [preset]}]
  practiceMistakes[]              // mistake objects (FILTER PRACTICE)
  practiceTactics[]               // tactic objects (DETECTED TACTICS + SEQUENCE
                                   //   CONVERSION both write here — see those
                                   //   sections for the shape distinction)

syncToGist(): reads fresh remote (gistRead), deep-copies in-memory gistData as a
  snapshot, calls mergeGistData(localSnapshot, remote), replaces gistData with the
  merged result, then gistWrite. On READ FAILURE it REFUSES to write (progress is
  non-recomputable), schedules a 30s retry, keeps the dirty flag set.
mergeGistData(local, remote) field semantics:
  - positions: per-pid newer-wins by lastSeen; invalidated OR'd; invalidatedLines union
  - notes: per-key newer-wins by .updated
  - repertoire.studies: merge by id, LOCAL wins
  - repertoire.customDeviations: merge by positionKey, REMOTE wins
  - repertoire.dismissed: set union
  - repertoire.todoLists: merge by id, LOCAL wins
  - practiceScoreboard: per-posKey merge, dedup by ts, sorted by ts
  - practiceMistakes: dedup by fenBefore|sanPlayed
  - practiceTactics: dedup by fenBefore|_chainSig
debounceSyncToGist: sets _gistProgressDirtyLocal, writes gistData + dirty sentinel
  to LS UNCONDITIONALLY (so an OS-kill mid-debounce doesn't lose the mutation),
  then 3s debounce → syncToGist.
gistFetchAll(): cached response, 5s TTL. gistReadFileContent falls back to raw_url
  (no auth header) for truncated files. gistWrite no longer PATCHes the
  description (so the analyzer's description wins).
Analyzer → Gist → Browser (read-only games path). Browser does NOT write games.
airplaneMode: suppresses Gist requests, loads from 'mistakelab_progress_cache';
  restoreDirtyFlagFromLS so airplane-off auto-push picks up unsynced mutations.
disconnectGist is async — awaits sync/flush before clearing credentials.
beforeunload + pagehide: flush dirty eval cache + pending Gist sync (keepalive
  PATCH fetches, best-effort).

─── SETTINGS MODAL ───

Gear in header. Training section starts with "Start view" (dropdown, see START VIEW).
  ✈ Airplane Mode
  GitHub Token + Gist ID
  Instant Feedback toggle (default true) — off enters silent continuation mode
    for all continuation entries except advantage (always silent).
  Tactic Maia Alts toggle (mistakelab_tactic_maia_alts, default true)
  Search Depth Offset slider (mobile only, 0–6, default 1, log2 hash 5–10)
  Hash Size slider — default 512MB desktop / 128MB mobile. Slider max: mobile 9
    (512MB ceiling) / desktop 10 (1024MB ceiling). Applies next page load.
  Save & Sync + Disconnect live inside the Cloud Sync section (they're Gist
    ops; saveSettingsGist requires a token). Disconnect is two-tap-confirm via
    confirmDisconnectGist (arms red for 3s, self-disarms). Footer: Change user
    (left) / Close. Training settings persist instantly onchange — the modal
    is NOT transactional; Close never reverts anything.

postProgressOnReturn = true by default.

─── VOICE RECOGNITION ───

Web Speech API, max 5 alternatives, SpeechRecognitionPhrase biasing when available.
VOICE_LEX: custom lexicon with homophones ("knights"→N, "of"→f, "height/might"→N,
  "eat/eve"→e, "ninety"→"knight e", castling homophones, etc.).
Build phrasings per legal move, fuzzy token edit-distance match (voiceTokenDistance).
Recommended clarity words: ace/boy/cat/dog/egg/fox/golf/hot.

State: voiceMode, voiceRecognition, voiceListening, voiceSpeaking, voicePendingMove,
       voiceLastOpponentSan, voiceSelectedVoice, voiceBtKeepAliveCtx, voiceUnsupported
       (set when SpeechRecognition is missing; short-circuits future voiceStart
       attempts to prevent feedback spam).
voiceSettings: {enabled, confirm, speed, voiceName} persisted in
  'mistakelab_voice_settings'. **enabled is forced false on every load** — voice
  always off by default each session; all TTS is gated on voiceMode.

TTS opponent move announcement in advanceContinuation after addContLineMove.
BT A2DP anti-sleep: silent oscillator @ gain 0.0001 (voiceStartBtKeepAlive).
TTS echo suppression via voiceSpeaking flag (post-await state re-validation).
voiceShowIndicator: floating 44×44 button, bottom:70px right:12px, three states
  ('listening'|'muted'|'off'), tappable to mute. voiceToggleMic: mute keeps
  voiceMode active (TTS still works); unmute resumes recognition.

Keybinds (when voiceMode && !INPUT/TEXTAREA):
  Space — voiceToggleMic; 1 — replay last opponent move; 2/Enter — confirm
  pending move; 4/Backspace — deny pending move; Escape — deny if pending, else
  normal escape teardown.

Hook points: startContinuationMode, launchFilterPractice, startAdvantageContinuation,
  startTacticContinuation, retry path, and the TODO-list play-out (toggleTodoVoice,
  hooked separately — bypasses startContinuationMode).
voiceStop() on all end-of-continuation exit paths + enterAnalysisMode/enterOpeningFilter.
logout() tears down voice and clears cont/advantage/silent/wrongMove flags.
TTS deviation alert during filter practice announces the study move by name.

─── MOBILE UX ───

Board-first layout, auto-hiding header, drawer + backdrop + scroll lock + Escape.
Mobile toolbar prev/next; desktop hidden on mobile. mobile-browsing /
  mobile-training body classes. initBoard deferred to first loadPosition.
  touch-action:none on #board. lastTouchTime prevents double-fire.
enterMobileTrainingLayout() is the ONLY browsing→training flip (never toggle the
  classes inline). It also: (a) is called by initBoard BEFORE Chessboard
  construction — mobile-browsing keeps panel-center display:none, so a board
  built there measures 0-width and is invisible; (b) applies a deferred
  boardObj.resize() if a window resize fired while the container was hidden
  (_boardResizePending, set by the resize handler when #board.offsetWidth is 0).
  Call it BEFORE board position/orientation/highlight calls — the deferred
  resize redraws squares and wipes custom CSS classes, like orientation() does.
activatePanelTab(tab, keepMobileLayout=false): on mobile, a tab tap ends in
  full-screen mobile-browsing (list takes over the screen, same as pre-board
  startup) — applied at the END of the function so it wins over any
  loadPosition-triggered flip (review tab auto-load). keepMobileLayout=true
  (start-view 'review') opts out so startup lands directly in the drill.
  The ☰ drawer peek is unchanged — only tab taps go full-screen.
Start view 'repertoire': loadRepertoireTrie().then() re-renders the repertoire
  panel (if repertoireMode) once deviations/gaps are detected — the panel is
  first drawn by enterStartView before that background load resolves.
Feedback below board (not popup). #contLineContainer order:-1. Full-width ‹ ›
  tap zones at bottom.

depthAdj(d) = isMobileView() ? max(8, d - mobileDepthOffset) : d. Applied to ALL
  SF depths: go, sfAnalyzeAdaptive min/max, pre-analysis 30, waitForPreAnalysis
  thresholds, eval worker phases.

Established mobile conventions to follow for any new UI: 44×44 minimum tap
  targets, settings modal and logout reachable from mobile-training mode, modal
  z-index above the drawer with a max-height, drawer closes on item taps and
  clears on rotation, tap-to-premove works during the opponent's turn, SCREEN
  WAKE LOCK held during drills (acquireWakeLock/releaseWakeLock, guarded against
  a logout-during-acquire race), eval worker paused on tab-hidden, sync errors
  visible in training mode (setSyncStatus mirrors state==='error' onto
  #btnMobileSettings via .sync-error — the header dot is hidden there).

PWA: manifest.json has NO orientation key → respects system orientation lock.
  Do not add "orientation": "any".

─── ANALYZER (analyzer/analyze.js) ───

Unified run: eval + clocks + tactics per game in one pass.
Flags: --username, --platform (lichess|chesscom), --workers N, --stockfish PATH,
       --depth (18), --threads, --hash, --max (500), --gist-token, --gist-id,
       --save-every (5), --output (analyzed_games.json), --rated-only (true),
       --time-controls, --scan-tactics, --tactic-depth (20), --rescan-tactics,
       --rescan-clocks, --maia-model PATH, --no-tactics, --no-clocks.

Tactic constants (analyzer): TACTIC_MIN_USER_MOVES=2, TACTIC_MIN_OPP_CP_LOSS=100,
  TACTIC_MIN_PLY=6, TACTIC_MIN_REMAINING=2, TACTIC_OPP_WP_CAP=0.15 (fractional),
  TACTIC_MAX_ALT_LINES=3, TACTIC_MAX_ALT_DEPTH=3.

walkTacticTree → alt lines. buildBestTacticChain → primary. generateMaiaLine for
  human-likely opponent moves (null if matches primary).

Tactic shape: { fenBefore, movePly (startPly), playerColor, moves[{uci,san,fen,isUser}],
  alternativeLines?, maiaOpponentMoves?, wpSwing, wpDrop, found, evalBefore, evalAfter }.

detectPlayerColor: uses knownUsernames Set (lowercase, from Gist v2 usernames[] +
  current), checks .user.name AND .user.id, falls back to game._playerColor.
  Current users: ['blindsmurf', 'xvigustaf'].

Note: browser-authored sequences (SEQUENCE CONVERSION) never touch this file —
  they're built client-side from cached PV/eval data straight into gistData.practiceTactics.

─── MAIA INIT PHASES ───

Split for startup perf:
  prefetchMaia (phases 1-3, pure I/O, at showMainApp): loadOrtScript → fetch move
    mappings JSONs → fetch 43.5MB ONNX as ArrayBuffer.
  compileMaia (phase 4, WASM compile, on first advanceContinuation): new
    InferenceSession from buffer. ort.env.wasm.wasmPaths points to jsdelivr CDN.
initMaia: guards via maiaInitStarted. All ort.* usage gated on maiaReady. UI
  controls hidden until ready.
ORT_SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/onnxruntime-web@1.21.0/dist/ort.min.js'.
yieldToIdle wraps requestIdleCallback with timeout fallback.
maiaMirrorFEN for black-to-move: 64×12 board always from white's perspective.
4352-element policy space.
Continuation-opponent sliders: ELO 1600–2200 step 25, default 2000; precision
  0.0–1.0 step 0.05, default 0.75 (1.0=argmax, 0.0=high-temp sample).

Analysis Mode has its OWN Maia usage — `maiaPolicyTopMoves(fen, elo)` reuses the
  SAME compiled ONNX session but runs the full policy softmax at temperature 1
  rather than sampling one move at a precision setting. Its ELO slider
  (`analysisMaiaElo`, same 1600–2200/step-25 range) is independent of the
  continuation opponent's maiaElo/precision — no precision knob there.

─── RENDERARROWS PIPELINE ───

renderArrows() = unified SVG overlay draw, z-order back-to-front:
  1. filterArrows (frequency, green; skip if equal to hoverFilterArrow)
  2. hoverFilterArrow (blue 0.8)
  3. analysisArrow (light-blue 0.5; SUPPRESSED when continuationMode)
  4. analysisPvArrows OR bestMoveArrow (mutually exclusive; bestMoveArrow gated
     on !suppressEngineArrows = !(advantageMode && continuationMode))
  5. noteArrows (semi-transparent 0.55; only when note panel OPEN)
  6. hintArrow (blue 0.7)
  7. activeArrows (user-drawn)
  8. ghostArrow (during drag, 0.6)
  9. activeCircles
 10. noteCircles (0.55)
 11. ghostCircle
 12. moveClassBadge (foreignObject + flexbox; 'book' uses emoji font stack)

chessboard.js highlight1/highlight2 overridden transparent.
Previous move highlight: showLastMoveHighlight / Deferred / Clear. Applies to all
  moves including auto-played. Clears on retryPosition and loadPosition.
clearBoardOverlays = clearHighlights + clearHintHighlights + clearArrows + clearCheckHighlight.
clearFeedbackAndOverlays = above + feedback box + contLine + posInfo + premove +
  ghosts + lastmove + window._reviewBestLine. Single caller (activatePanelTab).
Analysis-mode Maia/DB overlay clickable extra rows draw through the existing
  analysisArrow/bestMoveArrow machinery — no new arrow layer for them.

─── MISC ───

- Toast notifications: showToast(message, {type:'success'|'error', duration,
  actionLabel, onAction}) — fixed bottom-center stack (#toastContainer,
  z-index 300, max 3, auto-dismiss default 4s, tap-to-dismiss). The app's
  replacement for native alert(); actionLabel/onAction is the undo pattern.
  Message set via textContent (safe for interpolated strings). Mobile bottom
  offsets: 90px, 136px when body.line-nav-active.
- MISTAKES_CACHE_VERSION = 8 (bump on schema/extraction change)
- REPERTOIRE_CACHE_VERSION = 5
- FEN_DB_VERSION = 1
- REVIEWS_DB_VERSION = 1 (separate DB 'mistakelab_reviews' — never bump
  FEN_DB_VERSION for review-related schema changes)
- REVIEW_HISTORY_CAP = 150
- TODO_WINRATE_WINDOW = 5
- ANALYSIS_MAIA_CACHE_MAX = 300, ANALYSIS_DB_CACHE_MAX = 300
- ANALYSIS_DB_MIN_GAMES = 5, ANALYSIS_DB_HIDE_MAIA_MIN = 25,
  ANALYSIS_DB_EXTRA_MIN = 15%, ANALYSIS_DB_DEBOUNCE_MS = 300
- GIST_FETCH_CACHE_TTL = 5000 ms
- onSnapEnd must use animate=false (prevent teleport)
- showMainApp guarded by appShown (once only)
- Close MistakeLab tabs before running analyzer (debounced Gist overwrites)
- SILENT_EVAL_DEPTH = 22, SILENT_REVIEW_MIN_MOVES = 5 (also the min user-move
  count for a Game Review History snapshot to be captured at all)
- "Game Review" in code comments = contLineReviewMode (live in-session review),
  distinct from — but feeding — the persisted Game Review HISTORY reachable
  later from the Games tab.
- Test injections still present (remove before release):
    Test game: id '_test_clocks_6sS121tG' (Zukertort Sicilian Invitation)
    Test tactic: id '_test_bxf7' Bxf7+ puzzle (TWO alt lines: Kxf7→Ne6, Kxf7→Qf3#)

─── FIXED GOTCHAS TO REMEMBER (unique/non-obvious traps not fully covered above) ───

- Never race IDB preload against chess.js fallback — chess.js saturates main
  thread and blocks IDB result delivery. Wait unconditionally.
- Brave silently enforces ~2.86MB localStorage quota.
- getAll()/getAllKeys() is 50× faster than cursor iteration on mobile.
- gameFenIndex.size > 0 guard in buildFenIndex — an empty Map passes truthy guards.
- Voice is never auto-enabled across sessions (voiceSettings.enabled forced false
  on load) — don't "fix" this if it looks like a bug.
- Manifest has NO orientation key — respects OS lock. Do not add "orientation": "any".
- practiceContItem manipulates filteredMistakes directly; cleanupPracticeCont
  avoids exitOpeningFilter (which applyFilters-wipes the practice item).
- Repertoire trie is Map<posKey, Array<{san, uci, ...meta}>> — multi-value, NOT a
  single {san, uci} object. Use trieFirst/trieMatchesMove.
- Repertoire continuation uses trie-only (getRepertoireContinuations), NOT the
  DB→Maia→SF chain used everywhere else.
- Explorer scoring in TODO generation keys BOTH uci AND 's:'+SAN — chess.js
  castling (e1g1) differs from the explorer's (e1h1); uci-only lookup would sink
  O-O below rare sidelines. Lichess explorer requires an auth token.
- savePracticeMistake / savePracticeTactic / recordPracticeResult /
  confirmSaveSequence all produce different shapes even where they overlap
  (savePracticeTactic and confirmSaveSequence both write practiceTactics) — check
  the relevant section before assuming a field exists.
- Mistakes have no posId prefix; only tactics ('t') and advantages ('a') do.
  Repertoire posId is 'r_' + positionKey (no gameId).
- DOM selector `#feedbackContainer .engine-line-moves .move` matches BOTH
  engineLineBox and .feedback-box; use `#feedbackContainer > .feedback-box
  .engine-line-moves .move` for scoped selection.
- Maia generation steps: 25 cp for ELO, 0.05 for precision. Defaults: 2000, 0.75.
  Analysis Mode's Maia overlay shares the ELO step/range but has no precision knob.

─── DOC MAINTENANCE ───

This file lives in the repo (not Project Knowledge) specifically so it can be
  edited with str_replace in the same session as the code change it documents,
  and so its history is real git history instead of manually-managed uploads.
Update it whenever a change alters behavior/architecture described above — same
  commit as the code change where practical, or a dedicated "docs: update
  architecture reference" commit otherwise. No buildVersion bump needed for
  doc-only commits (that rule is index.html-specific).
