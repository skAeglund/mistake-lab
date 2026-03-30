# MistakeLab — Chess Mistake Review Trainer

Fetch your analyzed Lichess games, find mistakes and blunders, and practice finding better moves with Stockfish 18 evaluation and FSRS-5 spaced repetition.

## Setup

### 1. Get Stockfish 18 engine files

```bash
npm pack stockfish@18
tar -xzf stockfish-18.*.tgz
mkdir -p engine
cp package/bin/stockfish-18-lite-single.js engine/
cp package/bin/stockfish-18-lite-single.wasm engine/
rm -rf package stockfish-18.*.tgz
```

Your directory should look like:

```
mistakelab/
├── index.html
├── engine/
│   ├── stockfish-18-lite-single.js
│   └── stockfish-18-lite-single.wasm
└── README.md
```

### 2. Deploy to GitHub Pages

1. Create a new GitHub repository (e.g. `mistakelab`)
2. Push these files to the `main` branch
3. Go to **Settings → Pages → Source** → select `main` branch → Save
4. Your trainer will be live at `https://yourusername.github.io/mistakelab/`

### 3. Cloud sync (optional)

To sync progress across devices:

1. Create a [GitHub personal access token](https://github.com/settings/tokens/new?scopes=gist&description=MistakeLab) with only the `gist` scope
2. On the login screen, expand "Set up cloud sync"
3. Paste your token and click Save — a private Gist will be created automatically

### Local development

To test locally, serve the files with any HTTP server:

```bash
# Python
python -m http.server 8000

# Node.js
npx serve .
```

Then open `http://localhost:8000`

> **Note:** Opening `index.html` directly via `file://` won't work because browsers block Web Workers from file URLs.

## Features

- Fetches analyzed Lichess games and extracts mistakes using win% probability model
- Interactive chessboard with drag-and-drop and click-to-move
- Stockfish 18 WASM evaluation with streaming pre-analysis (depth 16–30)
- Color-coded feedback (green→red gradient based on win% drop)
- Engine continuation lines you can click through
- FSRS-5 spaced repetition with automatic grading
- Review mode for due positions
- Cloud sync via GitHub Gist
- Pause/resume engine analysis
- Open any position in Lichess analysis

## License

GPL-3.0 (due to Stockfish dependency)
