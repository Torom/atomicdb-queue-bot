# AtomicDB Minimax Queue Bot

An intelligent automation bot for the **[AtomicDB](https://belzedar.duckdns.org/atomicdb/)** project to solve atomic chess. Managed with the modern Python project manager **`uv`**.

---

## Overview & How It Works

The bot traverses the minimax / negamax game tree starting from a root position (by default, the standard atomic chess opening) and submits critical **leaf nodes live to the server queue** via the official AtomicDB API (`POST /atomicdb/api/request`).

Because leaves are submitted immediately as they are discovered during tree traversal, server workers can start computing positions right away while the bot continues analyzing further branches.

### Key Features

1. **Live Queue Submissions**: Discovered leaf nodes are submitted directly, minimizing worker idle time.
2. **Near-Tie Exploration (`margin`)**: The tree frequently features moves that share the top score or differ by only a few centipawns. The bot explores these near-optimal alternatives within a configurable margin.
3. **Game-Theoretic AND/OR Proof Search (`and_or`)**: When solving an advantage (e.g. White winning after `1. Nf3`), the attacker (OR node) plays the best moves while the defender (AND node) must have every legal response refuted. The bot systematically identifies and refutes defensive tries.
4. **Cycle & Transposition Handling**: Detects repetition loops in the database and breaks cycles cleanly (`cycle_exit`) by queuing unanalyzed alternative moves.
5. **Draw & 0 cp Filtering**: By default, avoids queuing draws and 0 cp perpetual check traps (`--skip-zero-cp`), focusing computing power on decisive advantages.
6. **Reactive Daemon Mode (`--mode loop`)**: Rather than sleeping for fixed intervals, the daemon actively monitors submitted tasks. As soon as workers finish a task, the bot instantly re-traverses the updated tree.
7. **Dynamic Minimax Updates**: As workers submit evaluations, values backpropagate through the database. The bot recalculates paths dynamically on each cycle, adapting immediately to refuted lines or deepened variations.

---

## Installation & Setup with `uv`

The project is fully managed using `pyproject.toml` and `uv`:

```bash
# Clone the repository
git clone <repo-url>
cd atomicdb

# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies into the virtual environment
uv sync
```

---

## Usage

### 1. Dry Run (Simulation)
Test traversal and see which positions would be queued without sending any real API requests:
```bash
uv run atomic_minimax_queue.py --dry-run
```

Limit the number of discovered leaves for a quick test:
```bash
uv run atomic_minimax_queue.py --max-leaves 2 --dry-run
```

---

### 2. Search Strategies

- **`--strategy margin` (Default, recommended for general analysis)**:
  Explores all branches within a centipawn margin (`--margin-cp 10` by default) of the best move at each ply.
  ```bash
  uv run atomic_minimax_queue.py --strategy margin --margin-cp 10
  ```

- **`--strategy exact` (Strict Principal Variation)**:
  Only follows moves that exactly share the top minimax evaluation (`score == best_score`).
  ```bash
  uv run atomic_minimax_queue.py --strategy exact
  ```

- **`--strategy and_or` (Game Tree Solving)**:
  Treats the root player as the attacker (OR nodes, picking best moves within margin) and the opponent as the defender (AND nodes, queuing multiple unrefuted defensive options up to `--defender-branching`).
  ```bash
  uv run atomic_minimax_queue.py --strategy and_or --margin-cp 10 --defender-branching 3
  ```

---

### 3. Reactive Daemon Mode (`--mode loop`)

Runs continuously in the background as a self-driving service:
- Tracks submitted task IDs using real-time polling.
- Displays live status updates (task ID, queue position, active worker, node budget).
- Triggers the next tree traversal **immediately** when an active task finishes.
```bash
uv run atomic_minimax_queue.py --mode loop
```

---

### 4. Authenticated Runs (OpenBench / AtomicDB Account)

When providing your OpenBench account credentials, queued tasks are assigned to your account, granting worker affinity, priority handling, and tracking via your user queue (`my-queue`):
```bash
uv run atomic_minimax_queue.py --username YOUR_USERNAME --password YOUR_PASSWORD --mode loop
```

---

### 5. Analyzing Specific Variations or Openings (`--moves` or `--fen`)

You can anchor the search at any line or defense variation:

- **Via Move Sequence (`--moves`)** *(Easiest)*:
  Accepts moves in standard UCI or SAN format:
  ```bash
  # Analyze Black's defenses after 1. Nf3:
  uv run atomic_minimax_queue.py --moves "g1f3"

  # Analyze a specific variation (e.g. 1. Nf3 f6 2. e4 c6):
  uv run atomic_minimax_queue.py --moves "1. Nf3 f6 2. e4 c6"
  ```

- **Via Custom FEN (`--fen`)**:
  Works whether White or Black is to move (scores are automatically evaluated from the mover's perspective):
  ```bash
  # Position after 1. Nf3 (Black to move):
  uv run atomic_minimax_queue.py --fen "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq - 0 1"
  ```

---

## Command-Line Options

| Parameter | Default | Description |
|---|---|---|
| `--fen` | Start position | Root FEN position for tree traversal |
| `--moves` | `None` | Initial move sequence in UCI or SAN (`"1. Nf3 f6"`) to set root position |
| `--strategy` | `margin` | Traversal strategy (`margin` [default], `exact`, `and_or`) |
| `--margin-cp` | `10` | Centipawn tolerance for near-ties (used by `margin` and `and_or`) |
| `--defender-branching`| `3` | Max defensive moves per AND node (in `and_or` strategy) |
| `--max-depth` | `0` | Max search depth in plies (`0` = unlimited) |
| `--max-leaves` | `0` | Max leaves to queue per cycle (`0` = unlimited) |
| `--max-queries` | `0` | Max API queries per cycle (`0` = unlimited) |
| `--skip-zero-cp` | `True` | Skip 0 cp positions (draws / perpetuals) and do not queue them |
| `--min-score` | `Auto` | Minimum score for root player moves (`>= 1 cp` if winning, unrestricted if `<= 0 cp`) |
| `--budget` | `128000000` | Node budget for analysis tasks (default: 128M) |
| `--mode` | `once` | Operating mode: `once` (single run) or `loop` (reactive daemon) |
| `--poll-interval` | `5` | Polling interval in seconds to check task progress in loop mode |
| `--max-wait` | `600` | Max wait time in seconds for active tasks before forcing a new cycle (`0` = unlimited) |
| `--idle-interval` | `30` | Idle wait time in seconds if no tasks are active |
| `--server` | `https://belzedar.duckdns.org/atomicdb` | AtomicDB base URL |
| `--username` | `""` | OpenBench username (optional) |
| `--password` | `""` | OpenBench password (optional) |
| `--dry-run` | `False` | Simulate traversal without sending queue requests |

---

## License

MIT License. See repository for details.
