#!/usr/bin/env python3
"""
AtomicDB Minimax Queue Bot
==========================
Automated exploration and queuing of critical minimax leaf nodes
for the Atomic Chess solving project AtomicDB (https://belzedar.duckdns.org/atomicdb/).

How it works:
1. Traverses the Minimax/Negamax game tree starting from a root position (default: start position).
2. Identifies leaf nodes along critical paths (Principal Variations):
   - Within a centipawn tolerance window (strategy 'margin', default: 10 cp)
   - Exactly matching the top evaluation (strategy 'exact')
   - Or using Proof-Number / AND-OR tree logic (strategy 'and_or'):
     * Attacker (White): Explores best winning attempt(s)
     * Defender (Black): Explores all unsolved defensive replies
3. Prioritizes deeper leaves along the main line for maximum leverage on the minimax value.
4. Queues discovered leaves live during traversal via the official AtomicDB API ('/atomicdb/api/request'),
   logs the result, and continues searching seamlessly.
5. Dynamically updates the minimax tree as new engine evaluations become available.
"""

import argparse
import hashlib
import http.client
import json
import logging
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from heapq import heappop, heappush
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Import chess.variant (with automatic virtual environment detection)
try:
    import chess
    import chess.variant
except ImportError:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_site = os.path.join(script_dir, '.venv', 'lib', 'python3.14', 'site-packages')
    if os.path.exists(venv_site):
        sys.path.insert(0, venv_site)
        import chess
        import chess.variant
    else:
        # Search for other Python versions in .venv
        venv_lib = os.path.join(script_dir, '.venv', 'lib')
        found = False
        if os.path.exists(venv_lib):
            for py_dir in os.listdir(venv_lib):
                cand = os.path.join(venv_lib, py_dir, 'site-packages')
                if os.path.exists(cand):
                    sys.path.insert(0, cand)
                    try:
                        import chess
                        import chess.variant
                        found = True
                        break
                    except ImportError:
                        pass
        if not found:
            print("Error: 'chess' module not found. Please run 'pip install chess' or 'uv sync'.", file=sys.stderr)
            sys.exit(1)

# Default Server URL
DEFAULT_SERVER = "https://belzedar.duckdns.org/atomicdb"
DEFAULT_START_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

logger = logging.getLogger("AtomicQueueBot")


def canonicalize_fen(fen: str) -> str:
    """
    Standardizes the FEN string to AtomicDB's format:
    Halfmove clock and fullmove number are normalized to '0 1'.
    """
    parts = fen.strip().split()
    if len(parts) >= 4:
        return f"{parts[0]} {parts[1]} {parts[2]} {parts[3]} 0 1"
    return fen


def resolve_root_fen(base_fen: str, moves_str: Optional[str] = None) -> str:
    """
    Computes the resulting root FEN by playing moves_str (UCI or SAN)
    starting from base_fen.
    """
    board = chess.variant.AtomicBoard(base_fen)
    if not moves_str:
        return canonicalize_fen(board.fen())

    tokens = moves_str.replace(",", " ").split()
    for token in tokens:
        cleaned = token.rstrip(".").strip()
        if not cleaned or cleaned.isdigit():
            continue
        if "." in token:
            token = token.split(".")[-1]
            if not token:
                continue

        move_obj = None
        try:
            m = chess.Move.from_uci(token)
            if m in board.legal_moves:
                move_obj = m
        except Exception:
            pass

        if move_obj is None:
            try:
                move_obj = board.parse_san(token)
            except Exception as e:
                raise ValueError(f"Could not parse move '{token}' from sequence '{moves_str}': {e}")

        board.push(move_obj)

    return canonicalize_fen(board.fen())


def key_of(fen: str) -> str:
    """Computes the SHA256 key of a position, as used by AtomicDB."""
    return hashlib.sha256(canonicalize_fen(fen).encode('utf-8')).hexdigest()


@dataclass
class LeafCandidate:
    fen: str
    path: List[str]
    depth: int
    score: Optional[int]
    reason: str  # 'not_in_db', 'zero_visits', 'child_zero_visits', 'cycle_exit'
    parent_fen: Optional[str] = None
    priority: float = 0.0

    def __lt__(self, other: 'LeafCandidate') -> bool:
        return self.priority < other.priority


class AtomicDBClient:
    """Client for the AtomicDB HTTP API with persistent HTTP/1.1 Keep-Alive connection pooling."""

    def __init__(self, base_url: str = DEFAULT_SERVER, username: str = "", password: str = ""):
        self.base_url = base_url.rstrip('/')
        parsed = urllib.parse.urlparse(self.base_url)
        self.scheme = parsed.scheme.lower() if parsed.scheme else 'https'
        self.host = parsed.netloc
        self.path_prefix = parsed.path.rstrip('/')
        self.username = username
        self.password = password
        self.cache: Dict[str, Optional[dict]] = {}
        self.query_count = 0
        self.last_request_time = 0.0
        self.ssl_context = ssl.create_default_context() if self.scheme == 'https' else None
        self._conn: Optional[http.client.HTTPConnection] = None

    def _get_connection(self, timeout: float = 15.0) -> http.client.HTTPConnection:
        if self._conn is None:
            if self.scheme == 'https':
                self._conn = http.client.HTTPSConnection(
                    self.host, timeout=timeout, context=self.ssl_context
                )
            else:
                self._conn = http.client.HTTPConnection(
                    self.host, timeout=timeout
                )
        return self._conn

    def _close_connection(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self):
        """Closes the underlying persistent HTTP connection."""
        self._close_connection()

    def _rate_limit(self, min_interval: float = 0.21):
        """
        Enforces a minimum interval between requests (default: 0.21s = ~4.76 req/s)
        to stay safely within Nginx's rate limit of 5 req/s regardless of network RTT.
        """
        now = time.perf_counter()
        elapsed = now - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.perf_counter()

    def _http_request(
        self,
        method: str,
        path: str,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: float = 15.0
    ) -> Tuple[int, str, Dict[str, str], bytes]:
        """
        Executes an HTTP request reusing the persistent Keep-Alive connection.
        Automatically reconnects if the socket was dropped by the server (e.g. idle timeout).
        """
        self._rate_limit(0.21)

        full_path = f"{self.path_prefix}/{path.lstrip('/')}"
        req_headers = {
            'User-Agent': 'AtomicMinimaxQueueBot/1.0',
            'Connection': 'keep-alive',
        }
        if headers:
            req_headers.update(headers)

        body_bytes = None
        if data is not None:
            body_bytes = urllib.parse.urlencode(data).encode('utf-8')
            req_headers.setdefault('Content-Type', 'application/x-www-form-urlencoded')

        for attempt in range(2):
            conn = self._get_connection(timeout)
            try:
                conn.request(method, full_path, body=body_bytes, headers=req_headers)
                resp = conn.getresponse()
                raw_data = resp.read()
                resp_headers = {k.lower(): v for k, v in resp.getheaders()}
                return resp.status, resp.reason, resp_headers, raw_data
            except (http.client.HTTPException, ConnectionError, OSError):
                self._close_connection()
                if attempt == 0:
                    continue
                raise

    def query(self, fen: str, bypass_cache: bool = False) -> Optional[dict]:
        """
        Queries status and moves of a position (GET /atomicdb/api/query?fen=...).
        Returns None on 404 (position not yet in tree).
        """
        fen = canonicalize_fen(fen)
        if not bypass_cache and fen in self.cache:
            return self.cache[fen]

        self.query_count += 1
        path = f"/api/query?fen={urllib.parse.quote(fen)}"

        retries = 3
        while retries > 0:
            try:
                status, reason, hdrs, body = self._http_request('GET', path, timeout=15.0)
                if status == 200:
                    data = json.loads(body.decode('utf-8'))
                    self.cache[fen] = data
                    return data
                elif status == 404:
                    self.cache[fen] = None
                    return None
                elif status in (429, 503):
                    retry_after = int(hdrs.get('retry-after', '2'))
                    logger.warning(f"HTTP {status} ({reason}) from server. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    retries -= 1
                    continue
                else:
                    logger.error(f"HTTP error {status} querying {fen}: {reason}")
                    return None
            except Exception as e:
                logger.error(f"Network error querying {fen}: {e}")
                time.sleep(1)
                retries -= 1

        return None

    def request_analysis(
        self,
        fen: str,
        budget: Optional[int] = None,
        only_if_missing: bool = True
    ) -> dict:
        """
        Queues a position for analysis (POST /atomicdb/api/request).
        """
        fen = canonicalize_fen(fen)
        post_data = {'fen': fen}
        if only_if_missing:
            post_data['only_if_missing'] = '1'
        if self.username and self.password:
            post_data['username'] = self.username
            post_data['password'] = self.password
        if budget is not None:
            post_data['budget'] = str(budget)

        try:
            status, reason, hdrs, body = self._http_request('POST', '/api/request', data=post_data, timeout=15.0)
            try:
                return json.loads(body.decode('utf-8'))
            except Exception:
                return {'status': 'http_error', 'code': status, 'reason': reason}
        except Exception as e:
            return {'status': 'error', 'reason': str(e)}

    def my_queue(self) -> Optional[List[dict]]:
        """
        Retrieves the account's task list (POST /atomicdb/api/my-queue/).
        Returns the list of tasks (found under 'tasks' in the server response).
        """
        if not self.username or not self.password:
            return []

        post_data = {
            'username': self.username,
            'password': self.password
        }
        try:
            status, reason, hdrs, body = self._http_request('POST', '/api/my-queue/', data=post_data, timeout=15.0)
            if status == 200:
                data = json.loads(body.decode('utf-8'))
                if isinstance(data, dict):
                    if 'error' in data:
                        logger.warning(f"my-queue reported error: {data['error']}")
                        return None
                    return data.get('tasks', [])
                elif isinstance(data, list):
                    return data
                return []
            elif status in (429, 503):
                retry_after = int(hdrs.get('retry-after', '2'))
                logger.warning(f"HTTP {status} ({reason}) on my-queue. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return None
            else:
                logger.warning(f"HTTP {status} on my-queue: {reason}")
                return None
        except Exception as e:
            logger.warning(f"Could not retrieve 'my-queue' ({e}). Retrying in next interval...")
            return None

    def live_request(self, key: str) -> Optional[dict]:
        """
        Queries the live status of a task (GET /atomicdb/api/live-request/<key>/).
        Returns e.g. {'live': True, 'text': ..., 'chip': ...} or {'live': False}.
        """
        try:
            status, reason, hdrs, body = self._http_request('GET', f"/api/live-request/{key}/", timeout=10.0)
            if status == 200:
                return json.loads(body.decode('utf-8'))
            elif status == 404:
                return {'live': False}
            logger.warning(f"HTTP {status} in live-request for {key}: {reason}")
            return None
        except Exception as e:
            logger.warning(f"Error in live-request for {key}: {e}")
            return None

    def cancel_task(self, task_id: int) -> dict:
        """
        Withdraws a task from the queue (POST /atomicdb/api/queue-cancel/<task_id>/).
        """
        if not self.username or not self.password:
            return {'status': 'unauthorized', 'reason': 'No credentials provided'}

        post_data = {
            'username': self.username,
            'password': self.password
        }
        try:
            status, reason, hdrs, body = self._http_request('POST', f"/api/queue-cancel/{task_id}/", data=post_data, timeout=15.0)
            return json.loads(body.decode('utf-8'))
        except Exception as e:
            return {'status': 'error', 'reason': str(e)}


class MinimaxFrontierExplorer:
    """Traverses the game tree and determines critical leaf nodes."""

    def __init__(
        self,
        client: AtomicDBClient,
        strategy: str = "margin",
        margin_cp: int = 10,
        defender_branching: int = 3,
        max_depth: int = 0,
        max_leaves: int = 0,
        max_queries: int = 0,
        skip_zero_cp: bool = True,
        min_score: Optional[int] = None
    ):
        self.client = client
        self.strategy = strategy
        self.margin_cp = margin_cp
        self.defender_branching = defender_branching
        self.max_depth = max_depth  # 0 = unlimited
        self.max_leaves = max_leaves  # 0 = unlimited
        self.max_queries = max_queries  # 0 = unlimited
        self.skip_zero_cp = skip_zero_cp
        self.min_score = min_score
        self.known_analyzed_fens: Set[str] = set()

    def explore(
        self,
        root_fen: str,
        on_leaf: Optional[Callable[[LeafCandidate], None]] = None
    ) -> List[LeafCandidate]:
        """
        Executes a Best-First / Priority-Queue search starting from root_fen.
        If on_leaf is provided, each discovered leaf node is immediately passed
        to this callback (e.g. for live queuing and logging).
        Returns prioritized LeafCandidate objects.
        """
        root_fen = canonicalize_fen(root_fen)
        root_stm = root_fen.split()[1] if len(root_fen.split()) > 1 else 'w'
        leaves: List[LeafCandidate] = []
        seen_leaves: Set[str] = set()
        cycle_attacker_events: List[Tuple[int, List[str]]] = []

        # Effective min_score:
        # If user explicitly specified min_score, use it.
        # Otherwise, dynamically determine it from root position score:
        # If root player has an advantage (> 0 cp), protect that advantage (>= 1 cp).
        # If root player is disadvantaged or neutral (<= 0 cp, e.g. Black defending against 1. Nf3),
        # disable min_score (None) to allow exploring best defenses without filtering out negative scores.
        effective_min_score: Optional[int] = self.min_score
        min_score_initialized = (self.min_score is not None)

        # Priority Queue for open search nodes: (priority, depth, path, path_fens, fen)
        frontier = []
        heappush(frontier, (0.0, 0, [], {root_fen}, root_fen))
        visited_nodes: Set[str] = set()

        last_logged_query = 0
        while frontier:
            if self.max_leaves > 0 and len(leaves) >= self.max_leaves:
                logger.info(f"Max leaves limit ({self.max_leaves}) reached. Terminating traversal early.")
                break

            if self.max_queries > 0 and self.client.query_count >= self.max_queries:
                logger.info(f"Max queries limit ({self.max_queries}) reached for this cycle.")
                break

            if self.client.query_count - last_logged_query >= 20 and self.client.query_count > 0:
                last_logged_query = self.client.query_count
                curr_depth = frontier[0][1] if frontier else 0
                logger.info(
                    f"Traversing: {self.client.query_count} queries | "
                    f"{len(leaves)} leaves queued | Depth: {curr_depth} plies | "
                    f"Open nodes: {len(frontier)}"
                )

            priority, depth, path, path_fens, fen = heappop(frontier)

            if self.max_depth > 0 and depth > self.max_depth:
                continue

            if fen in visited_nodes:
                continue
            visited_nodes.add(fen)

            data = self.client.query(fen)
            if not data:
                # Position does not yet exist in DB -> leaf node!
                if fen not in seen_leaves:
                    seen_leaves.add(fen)
                    leaf = LeafCandidate(
                        fen=fen,
                        path=path,
                        depth=depth,
                        score=None,
                        reason='not_in_db',
                        priority=priority
                    )
                    leaves.append(leaf)
                    if on_leaf:
                        on_leaf(leaf)
                continue

            status = data.get('status', 'UNKNOWN')
            if status != 'UNKNOWN':
                # Already proven (WHITE_WIN, BLACK_WIN, DRAW) -> branch closed/solved!
                continue

            # Side to move
            stm = fen.split()[1] if len(fen.split()) > 1 else 'w'
            visits = data.get('visits', 0)
            moves = data.get('moves', [])
            best_move = data.get('best_move')
            node_score = data.get('score')

            if not min_score_initialized and depth == 0:
                min_score_initialized = True
                if node_score is not None and node_score > 0:
                    effective_min_score = 1
                else:
                    effective_min_score = None

            # 0 cp / loss check for existing nodes:
            if self.skip_zero_cp and node_score is not None and node_score == 0:
                if depth == 0:
                    logger.info("Root position has score 0 cp. With --skip-zero-cp, no tree will be explored.")
                continue

            if effective_min_score is not None and node_score is not None and stm == root_stm and node_score < effective_min_score:
                continue

            if visits == 0:
                # Position exists in DB but has 0 visits -> leaf node!
                if self.skip_zero_cp and node_score is not None and node_score == 0:
                    continue
                if effective_min_score is not None and stm == root_stm and node_score is not None and node_score < effective_min_score:
                    continue

                if fen not in seen_leaves:
                    seen_leaves.add(fen)
                    leaf = LeafCandidate(
                        fen=fen,
                        path=path,
                        depth=depth,
                        score=node_score,
                        reason='zero_visits',
                        priority=priority
                    )
                    leaves.append(leaf)
                    if on_leaf:
                        on_leaf(leaf)
                continue

            if not moves:
                # Position already has visits (>0) but no legal follow-up moves (terminal/dead end)
                continue

            # Select moves to follow according to chosen strategy
            selected_moves = self._select_moves(
                stm, moves, best_move,
                root_stm=root_stm,
                node_score=node_score,
                effective_min_score=effective_min_score
            )

            # Execute selected follow-up moves on the board
            try:
                board = chess.variant.AtomicBoard(fen)
            except Exception as e:
                logger.error(f"Invalid FEN {fen}: {e}")
                continue

            # Check which selected moves do not lead to a path repetition (cycle)
            valid_candidates = []
            repeated_fens = []
            for m in selected_moves:
                uci = m['uci']
                try:
                    board_copy = board.copy()
                    board_copy.push_uci(uci)
                    child_fen = canonicalize_fen(board_copy.fen())
                except Exception as ex:
                    logger.warning(f"Could not execute move {uci} on {fen}: {ex}")
                    continue

                if child_fen not in path_fens:
                    valid_candidates.append((m, child_fen))
                else:
                    repeated_fens.append(child_fen)
                    logger.debug(f"Move {uci} leads to repetition (position already on path).")

            # If ALL selected moves lead to a repetition (draw):
            if not valid_candidates:
                if stm != root_stm:
                    # Defender forces a draw by repetition (0 cp).
                    # In minimax, a losing defender chooses a draw over losing non-cyclic moves.
                    # This branch ends in a draw and is discarded for the winning-seeking attacker.
                    moves_str = ", ".join(m.get('uci', '?') for m in selected_moves)
                    logger.info(
                        f"Repetition at depth {depth}: Defender forces draw via "
                        f"{moves_str} -> branch discarded."
                    )
                    if len(path) >= 1:
                        # Inspect attacker nodes from recent moves (within the cycle range)
                        for p_len in range(len(path) - 1, max(0, len(path) - 16), -1):
                            is_attacker_turn = (p_len % 2 == 0)
                            if is_attacker_turn:
                                cycle_attacker_events.append((p_len, path[:p_len]))
                    continue

                # Only the attacker (stm == root_stm) diverges to non-cyclic alternatives
                # to avoid a draw and prove a real win (> 0 cp):
                non_cyclic_scored = []
                non_cyclic_unvisited = []

                for m in moves:
                    uci = m.get('uci')
                    if not uci or m.get('floor', False) or m.get('status') not in ('UNKNOWN', None):
                        continue
                    try:
                        board_copy = board.copy()
                        board_copy.push_uci(uci)
                        c_fen = canonicalize_fen(board_copy.fen())
                    except Exception:
                        continue

                    if c_fen in path_fens:
                        continue  # Circular / repetition

                    m_visits = m.get('visits', 0)
                    m_score = m.get('score')
                    if m_visits > 0 or m_score is not None:
                        if self.skip_zero_cp and m_score == 0:
                            continue
                        if stm == root_stm and effective_min_score is not None and m_score is not None and m_score < effective_min_score:
                            continue
                        non_cyclic_scored.append((m, c_fen))
                    else:
                        non_cyclic_unvisited.append((m, c_fen, board_copy))

                # Path 1: Scored non-cyclic moves exist
                if non_cyclic_scored:
                    non_cyclic_scored.sort(key=lambda item: item[0].get('score', -99999), reverse=True)
                    best_non_cyclic = non_cyclic_scored[0][0].get('score')
                    if best_non_cyclic is not None:
                        margin_tolerance = 0 if self.strategy == "exact" else self.margin_cp
                        min_allowed = best_non_cyclic - margin_tolerance
                        if self.skip_zero_cp and (stm == root_stm or best_non_cyclic > 0):
                            if effective_min_score is not None:
                                min_allowed = max(min_allowed, effective_min_score)
                        valid_candidates = [
                            (m, c_fen) for m, c_fen in non_cyclic_scored
                            if m.get('score') is not None and m['score'] >= min_allowed
                        ]

                # Path 2: No scored non-cyclic moves meet margin, but unanalyzed moves exist (visits == 0).
                # When all analyzed moves loop, unanalyzed moves are the only way to break the cycle ('cycle_exit')!
                if not valid_candidates and non_cyclic_unvisited:
                    def unvisited_priority(item):
                        m_cand, _c_fen, _b_copy = item
                        try:
                            move_obj = chess.Move.from_uci(m_cand['uci'])
                            is_cap = board.is_capture(move_obj)
                            gives_chk = board.gives_check(move_obj)
                            piece = board.piece_at(move_obj.from_square)
                            is_pawn = piece and piece.piece_type == chess.PAWN
                            return (1 if is_cap else 0, 1 if gives_chk else 0, 1 if is_pawn else 0)
                        except Exception:
                            return (0, 0, 0)

                    non_cyclic_unvisited.sort(key=unvisited_priority, reverse=True)
                    # Queue up to 5 unanalyzed exit moves as leaf candidates
                    max_exits = 5 if (self.max_leaves == 0 or self.max_leaves > 5) else self.max_leaves
                    for m, c_fen, _ in non_cyclic_unvisited[:max_exits]:
                        m_copy = dict(m)
                        m_copy['_is_cycle_exit'] = True
                        valid_candidates.append((m_copy, c_fen))

                    if valid_candidates:
                        logger.info(
                            f"Repetition at depth {depth} ({fen[:30]}...): "
                            f"{len(valid_candidates)} unanalyzed exit moves ('cycle_exit') selected as leaf candidates."
                        )
                    else:
                        logger.info(
                            f"Repetition at depth {depth}: Attacker has no non-cyclic exits left -> branch discarded."
                        )

            for m, child_fen in valid_candidates:
                uci = m['uci']
                move_visits = m.get('visits', 0)
                cached_child = self.client.cache.get(child_fen)
                if cached_child is not None and cached_child.get('visits', 0) > 0:
                    move_visits = cached_child.get('visits', 0)
                move_score = m.get('score')
                new_path = path + [uci]
                new_path_fens = path_fens | {child_fen}
                new_depth = depth + 1

                # 0 cp and min-score check for follow-up moves
                if self.skip_zero_cp and move_score is not None and move_score == 0:
                    continue
                if effective_min_score is not None and stm == root_stm and move_score is not None and move_score < effective_min_score:
                    continue

                # Branch priority calculation (Best-First / depth-first along PV):
                # 1. Deeper nodes first (factor -new_depth * 1000.0) -> advances to PV head immediately
                # 2. Prefer engine's official best move (best_move)
                # 3. Prefer moves with higher visit count
                # 4. Prefer higher minimax scores
                bm_bonus = -200.0 if (best_move and uci == best_move) else 0.0
                visit_bonus = -min(move_visits, 50) * 2.0
                score_bonus = -move_score / 10.0 if move_score is not None else 0.0
                child_priority = (-new_depth * 1000.0) + bm_bonus + visit_bonus + score_bonus

                if move_visits == 0 and child_fen not in self.known_analyzed_fens:
                    # Child has never been analyzed -> leaf node!
                    if child_fen not in seen_leaves:
                        seen_leaves.add(child_fen)
                        reason_str = 'cycle_exit' if m.get('_is_cycle_exit') else 'child_zero_visits'
                        leaf = LeafCandidate(
                            fen=child_fen,
                            path=new_path,
                            depth=new_depth,
                            score=move_score,
                            reason=reason_str,
                            parent_fen=fen,
                            priority=child_priority
                        )
                        leaves.append(leaf)
                        if on_leaf:
                            on_leaf(leaf)
                else:
                    heappush(frontier, (child_priority, new_depth, new_path, new_path_fens, child_fen))

        # Cycle Trap Recovery (Cycle-Exit Recovery):
        # If minimax search found no open leaves because all active branches
        # starved in closed repetition cycles:
        # Extract unanalyzed exit moves ('cycle_exit') from attacker cycle nodes
        # to actively break the cycle in the database!
        if not leaves and cycle_attacker_events:
            cycle_attacker_events.sort(key=lambda item: item[0], reverse=True)
            max_recovery = 5 if (self.max_leaves == 0 or self.max_leaves > 5) else self.max_leaves

            candidate_exits = []
            seen_cand_fens = set()
            for att_depth, att_path in cycle_attacker_events:
                if len(candidate_exits) >= max_recovery * 3:
                    break
                try:
                    b_att = chess.variant.AtomicBoard(root_fen)
                    for m_str in att_path:
                        b_att.push_uci(m_str)
                    att_fen = canonicalize_fen(b_att.fen())
                except Exception:
                    continue

                att_data = self.client.query(att_fen)
                if not att_data:
                    continue

                for m in att_data.get('moves', []):
                    uci = m.get('uci')
                    if not uci or m.get('floor', False) or m.get('status') not in ('UNKNOWN', None):
                        continue
                    if m.get('visits', 0) > 0:
                        continue
                    m_score = m.get('score')
                    if self.skip_zero_cp and m_score is not None and m_score == 0:
                        continue

                    try:
                        b_cand = b_att.copy()
                        move_obj = chess.Move.from_uci(uci)
                        is_cap = b_att.is_capture(move_obj)
                        gives_chk = b_att.gives_check(move_obj)
                        piece = b_att.piece_at(move_obj.from_square)
                        is_pawn = piece and piece.piece_type == chess.PAWN
                        b_cand.push_uci(uci)
                        cand_fen = canonicalize_fen(b_cand.fen())
                    except Exception:
                        continue

                    if cand_fen in seen_leaves or cand_fen in seen_cand_fens or cand_fen in self.known_analyzed_fens:
                        continue
                    seen_cand_fens.add(cand_fen)

                    prio_score = m_score if m_score is not None else -9999
                    prio_tuple = (prio_score, 1 if is_cap else 0, 1 if gives_chk else 0, 1 if is_pawn else 0)
                    candidate_exits.append((prio_tuple, att_depth + 1, att_path + [uci], cand_fen, m, att_fen))

            if candidate_exits:
                candidate_exits.sort(key=lambda x: x[0], reverse=True)
                for _, depth_val, path_val, cand_fen, m_dict, parent_fen_val in candidate_exits[:max_recovery]:
                    if cand_fen not in seen_leaves:
                        seen_leaves.add(cand_fen)
                        leaf = LeafCandidate(
                            fen=cand_fen,
                            path=path_val,
                            depth=depth_val,
                            score=m_dict.get('score'),
                            reason='cycle_exit',
                            parent_fen=parent_fen_val,
                            priority=-depth_val * 1000.0
                        )
                        leaves.append(leaf)
                        if on_leaf:
                            on_leaf(leaf)

                if leaves:
                    logger.info(
                        f"Cycle trap resolved: {len(leaves)} unanalyzed exit moves ('cycle_exit') "
                        f"found at depth {leaves[0].depth} to break repetition."
                    )

        # Sort leaves: most important and deepest along main variation first
        leaves.sort(key=lambda l: l.priority)
        return leaves

    def _select_moves(
        self,
        stm: str,
        moves: List[dict],
        best_move: Optional[str],
        root_stm: str = 'w',
        node_score: Optional[int] = None,
        effective_min_score: Optional[int] = None
    ) -> List[dict]:
        """Selects moves relevant for minimax/solving based on strategy."""
        scored_moves = [
            m for m in moves
            if m.get('score') is not None and not m.get('floor', False)
        ]

        if self.skip_zero_cp:
            # Filter out 0 cp moves (draw)
            scored_moves = [m for m in scored_moves if m['score'] != 0]
            # For root player (attacker), filter out moves below effective_min_score if active
            if stm == root_stm and effective_min_score is not None:
                scored_moves = [m for m in scored_moves if m['score'] >= effective_min_score]

        if not scored_moves:
            if not self.skip_zero_cp and best_move:
                return [m for m in moves if m['uci'] == best_move]
            return []

        best_score = max(m['score'] for m in scored_moves)
        if stm == root_stm and node_score is not None:
            # If server node_score is lower than max child score (e.g. child move loops
            # and was devalued by server backpropagation), orient margin on node_score:
            best_score = min(best_score, node_score)

        if self.strategy == "exact":
            # Strict best score
            selected = [
                m for m in scored_moves
                if m['score'] == best_score and m.get('status') == 'UNKNOWN'
            ]
        elif self.strategy == "margin":
            # All moves within margin_cp
            selected = [
                m for m in scored_moves
                if m['score'] >= best_score - self.margin_cp and m.get('status') == 'UNKNOWN'
            ]
            # Transposition/cycle resilience: If top move loops into a repetition, keep up
            # to 2 moves (within 30 cp) as lower-priority backups in tree:
            if stm == root_stm and len(selected) < 2:
                backup_margin = max(self.margin_cp, 30)
                selected = [
                    m for m in scored_moves
                    if m['score'] >= best_score - backup_margin and m.get('status') == 'UNKNOWN'
                ][:2]
        elif self.strategy == "and_or":
            # Proof-Number / Solving logic:
            # Root player (attacker) wants to win -> selects top moves
            # Opponent (defender) wants to survive -> all unsolved moves must be checked
            if stm == root_stm:
                # OR node: Top moves within tight margin
                selected = [
                    m for m in scored_moves
                    if m['score'] >= best_score - self.margin_cp and m.get('status') == 'UNKNOWN'
                ]
                if len(selected) < 2:
                    backup_margin = max(self.margin_cp, 30)
                    selected = [
                        m for m in scored_moves
                        if m['score'] >= best_score - backup_margin and m.get('status') == 'UNKNOWN'
                    ][:2]
            else:
                # AND node: Toughest defensive moves
                unsolved = [m for m in moves if m.get('status') == 'UNKNOWN']
                if self.skip_zero_cp:
                    unsolved = [m for m in unsolved if m.get('score') != 0]
                # Sort by best score for defender (highest score = toughest defense)
                unsolved.sort(key=lambda m: (m.get('score') is not None, m.get('score', -99999)), reverse=True)
                selected = unsolved[:self.defender_branching]
        else:
            # Default / Top-K
            selected = [
                m for m in scored_moves
                if m['score'] >= best_score - self.margin_cp and m.get('status') == 'UNKNOWN'
            ]

        if not selected and best_move and not self.skip_zero_cp:
            selected = [m for m in moves if m['uci'] == best_move]

        if best_move and len(selected) > 1:
            selected.sort(key=lambda m: (m.get('uci') != best_move, -m.get('score', -99999)))

        return selected


def print_leaf_report(leaves: List[LeafCandidate]):
    """Prints a structured summary of discovered leaf nodes."""
    print("\n" + "=" * 78)
    print(f" DISCOVERED MINIMAX LEAF NODES: {len(leaves)}")
    print("=" * 78)

    if not leaves:
        print(" No open leaf nodes found (tree is solved or saturated at current frontier).")
        return

    for idx, leaf in enumerate(leaves, 1):
        path_str = " ".join(leaf.path) if leaf.path else "(Root)"
        score_str = f"{leaf.score:+d} cp" if leaf.score is not None else "N/A"
        print(f"[{idx:02d}] Depth: {leaf.depth:2d} plies | Score: {score_str:>8} | Reason: {leaf.reason}")
        print(f"     Path: {path_str}")
        print(f"     FEN : {leaf.fen}")
        print("-" * 78)


def run_cycle(
    client: AtomicDBClient,
    explorer: MinimaxFrontierExplorer,
    root_fen: str,
    max_queue: int,
    budget: int,
    max_allowed_budget: int,
    dry_run: bool
) -> List[dict]:
    """
    Executes a single analysis and queuing pass.
    Discovered leaf nodes are logged live and queued immediately.
    Returns the list of submitted tasks for monitoring.
    """
    logger.info(f"Starting tree traversal from root FEN (strategy: {explorer.strategy})...")
    client.cache.clear()
    client.query_count = 0

    tracked_tasks: List[dict] = []
    queued_count = 0
    leaf_idx = 0
    already_analyzed_count = 0

    def handle_leaf(leaf: LeafCandidate) -> None:
        nonlocal queued_count, leaf_idx, already_analyzed_count
        if max_queue and max_queue > 0 and queued_count >= max_queue:
            return

        if explorer.skip_zero_cp and leaf.score is not None and leaf.score == 0:
            logger.info(f"Skipping 0 cp leaf ({leaf.fen}).")
            return

        leaf_idx += 1
        score_str = f"{leaf.score:+d} cp" if leaf.score is not None else "N/A"
        path_str = " ".join(leaf.path) if leaf.path else "(Root)"

        logger.info(
            f"[Leaf #{leaf_idx:02d} found] Depth: {leaf.depth:2d} plies | "
            f"Score: {score_str:>8} | Reason: {leaf.reason}"
        )
        if len(path_str) > 105:
            import textwrap
            wrapped = textwrap.wrap(path_str, width=105)
            print(f"     Path: {wrapped[0]}")
            for p_line in wrapped[1:]:
                print(f"           {p_line}")
        else:
            print(f"     Path: {path_str}")
        print(f"     FEN : {leaf.fen}")

        if dry_run:
            queued_count += 1
            print(f"     [DRY-RUN] Would queue: {leaf.fen}\n")
            return

        resp = client.request_analysis(leaf.fen, budget=budget)
        status = resp.get('status', 'unknown')
        reason = resp.get('reason', '')
        ahead = resp.get('ahead', '')
        task_id = resp.get('task')
        key = resp.get('key') or key_of(leaf.fen)
        budget_nodes = resp.get('budget_nodes')
        task_state = resp.get('task_state')

        # Response to only_if_missing: Position already analyzed with >= budget
        if status in ('already-analyzed', 'already-solved'):
            explorer.known_analyzed_fens.add(leaf.fen)
            already_analyzed_count += 1
            completed_budget = resp.get('completed_budget')
            b_label = f"{completed_budget // 1_000_000_000}B" if completed_budget and completed_budget >= 1_000_000_000 else f"{completed_budget // 1_000_000}M" if completed_budget else "?"
            print(f"     --> Status: {status} (already analyzed with {b_label} nodes)\n")
            return

        # Emergency safety net AFTER request:
        # If the server unexpectedly assigns more than the allowed budget (> 128M),
        # cancel the task immediately!
        if budget_nodes and budget_nodes > max_allowed_budget:
            b_str = f"{budget_nodes // 1_000_000_000}B" if budget_nodes >= 1_000_000_000 else f"{budget_nodes // 1_000_000}M"
            max_str = f"{max_allowed_budget // 1_000_000_000}B" if max_allowed_budget >= 1_000_000_000 else f"{max_allowed_budget // 1_000_000}M"
            if status == 'queued' and task_id:
                cancel_resp = client.cancel_task(task_id)
                cancel_st = cancel_resp.get('status', 'unknown')
                print(
                    f"     --> [CANCELLED] Server assigned {b_str} nodes (> {max_str}). "
                    f"Task #{task_id} cancelled immediately ({cancel_st})!\n"
                )
                return
            elif status == 'already-queued':
                print(
                    f"     --> [IGNORED] Task #{task_id} running with {b_str} nodes "
                    f"(> {max_str}). Not tracking.\n"
                )
                return

        extra = []
        if task_id:
            extra.append(f"Task #{task_id}")
        if task_state:
            extra.append(f"Status: {task_state}")
        if budget_nodes:
            b_label = f"{budget_nodes // 1_000_000_000}B" if budget_nodes >= 1_000_000_000 else f"{budget_nodes // 1_000_000}M"
            extra.append(b_label)
        if ahead != '':
            extra.append(f"Queue pos: {ahead}")
        if reason:
            r_clean = reason.replace("this address already has a live request on this position; wait for it to land", "live request active")
            if len(r_clean) > 30:
                r_clean = r_clean[:27] + "..."
            extra.append(f"Reason: {r_clean}")
        extra_str = f" ({', '.join(extra)})" if extra else ""

        print(f"     --> Status: {status:<15}{extra_str}\n")
        if status in ('queued', 'already-queued', 'already-requested', 'expanded'):
            queued_count += 1
            tracked_tasks.append({
                'fen': leaf.fen,
                'key': key,
                'task_id': task_id,
                'status': status,
                'path': leaf.path
            })

    leaves = explorer.explore(root_fen, on_leaf=handle_leaf)

    print("=" * 78)
    if not leaves:
        logger.info("No open leaf nodes found (tree is solved or saturated at frontier).")
    else:
        extra_info = [f"{queued_count} queued"]
        if already_analyzed_count > 0:
            extra_info.append(f"{already_analyzed_count} already analyzed")
        summary_str = ", ".join(extra_info)
        logger.info(
            f"Tree traversal complete: {len(leaves)} leaves found, "
            f"{summary_str} ({client.query_count} queries)."
        )
    print("=" * 78 + "\n")

    return tracked_tasks


def wait_for_completion(
    client: AtomicDBClient,
    tracked_tasks: List[dict],
    poll_interval: int = 5,
    max_wait: int = 600
) -> bool:
    """
    Dynamically waits for submitted tasks to finish analysis.
    Returns immediately once all tasks are completed in the tree with visits > 0.

    Efficiency & Rate-Limiting:
    - For logged-in user: Polls the central 'my-queue' endpoint directly (1 request per poll),
      instead of sending hundreds of individual queries for each position.
    - For external/anonymous tasks: Strictly throttled polling (max 4 queries/s, max 5 per interval),
      to never exceed the server rate limit (5 req/s).
    """
    if not tracked_tasks:
        return True

    start_time = time.time()
    active_tasks = {t['key']: t for t in tracked_tasks if t.get('key')}
    initial_count = len(active_tasks)

    logger.info(
        f"Waiting for {initial_count} task(s) "
        f"(polling every {poll_interval}s, max wait: {max_wait if max_wait > 0 else '∞'}s)..."
    )

    last_status_msg = ""
    while active_tasks:
        elapsed = int(time.time() - start_time)
        if max_wait > 0 and elapsed >= max_wait:
            logger.info(f"Max wait interval ({max_wait}s) reached. Starting next cycle...")
            return False

        # --- PATH 1: Logged-in user (my-queue) ---
        if client.username and client.password:
            user_queue = client.my_queue()

            # On network/connection error: DO NOT abort, wait and retry!
            if user_queue is None:
                time.sleep(poll_interval)
                continue

            if len(user_queue) > 0:
                leased = sum(1 for t in user_queue if t.get('state') == 'LEASED')
                pending = sum(1 for t in user_queue if t.get('state') == 'PENDING')
                count_str = f"{len(user_queue)}+" if len(user_queue) >= 200 else str(len(user_queue))
                first_tid = user_queue[0].get('task_id')
                extra = f" | Task #{first_tid}" if first_tid else ""

                msg = f"[Waiting] {count_str} in my-queue ({leased} searching, {pending} queued{extra}) · {elapsed}s"
                if len(msg) > 98:
                    msg = msg[:95] + "..."
                if msg != last_status_msg:
                    logger.info(msg)
                    last_status_msg = msg

                time.sleep(poll_interval)
                continue

            # my-queue was successfully queried and is empty ([]):
            # All account tasks have finished!
            active_tasks.clear()
            break

        # --- PATH 2: External or anonymous tasks (throttled!) ---
        # Check at most 5 tasks per poll step, with 0.25s pause (max 4 req/s)
        check_batch = list(active_tasks.items())[:5]
        finished_keys = []
        leased_count = 0

        for key, task in check_batch:
            fen = task.get('fen', '')
            if fen:
                data = client.query(fen, bypass_cache=True)
                if data and (data.get('visits', 0) > 0 or data.get('status', 'UNKNOWN') != 'UNKNOWN'):
                    finished_keys.append(key)
                else:
                    lr = client.live_request(key)
                    if lr and not lr.get('live', True):
                        finished_keys.append(key)
                    else:
                        leased_count += 1
            else:
                finished_keys.append(key)

        for k in finished_keys:
            if k in active_tasks:
                del active_tasks[k]

        if not active_tasks:
            break

        running_count = len(active_tasks)
        pending_count = max(0, running_count - leased_count)
        state_parts = []
        if leased_count > 0:
            state_parts.append(f"{leased_count} searching")
        if pending_count > 0:
            state_parts.append(f"{pending_count} queued")
        state_summary = ", ".join(state_parts) if state_parts else "waiting"

        msg = f"[Waiting] {running_count}/{initial_count} active ({state_summary}) · {elapsed}s"
        if len(msg) > 98:
            msg = msg[:95] + "..."
        if msg != last_status_msg:
            logger.info(msg)
            last_status_msg = msg

        time.sleep(poll_interval)

    logger.info(f"All submitted tasks completed (Duration: {int(time.time() - start_time)}s)!")
    return True


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="AtomicDB Minimax Queue Bot: Finds and queues critical leaf nodes."
    )
    parser.add_argument("--fen", default=DEFAULT_START_FEN, help="Root FEN (default: starting position)")
    parser.add_argument(
        "--moves",
        type=str,
        default=None,
        help="Initial move sequence (e.g. 'g1f3 f7f6' or '1. Nf3 f6') to apply to base FEN to set the root position."
    )
    parser.add_argument(
        "--strategy",
        choices=["margin", "exact", "and_or"],
        default="margin",
        help="Search strategy: 'margin' (default: all moves within margin-cp), "
             "'exact' (strict best values along principal variation), "
             "'and_or' (attacker=OR, defender=AND)"
    )
    parser.add_argument("--margin-cp", type=int, default=10, help="Centipawn tolerance for near-ties (default: 10)")
    parser.add_argument(
        "--defender-branching",
        type=int,
        default=3,
        help="Max defensive moves per AND node in and_or mode (default: 3)"
    )
    parser.add_argument("--max-depth", type=int, default=0, help="Max search depth in plies (0 = unlimited, default: 0)")
    parser.add_argument("--max-leaves", type=int, default=0, help="Max leaves per cycle (0 = all discovered, default: 0)")
    parser.add_argument("--max-queries", type=int, default=0, help="Max API queries per cycle (0 = unlimited, default: 0)")
    parser.add_argument(
        "--skip-zero-cp",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip 0 cp positions (draw / perpetual) and do not queue them as leaves (default: True)"
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        help="Minimum centipawn score for root player moves (default: auto: 1 cp if root advantage > 0, disabled if root <= 0). Filters out draws and losing moves."
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=None,
        choices=[128000000, 512000000, 2000000000, 10000000000],
        help="Node budget (default: 128000000. Larger budgets like 512000000 or 2000000000 only when explicitly specified)."
    )
    parser.add_argument("--username", default="", help="OpenBench username")
    parser.add_argument("--password", default="", help="OpenBench password")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="AtomicDB base URL")
    parser.add_argument("--dry-run", action="store_true", help="Simulate only, do not send real requests")
    parser.add_argument(
        "--mode",
        choices=["once", "loop"],
        default="once",
        help="Operating mode: 'once' (single pass) or 'loop' (reactive daemon)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Interval in seconds for polling task completion (default: 5s)"
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=600,
        help="Max wait time in seconds before re-checking (default: 600s, 0 = unlimited)"
    )
    parser.add_argument(
        "--idle-interval",
        type=int,
        default=30,
        help="Wait time in seconds if no tasks are active or during dry run (default: 30s)"
    )
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    args = parse_arguments()

    target_budget = args.budget if args.budget is not None else 128_000_000
    max_allowed_budget = args.budget if args.budget is not None else 128_000_000

    b_label = f"{target_budget // 1_000_000_000}B nodes" if target_budget >= 1_000_000_000 else f"{target_budget // 1_000_000}M nodes"
    if args.budget is None:
        b_label += " (default limit: max 128M)"
    else:
        b_label += " (explicitly specified)"

    effective_root_fen = resolve_root_fen(args.fen, args.moves)
    root_stm = effective_root_fen.split()[1] if len(effective_root_fen.split()) > 1 else 'w'
    stm_name = "White" if root_stm == 'w' else "Black"

    min_score_display = (
        f"{args.min_score} cp (explicit)" if args.min_score is not None
        else "Auto (>= 1 cp if winning, unrestricted if <= 0 cp)"
    )

    print("================================================================================")
    print("                     ATOMICDB MINIMAX QUEUE BOT                                 ")
    print("================================================================================")
    print(f" Server     : {args.server}")
    print(f" Strategy   : {args.strategy} (margin: {args.margin_cp} cp, defender branching: {args.defender_branching})")
    print(f" Root FEN   : {effective_root_fen} ({stm_name} to move)")
    if args.moves:
        print(f" Moves      : {args.moves}")
    depth_str = "Unlimited" if args.max_depth == 0 else f"{args.max_depth} plies"
    leaves_str = "All discovered" if args.max_leaves == 0 else str(args.max_leaves)
    print(f" Max Depth  : {depth_str} | Max Leaves: {leaves_str}")
    print(f" 0 cp Filter: {'Active (skip 0 cp / draw)' if args.skip_zero_cp else 'Inactive'} | Min Score: {min_score_display}")
    print(f" Budget     : {b_label}")
    print(f" Mode       : {args.mode.upper()}" + (f" (polling: every {args.poll_interval}s)" if args.mode == "loop" else ""))
    print(f" Dry Run    : {args.dry_run}")
    if args.username:
        print(f" Account    : {args.username}")
    print("================================================================================\n")

    client = AtomicDBClient(base_url=args.server, username=args.username, password=args.password)
    explorer = MinimaxFrontierExplorer(
        client=client,
        strategy=args.strategy,
        margin_cp=args.margin_cp,
        defender_branching=args.defender_branching,
        max_depth=args.max_depth,
        max_leaves=args.max_leaves,
        max_queries=args.max_queries,
        skip_zero_cp=args.skip_zero_cp,
        min_score=args.min_score
    )

    try:
        if args.mode == "once":
            run_cycle(
                client=client,
                explorer=explorer,
                root_fen=effective_root_fen,
                max_queue=args.max_leaves,
                budget=target_budget,
                max_allowed_budget=max_allowed_budget,
                dry_run=args.dry_run
            )
        else:
            logger.info("Starting reactive monitoring loop...")
            while True:
                try:
                    tracked_tasks = run_cycle(
                        client=client,
                        explorer=explorer,
                        root_fen=effective_root_fen,
                        max_queue=args.max_leaves,
                        budget=target_budget,
                        max_allowed_budget=max_allowed_budget,
                        dry_run=args.dry_run
                    )

                    if args.dry_run:
                        logger.info(f"Dry run: Pausing {args.idle_interval}s before next check...")
                        time.sleep(args.idle_interval)
                        continue

                    if not tracked_tasks:
                        if client.username and client.password:
                            user_tasks = client.my_queue()
                            if user_tasks:
                                logger.info(
                                    f"{len(user_tasks)} active task(s) found in 'my-queue'. "
                                    "Waiting for completion..."
                                )
                                wait_for_completion(
                                    client=client,
                                    tracked_tasks=[
                                        {'task_id': t.get('task_id'), 'key': t.get('position')}
                                        for t in user_tasks if isinstance(t, dict)
                                    ],
                                    poll_interval=args.poll_interval,
                                    max_wait=args.max_wait
                                )
                                continue

                        logger.info(f"No active tasks to monitor. Waiting {args.idle_interval}s before next check...")
                        time.sleep(args.idle_interval)
                        continue

                    # Dynamically and reactively wait for completion of tasks!
                    wait_for_completion(
                        client=client,
                        tracked_tasks=tracked_tasks,
                        poll_interval=args.poll_interval,
                        max_wait=args.max_wait
                    )
                    logger.info("Starting next minimax cycle immediately...")

                except KeyboardInterrupt:
                    logger.info("Terminated by user.")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in cycle: {e}", exc_info=True)
                    time.sleep(args.idle_interval)
    finally:
        client.close()


if __name__ == '__main__':
    main()
