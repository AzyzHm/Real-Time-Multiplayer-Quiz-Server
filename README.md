# Real-Time Multiplayer Quiz Server

A console quiz (a small "Kahoot") where several players answer the same questions at the
same time over the network. The server keeps the shared game state correct, fair and
responsive while all the players answer at once.

Written in Python with the standard library only (no framework, no dependency).
Written for Python 3.8 or later, tested with Python 3.12.

> **Course project**: *Systèmes distribués et parallèles*, ISAMM, Université de la Manouba,
> 2ème année ingénieur, 2026-2027. Professor: Dr. Mounira Ebdelli.
> Goal of the project: apply concurrent programming techniques in Python to solve a problem.
> This project uses **multithreading, multiprocessing, TCP sockets** and, in one small
> optional place, **asyncio**, exactly at the level taught in class.

---

## 1. The problem

> How can N players take part in the same quiz in real time, over a network, while keeping
> the shared game state correct, fair and responsive?

It breaks down into six concrete sub-problems:

| # | Sub-problem | Why it is a real problem | Tool that solves it |
|---|---|---|---|
| 1 | Serving many clients at once | An iterative TCP server serves one client at a time, so the quiz could not even start | A listener thread plus one worker `Thread` per client, over TCP sockets |
| 2 | Corrupted shared round data | Many workers read and modify the same round data (answers, scores, ranking). A compound operation such as "check, then update" is not atomic, so threads can interleave in the middle (same family of race as `cpt = cpt + 1`) | One `Lock`, always used with `with lock:` |
| 3 | Fairness of the "first correct answer" bonus | "Has somebody already taken the bonus? If not, take it" must be one atomic step, otherwise several players can all get the bonus | The same `Lock` protects this check-and-set |
| 4 | Synchronized start and end of each round | No question may start before every player is ready, and no result may be shown before everybody answered or timed out | Two `threading.Barrier` with an `action` and a `timeout` |
| 5 | Slow I/O must not slow the game | Writing logs and computing statistics on the game threads would delay the answers | A separate `multiprocessing.Process` fed through an `mp.Queue` |
| 6 | Countdown (optional) | Show the remaining time of the current question on the host console | `async def` and `await asyncio.sleep`, in one small daemon thread |

### Scope

**In scope**
- One game room on one server, 2 to 6 players (configurable).
- A pool of **50** multiple-choice questions in `questions.json`; each game draws **15** of
  them at random (random order, no repeat). Each question lasts **15 seconds**.
- Console clients only, TCP only.
- Scoring: points for a correct answer plus a bonus for the first correct answer of the round.
- Live ranking after each question, final ranking and statistics at the end.
- Logging and leaderboard handled by a separate process.
- A demonstration of the race condition (unsafe mode against safe mode) with bot players.

**Out of scope** (kept simple on purpose)
GUI or web interface, authentication, database, several simultaneous rooms, encryption, UDP,
third-party libraries, internet deployment.

---

## 2. Quick start

You need only Python. Open several terminals in this folder.

```bash
# Terminal 1: the server (type "start" on its console when the players have joined)
python server.py

# Terminals 2, 3, ...: one console client per player
python client.py                  # server on the same machine
python client.py 192.168.1.20     # server on another machine of the network
```

**Host console** (the server terminal):

| Command | Effect |
|---|---|
| `list` | show the players in the lobby |
| `start` | close the lobby and start the game (needs at least 2 players) |
| `quit` | stop the server cleanly (also Ctrl+C) |

**Player console**: enter a name, wait in the lobby, then type the letter of your answer
(`A`, `B`, `C` or `D`) when a question appears. Type `quit` to leave.

**Server options**

| Option | Meaning |
|---|---|
| `--port N` | TCP port (default 50000) |
| `--max-players N` | size of the lobby (default 6) |
| `--questions N` | questions drawn from the pool for the game (default 15) |
| `--autostart N` | start by itself as soon as N players have joined |
| `--countdown` | show a countdown for each question on the host console (asyncio) |
| `--unsafe` | **demo only**: remove the lock around the scoring (see section 8) |

The server writes `game.log` next to `server.py` (appended at each run).

**Bots, race demo and benchmark**

```bash
python server.py --autostart 12 --max-players 12 --unsafe   # terminal 1
python bots.py --players 12 --sync                          # terminal 2
python benchmark.py --players 200 --rounds 5                # cost of logging
```

**Tests**

```bash
python -m unittest          # every test file, about 30 seconds
python test_game_state.py   # or one file at a time
```

---

## 3. Architecture

```
          +---------------------------- Server process ------------------------------+
          |                                                                          |
Client 1 --TCP--> Worker thread 1 --+                                                |
Client 2 --TCP--> Worker thread 2 --+--> GameState (players, scores, round data)     |
Client N --TCP--> Worker thread N --+        protected by ONE Lock                   |
          |            ^                                                             |
          |  Listener thread (accept loop) creates the workers                       |
          |            |                                                             |
          |  Barriers: start_barrier, end_barrier (parties = number of players)      |
          |            |                                                             |
          |  Host console (main thread)  +  helper threads (see below)               |
          |            |                                                             |
          |            +--> events --> mp.Queue -----------------------+             |
          +--------------------------------------------------------------|-----------+
                                                                         v
                                              Logger / leaderboard process
                                              (game.log, live ranking, final statistics)
```

### Threads and process

| Component | Kind | Role |
|---|---|---|
| Host console | main thread | reads `list`, `start`, `quit` |
| Listener | thread | blocks on `accept()`, creates one worker per connection, goes back to `accept()` |
| Worker (one per player) | thread | the dialogue with one client: lobby, then the rounds, scoring under the lock |
| End watcher | daemon thread | announces the end of the game |
| Autostart | daemon thread, optional | waits on a `Condition` until N players have joined |
| READY timer | `threading.Timer` | removes players who never answer `READY` |
| Countdown | daemon thread, optional | the asyncio countdown on the host console |
| Logger / leaderboard | **separate process** | consumes events from the `mp.Queue`, writes `game.log`, prints the ranking and the statistics |

### Game flow

1. The server starts the logger process, binds and listens on port 50000.
2. Players connect and send `JOIN` with a name. The server answers `WELCOME` and broadcasts
   the lobby to everybody.
3. The host types `start` (or `--autostart` triggers it). The lobby closes, the two barriers
   are created with one party per player, and every client receives the closed lobby.
4. Each client answers `READY`. Its worker then plays the game. For each question:
   1. `start_barrier.wait()`: nobody starts before everybody is ready. The barrier `action`
      picks the question and resets the round data.
   2. The worker sends `QUESTION` to its client.
   3. It waits for the `ANSWER` with a socket timeout up to the shared deadline of the round.
      A missing answer counts as no answer.
   4. It scores the answer **inside the lock**. The lock is released **before** any send.
   5. It publishes an event for the logger process.
   6. `end_barrier.wait()`: everybody answered or timed out. The barrier `action` freezes the
      ranking under the lock.
   7. It sends `RESULT` and `SCORES` to its own client.
5. After the last question each worker sends `GAME_OVER`. The logger process prints the
   final statistics. The host types `quit` to stop the server.

### Scoring

| Case | Points |
|---|---|
| Correct answer | 100 |
| First correct answer of the round | +50 bonus |
| Wrong answer or no answer | 0 |

---

## 4. Concurrency design, tool by tool

| Tool | Where | Why this tool | Course source |
|---|---|---|---|
| TCP sockets | all client/server communication | reliable, ordered delivery | Chapter 4 |
| `threading.Thread` | listener plus one worker per client | concurrent server architecture, threads share the game state | Chapters 1 and 4 |
| `threading.Lock` with `with` | `GameState` | prevents the race on the shared round data and the double bonus | Chapter 1 |
| `threading.Barrier(parties, action, timeout)` | round start and round end | all players synchronized, `action` prepares the round, `timeout` avoids a permanent block | Chapter 1 |
| `threading.Condition` | lobby changes (`--autostart`) | wait without busy waiting, test in a `while`, built on the same lock | Chapter 1 |
| `multiprocessing.Process` plus `mp.Queue` | logger and leaderboard | isolation and IPC, producer/consumer, `None` as end signal | Chapter 2 |
| `if __name__ == '__main__':` | `server.py`, tests, benchmark | required when creating processes | Chapter 2 |
| `async def` / `await asyncio.sleep` | host countdown | minimal use of the async syntax seen in class | Chapter 3 |

### Honest justification points

- **Why threads for the clients?** The work is I/O-bound (waiting for the network). The GIL is
  released during socket waits, and the threads share the game state directly, which is
  exactly the shared-memory situation that needs a lock.
- **What does the lock really protect?** The score of a player is written only by its own
  worker, so there is no lost update on one score. The lock protects the **compound
  operations on shared round data**: "has anybody taken the bonus? if not, take it", the
  answers of the round, and consistent snapshots of the ranking. The `--unsafe` demo shows
  the double bonus.
- **Why a separate process for the logs?** The logging work is light, so the argument is
  **isolation and IPC**, not a raw speedup. With 200 simultaneous answers, the benchmark
  measured about 1.8 ms per answer for a game thread that writes the log file itself, against
  about 0.01 ms when it only hands the event to the logger process (numbers depend on the
  machine). Compared with no logging at all, the logger process is not faster.
- **Why is asyncio optional?** The game is correct without it. The countdown is a small,
  isolated use of `async` / `await`. It only prints and never touches the game state.
- **Is there only one lock?** There is **one lock for the game state**. A few other locks
  exist, each protecting one tiny thing and never held while taking another lock: the send
  lock of each socket (two threads must not interleave bytes on the same socket), the lock of
  the host display, and the lock of the client for the current question. The `Condition` is
  built on the game lock, so it adds no second lock.

### Design rules followed

1. **One lock** for the game state, so no nested locks and no deadlock between game locks.
2. **Never call `send` while holding the lock**: a slow client would freeze everybody. Update
   the state under the lock, release it, then send (snapshot pattern).
3. **Message framing**: TCP is a stream, so messages are one JSON object per line and the
   receiver keeps a buffer (`protocol.py`).
4. **Always send completely** (`sendall`) and use UTF-8 everywhere.
5. **`with lock:`** instead of manual `acquire` / `release`.
6. **Barrier robustness**: barriers need a fixed number of parties, so a disconnected player
   becomes a **ghost** (see section 6). Barriers have a `timeout` and `BrokenBarrierError` is
   caught as a safety net.
7. **Socket timeouts** during a round, so a silent player cannot block it.
8. **Clean shutdown**: stop flag for the listener, closing the sockets wakes the workers, all
   threads and the logger process are joined, `SO_REUSEADDR` on the listening socket.
9. **Daemon threads only for helpers** (end watcher, autostart, countdown). The console stays
   in the main thread, because a daemon thread blocked in `input()` crashes the interpreter
   at exit.

---

## 5. Network protocol

One JSON object per line over TCP. Every message has a `type`.

**Client to server**

| Message | Example | Meaning |
|---|---|---|
| `JOIN` | `{"type":"JOIN","name":"Sana"}` | enter the lobby |
| `READY` | `{"type":"READY"}` | sent by the client when it sees the lobby close |
| `ANSWER` | `{"type":"ANSWER","q":3,"choice":"B"}` | answer the current question |
| `QUIT` | `{"type":"QUIT"}` | leave |

**Server to client**

| Message | Example | Meaning |
|---|---|---|
| `WELCOME` | `{"type":"WELCOME","player_id":2}` | join accepted |
| `LOBBY` | `{"type":"LOBBY","version":4,"players":["Ali","Sana"],"open":true}` | current players; `version` lets a client ignore a stale list; `open` is false once the game starts |
| `QUESTION` | `{"type":"QUESTION","q":3,"number":1,"total":15,"text":"...","choices":{"A":"..","B":"..","C":"..","D":".."},"time_limit":15}` | new round (the right answer is never sent) |
| `RESULT` | `{"type":"RESULT","q":3,"correct":true,"points":150,"first":true,"answer":"B"}` | personal result |
| `SCORES` | `{"type":"SCORES","ranking":[["Sana",250],["Ali",100]]}` | ranking after the round |
| `GAME_OVER` | `{"type":"GAME_OVER","ranking":[["Sana",750],["Ali",600]]}` | end of the game |
| `ERROR` | `{"type":"ERROR","reason":"lobby closed"}` | rejected action |

Reasons of `ERROR`: `invalid name`, `name already taken`, `lobby full`, `lobby closed`,
`already joined`, `no game in progress`, `game not started`, `wrong question`,
`invalid choice`, `round in progress`, `unknown message type`, `game aborted`.

**Events sent to the logger process** (picklable dicts through the `mp.Queue`): `join`,
`leave`, `game_start`, `round_start`, `answer`, `timeout`, `disconnect`, `round_end`,
`game_over`. Example:
`{"type":"answer","player":"Sana","q":3,"correct":True,"points":150,"first":True,"t":4.2}`.
The end signal is `None`.

---

## 6. Robustness

| Situation | What happens |
|---|---|
| A player stays silent | the socket timeout ends its wait at the shared deadline, the answer counts as 0, and the round ends |
| A player disconnects during the game | its worker becomes a **ghost**: it keeps calling the barriers every round without any socket I/O, so nobody waits for it |
| A player disconnects after the lobby closed but before `READY` | same: the worker turns into a ghost and plays the barriers |
| A client stays connected but never answers `READY` | a timer removes it after 10 seconds (closing its socket turns its worker into a ghost) |
| A worker thread crashes (bug) | `abort_game()` breaks both barriers, every worker stops at once instead of waiting for the barrier timeout |
| Somebody never reaches a barrier | the barrier times out after the longest question time plus 20 seconds, `BrokenBarrierError` aborts the game for everybody |
| The logger process dies | publishing never blocks or raises, and `stop()` does not hang on the full pipe |
| Ctrl+C on the server | the server stops cleanly, the logger ignores `SIGINT` and is stopped with the end signal |
| Game over | each worker waits for the client to close first, so the last message is not lost by an early reset |

---

## 7. Logs and statistics

`game.log` holds one line per event, for example:

```
[14:03:22.123] answer     player=Sana q=3 correct=True points=150 first=True t=4.2
```

The logger process also prints the live ranking after each question and, at the end, the final
statistics: ranking, and per player the number of answers, correct answers, average and
fastest response time, first-answer bonuses, timeouts and disconnection, plus the hardest
question.

---

## 8. Demonstrations and tests

### The race condition demo (key moment of the presentation)

`--unsafe` removes the lock around the scoring and adds a 1 ms pause between the check and the
update of the "first correct answer" marker, to make the race visible reliably. `bots.py
--sync` makes all the bots answer correctly at the very same instant (they wait on a
`threading.Barrier`) and checks the results from the client side.

| Mode | Result with 12 synchronized bots, 10 questions |
|---|---|
| `--unsafe` | `RACE CONDITION DETECTED in 10 question(s)`: 12 players got the bonus on every question instead of 1 |
| default (safe) | `OK: exactly one first-answer bonus per question` |

### Other demonstrations

- **Normal game**: 2 to 6 console clients, a full game of 15 questions.
- **Disconnection**: close a client during a round, the others are not blocked (ghost worker).
- **Timeout**: one silent client, the round still ends at the time limit.
- **Stress**: `python server.py --autostart 30 --max-players 30` and
  `python bots.py --players 30 --accuracy 0.7`.
- **Benchmark**: `python benchmark.py` compares no logging, inline logging and the logger
  process.

### Test suite (`python -m unittest`)

| File | What it checks |
|---|---|
| `test_protocol.py` | framing: glued and split messages, accents, closed connection, bad messages, timeout keeping partial data, concurrent senders |
| `test_game_state.py` | lobby rules, scoring and bonus, 20 simultaneous answers (safe and unsafe), barriers and their actions, `Condition`, abort, events, random draw of questions, quality of `questions.json` |
| `test_server.py` | real server and real sockets: lobby, full games, silent player, disconnections, frozen client, crashing worker, autostart, countdown |
| `test_logger.py` | the logger process: order, end signal, statistics, dead logger, a fully logged game |
| `test_host_display.py` | the asyncio countdown |
| `test_bots.py` | the bonus check, the race demo as a test, a 30-bot stress test |

---

## 9. Files

```
server.py          entry point: sockets, listener thread, game setup, logger process, options
worker.py          ClientWorker(threading.Thread): the dialogue and the rounds of one player
game_state.py      GameState: lock, lobby, rounds, scoring; questions and random draw
protocol.py        MessageChannel: JSON-lines framing, send lock, broadcast, constants
logger_process.py  logger and leaderboard process, statistics, GameLogger (queue and process)
host_display.py    optional asyncio countdown on the host console
client.py          console client (receiver thread plus keyboard loop)
bots.py            bot players for stress tests and the race demo
benchmark.py       cost of logging for N simultaneous answers
questions.json     pool of 50 questions (15 drawn per game)
test_*.py          the test suite
```

### Adding questions

Append an object to `questions.json` with a new unique `id`:

```json
{"id": 51, "text": "Which module provides threads in Python?",
 "choices": {"A": "thread_pool", "B": "threading", "C": "tasks", "D": "parallel"},
 "answer": "B", "time_limit": 15}
```

`test_game_state.py` checks the file: four distinct choices A to D, an existing right answer,
unique ids and texts, 15 seconds per question, right answers spread over the four letters.

---

## 10. Limits and possible extensions

- One room, console only, no authentication, no persistence.
- A client that stops reading its socket could briefly delay the sends of a broadcast (only
  receive timeouts are implemented).
- `--unsafe` only disables the scoring guard, on purpose, for the demonstration.
- The countdown redraws a terminal line, so it looks right only on a real terminal.
- Ideas: several rooms, discovery of servers with UDP, a graphical client, an asynchronous
  server, a speed-based bonus.