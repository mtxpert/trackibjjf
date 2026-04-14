"""Push all locally-cached final brackets to Supabase bracket_finals + fighter_results."""
import json, logging, sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("push")

sys.path.insert(0, str(Path(__file__).parent))

from watcher import STATE_DIR
from results import save_bracket_final

seed_dir = Path(__file__).parent / "seed_cache"
tourney_names = {}
t_seed = seed_dir / "tournaments.json"
if t_seed.exists():
    for t in json.loads(t_seed.read_text()):
        tourney_names[str(t["id"])] = t["name"]
try:
    from scraper_naga import _load_naga_cache
    for eid, ev in _load_naga_cache().items():
        tourney_names[str(eid)] = ev.get("name", f"NAGA {eid}")
except Exception:
    pass

done = skipped = errors = 0
files = sorted(STATE_DIR.glob("*.json"))
log.info("Checking %d bracket state files", len(files))

for f in files:
    try:
        state = json.loads(f.read_text())
        if not state.get("results_final"):
            skipped += 1
            continue
        cid        = state.get("category_id") or f.stem
        tid        = str(state.get("tournament_id", ""))
        source     = state.get("source", "ibjjf")
        name       = tourney_names.get(tid, f"Tournament {tid}")
        div        = state.get("division", "")
        ranking    = state.get("ranking", [])
        event_date = state.get("event_date", "")

        save_bracket_final(
            category_id=cid,
            tournament_id=tid,
            tournament_name=name,
            division=div,
            source=source,
            ranking=ranking,
            state=state,
            event_date=event_date,
        )
        done += 1
        if done % 200 == 0:
            log.info("  %d uploaded...", done)
    except Exception as e:
        log.warning("Failed %s: %s", f.stem, e)
        errors += 1

log.info("Done: %d uploaded, %d skipped (not final), %d errors", done, skipped, errors)
