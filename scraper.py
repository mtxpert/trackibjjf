"""
IBJJF Tournament Scraper
Uses playwright (headless Chromium) to render JS and extract live bracket data.
Fetches categories list with plain requests; renders bracket pages with playwright.
"""

import re
import time
import asyncio
import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

ROSTER_DIR = Path(__file__).parent / "bracket_states"
ROSTER_DIR.mkdir(exist_ok=True)

BASE = "https://www.bjjcompsystem.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
CONCURRENCY = 8   # parallel browser pages


# ── Plain-requests helpers (no JS needed) ─────────────────────────────────────

def get_tournaments():
    resp = requests.get(f"{BASE}/tournaments", headers=HEADERS, timeout=12)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    result = []
    # Each tournament is in a div like <div class="tournament-display" id="tournament-display-{id}">
    for block in soup.find_all("div", id=re.compile(r"^tournament-display-\d+$")):
        tid = re.search(r"tournament-display-(\d+)$", block["id"]).group(1)
        # Name is in the img alt attribute
        img = block.find("img", alt=True)
        name = img["alt"] if img else f"Tournament {tid}"
        result.append({"id": tid, "name": name})

    return result


def get_category_ids(tournament_id):
    resp = requests.get(f"{BASE}/tournaments/{tournament_id}/categories",
                        headers=HEADERS, timeout=12)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    seen, cats = set(), []
    pat = re.compile(rf"/tournaments/{tournament_id}/categories/(\d+)")
    for a in soup.find_all("a", href=pat):
        cid = pat.search(a["href"]).group(1)
        name = a.get_text(strip=True)
        if cid not in seen:
            seen.add(cid)
            cats.append({"id": cid, "name": name})
    return cats


# ── Text parser (works on page.inner_text() output) ───────────────────────────

def parse_rendered_text(text, school_lower, division_name=""):
    """
    Parse the inner text of a rendered bjjcompsystem category page.

    Expected pattern per fight block:
        FIGHT N: Mat M
        Day MM/DD at HH:MM AM/PM
        <seed>
        ATHLETE NAME
        Team Name
        <seed>
        ATHLETE NAME
        Team Name

    Also uses the Ranking table at the bottom to collect all athletes.
    Returns list of dicts: name, team, division, mat, fight_num, fight_time
    """
    if school_lower not in text.lower():
        return []

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # ── Determine division name from page heading ──
    division = division_name
    for i, ln in enumerate(lines[:15]):
        upper = ln.upper()
        if any(b in upper for b in ["WHITE", "BLUE", "PURPLE", "BROWN", "BLACK"]):
            # Combine surrounding lines for full division name
            division = " ".join(lines[max(0,i-2):i+3])
            break

    # ── Parse ranking table for complete athlete list ──
    # Located near "Ranking" heading. Format:
    #   <position>
    #   Athlete Name
    #   \tTeam\t...
    ranking_athletes = {}  # name_lower -> {name, team}
    try:
        ranking_idx = next(i for i, l in enumerate(lines) if l.lower() == "ranking")
        # Skip header row ("Nº — COMPETITOR ...")
        i = ranking_idx + 2
        while i < len(lines) - 2:
            pos_line = lines[i]
            if not re.match(r"^\d+$", pos_line):
                break
            name_line = lines[i + 1] if i + 1 < len(lines) else ""
            team_line = lines[i + 2] if i + 2 < len(lines) else ""
            # team_line may contain tabs: "Team\t0\t3"
            team = team_line.split("\t")[0].strip()
            if name_line and re.match(r"^[A-Za-zÀ-ÿ '\-]+$", name_line):
                ranking_athletes[name_line.lower()] = {"name": name_line, "team": team}
            i += 3
    except StopIteration:
        pass

    # ── Parse fight blocks ──
    found = {}  # name_lower -> best athlete record

    fight_header_re = re.compile(
        r"^FIGHT\s+(\d+)\s*:\s*Mat\s+(\d+)$", re.IGNORECASE
    )
    time_re = re.compile(
        r"^(\w{3}\s+\d{2}/\d{2}\s+at\s+\d{1,2}:\d{2}\s+[AP]M)$", re.IGNORECASE
    )

    i = 0
    while i < len(lines):
        m = fight_header_re.match(lines[i])
        if not m:
            i += 1
            continue

        fight_num = m.group(1)
        mat       = m.group(2)
        fight_time = ""

        # Next non-empty line should be the time
        j = i + 1
        if j < len(lines) and time_re.match(lines[j]):
            fight_time = lines[j]
            j += 1

        # Collect athletes from this fight block (up to next FIGHT or empty block)
        block_lines = []
        while j < len(lines) and not fight_header_re.match(lines[j]):
            block_lines.append(lines[j])
            j += 1

        # Within the block, find pairs: seed / NAME / Team
        # Names are uppercase words; seeds are small integers
        k = 0
        while k < len(block_lines):
            seed_line = block_lines[k]
            if not re.match(r"^\d{1,2}$", seed_line):
                k += 1
                continue
            name_line = block_lines[k + 1] if k + 1 < len(block_lines) else ""
            team_line = block_lines[k + 2] if k + 2 < len(block_lines) else ""

            # Name is ALL-CAPS words; skip "BYE"
            if (name_line
                    and name_line.upper() == name_line
                    and name_line != "BYE"
                    and re.match(r"^[A-Z][A-Z '\-]+$", name_line)):

                name_title = name_line.title()  # "BRANDON LAVIN" -> "Brandon Lavin"
                key = name_title.lower()
                team = team_line if team_line and not re.match(r"^\d{1,2}$", team_line) else ""

                if school_lower in (team.lower() + " " + name_title.lower()):
                    if key not in found:
                        found[key] = {
                            "name": name_title,
                            "team": team,
                            "division": division,
                            "mat": mat,
                            "fight_num": fight_num,
                            "fight_time": fight_time,
                        }
                    else:
                        # Update with earliest fight time
                        existing = found[key]
                        if not existing["fight_time"] and fight_time:
                            existing["fight_time"] = fight_time
                            existing["mat"] = mat
                            existing["fight_num"] = fight_num
            k += 1

        i = j  # advance past this fight block

    # ── Also add athletes found in ranking that weren't in fights ──
    for key, ra in ranking_athletes.items():
        if school_lower in ra["team"].lower():
            if key not in found:
                found[key] = {
                    "name": ra["name"],
                    "team": ra["team"],
                    "division": division,
                    "mat": "",
                    "fight_num": "",
                    "fight_time": "",
                }

    return list(found.values())


# ── Async playwright scraper ──────────────────────────────────────────────────

async def _scrape_category(context, url, cat_name, school_lower):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=20000)
        text = await page.inner_text("body")
        return parse_rendered_text(text, school_lower, cat_name)
    except Exception:
        return []
    finally:
        await page.close()


async def _run_search(tournament_id, school_name, job):
    from playwright.async_api import async_playwright

    school_lower = school_name.lower().strip()
    cats = get_category_ids(tournament_id)
    job["total"] = len(cats)
    found = {}  # name_lower -> best record

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()

        sem = asyncio.Semaphore(CONCURRENCY)

        async def process(cat):
            async with sem:
                url = f"{BASE}/tournaments/{tournament_id}/categories/{cat['id']}"
                results = await _scrape_category(context, url, cat["name"], school_lower)
                for a in results:
                    a['category_id'] = cat['id']
                return results

        tasks = [asyncio.create_task(process(cat)) for cat in cats]

        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            job["progress"] = i + 1
            job["current_cat"] = cats[min(i, len(cats)-1)]["name"]

            for a in result:
                key = a["name"].lower()
                if key not in found:
                    found[key] = a
                else:
                    existing = found[key]
                    if a["mat"] and not existing["mat"]:
                        existing["mat"] = a["mat"]
                    if a["fight_time"] and not existing["fight_time"]:
                        existing["fight_time"] = a["fight_time"]
                    if a["fight_num"] and not existing["fight_num"]:
                        existing["fight_num"] = a["fight_num"]

        await browser.close()

    job["athletes"] = list(found.values())
    job["status"] = "done"


def run_search(tournament_id, school_name, job):
    """Sync wrapper — call from threading.Thread."""
    try:
        asyncio.run(_run_search(tournament_id, school_name, job))
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)


# ── Roster cache (full tournament, all athletes, for client-side search) ───────

def parse_all_athletes(text, division_name, category_id):
    """
    Extract ALL athletes from a rendered bracket page.
    Prefers the Ranking table (complete list + teams);
    falls back to / enriches from fight blocks (adds mat/time).
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    division = division_name
    for ln in lines[:15]:
        upper = ln.upper()
        if any(b in upper for b in ["WHITE","BLUE","PURPLE","BROWN","BLACK","NO-GI","NOGI"]):
            division = ln
            break

    athletes = {}  # name_lower -> record

    # ── Ranking table ──────────────────────────────────────────────────────────
    try:
        ranking_idx = next(i for i, l in enumerate(lines) if l.lower() == "ranking")
        i = ranking_idx + 2
        while i < len(lines) - 2:
            if not re.match(r"^\d+$", lines[i]):
                break
            name_line = lines[i + 1] if i + 1 < len(lines) else ""
            team_line = lines[i + 2] if i + 2 < len(lines) else ""
            team = team_line.split("\t")[0].strip()
            if name_line and re.match(r"^[A-Za-zÀ-ÿ '\-\.]+$", name_line):
                athletes[name_line.lower()] = {
                    "name": name_line,
                    "team": team,
                    "division": division,
                    "category_id": category_id,
                    "mat": "",
                    "fight_num": "",
                    "fight_time": "",
                }
            i += 3
    except StopIteration:
        pass

    # ── Fight blocks — enrich with mat/time; catch athletes missing from ranking ─
    fight_re = re.compile(r"^FIGHT\s+(\d+)\s*:\s*Mat\s+(\d+)$", re.IGNORECASE)
    time_re  = re.compile(r"^(\w{3}\s+\d{2}/\d{2}\s+at\s+\d{1,2}:\d{2}\s+[AP]M)$", re.IGNORECASE)
    seed_re  = re.compile(r"^\d{1,2}$")
    i = 0
    while i < len(lines):
        m = fight_re.match(lines[i])
        if not m:
            i += 1
            continue
        fight_num, mat = m.group(1), m.group(2)
        fight_time = ""
        j = i + 1
        if j < len(lines) and time_re.match(lines[j]):
            fight_time = lines[j]; j += 1
        while j < len(lines) and not fight_re.match(lines[j]):
            ln = lines[j]
            if seed_re.match(ln) and j + 1 < len(lines):
                name_ln = lines[j + 1]
                team_ln = lines[j + 2] if j + 2 < len(lines) else ""
                if name_ln and name_ln.upper() == name_ln and name_ln not in ("BYE",):
                    title = name_ln.title()
                    key   = title.lower()
                    team  = team_ln if (team_ln and not seed_re.match(team_ln)) else ""
                    if key not in athletes:
                        athletes[key] = {
                            "name": title, "team": team,
                            "division": division, "category_id": category_id,
                            "mat": mat, "fight_num": fight_num, "fight_time": fight_time,
                        }
                    elif not athletes[key]["mat"]:
                        athletes[key].update({"mat": mat, "fight_num": fight_num, "fight_time": fight_time})
                    j += 3 if (team_ln and not seed_re.match(team_ln)) else 2
                    continue
            j += 1
        i = j

    return list(athletes.values())


def load_roster_cache(tournament_id):
    path = ROSTER_DIR / f"{tournament_id}_roster.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def save_roster_cache(tournament_id, data):
    path = ROSTER_DIR / f"{tournament_id}_roster.json"
    path.write_text(json.dumps(data))


def filter_roster(cache, school_name):
    """Filter cached roster by school name — runs entirely in Python, no Playwright."""
    sl = school_name.lower().strip()
    seen = {}
    for a in cache.get("athletes", []):
        if sl in a.get("team", "").lower() or sl in a.get("name", "").lower():
            key = a["name"].lower()
            if key not in seen:
                seen[key] = dict(a)
    return list(seen.values())


async def _build_roster(tournament_id, job):
    from playwright.async_api import async_playwright

    cats = get_category_ids(tournament_id)
    job["total"] = len(cats)
    all_athletes = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        sem = asyncio.Semaphore(CONCURRENCY)

        async def process(cat):
            async with sem:
                page = await context.new_page()
                try:
                    url = f"{BASE}/tournaments/{tournament_id}/categories/{cat['id']}"
                    await page.goto(url, wait_until="networkidle", timeout=20000)
                    text = await page.inner_text("body")
                    return parse_all_athletes(text, cat["name"], cat["id"])
                except Exception:
                    return []
                finally:
                    await page.close()

        tasks = [asyncio.create_task(process(cat)) for cat in cats]
        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            all_athletes.extend(result)
            job["progress"] = i + 1
            job["current_cat"] = cats[min(i, len(cats) - 1)]["name"]

        await browser.close()

    # Dedupe by (name, category_id)
    seen, deduped = set(), []
    for a in all_athletes:
        key = (a["name"].lower(), a.get("category_id", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(a)

    cache = {
        "tournament_id": tournament_id,
        "built_at": datetime.now().isoformat(),
        "total_cats": len(cats),
        "athletes": deduped,
    }
    save_roster_cache(tournament_id, cache)
    job["status"] = "done"
    job["athlete_count"] = len(deduped)


def build_roster(tournament_id, job):
    """Sync wrapper — call from threading.Thread."""
    try:
        asyncio.run(_build_roster(tournament_id, job))
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
