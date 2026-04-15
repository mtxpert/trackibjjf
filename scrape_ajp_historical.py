"""
scrape_uaejjf_historical.py — Scrape all UAEJJF event results from events.uaejjf.org.

API (same pattern as Smoothcomp):
  GET  /en/event/{id}/results          → get CSRF token
  POST /en/event/{id}/results/getResults → JSON: {eventResults: [...]}

Each result: group.name (division), top3+after3 (placements with athlete+club+country)

Event IDs discovered via Playwright from /en/events/past (6 pages, 218 events).

Usage:
    python scrape_uaejjf_historical.py                        # full run
    python scrape_uaejjf_historical.py --dry-run              # no saves
    python scrape_uaejjf_historical.py --event-id 183         # single event test
    python scrape_uaejjf_historical.py --save-local out.json  # custom output
    python scrape_uaejjf_historical.py --search "Gordon Ryan" # search after
    python scrape_uaejjf_historical.py --resume               # skip already-saved
    python scrape_uaejjf_historical.py --upload               # upload to Supabase
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ajp_historical.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("uaejjf_hist")

BASE = "https://ajptour.com"
SOURCE = "ajp"
DEFAULT_OUT = "ajp_all_results.json"
DELAY = 1.2

# All event IDs discovered from /en/events/past (218 events, scraped 2026-04-15)
ALL_EVENT_IDS = [
    294,296,303,306,307,324,329,331,332,333,342,346,363,370,373,375,380,381,383,384,
    387,390,394,395,398,404,405,406,408,410,415,416,418,420,425,431,433,435,440,441,
    447,448,449,450,453,454,455,456,458,459,460,461,462,463,464,466,468,471,472,475,
    476,477,478,479,481,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,
    499,500,501,502,506,507,509,510,511,512,514,516,517,518,519,522,523,524,525,526,
    527,528,529,530,531,532,533,534,535,536,537,538,539,542,543,544,545,546,547,548,
    549,550,551,552,553,554,555,556,557,558,559,561,562,563,564,565,566,567,572,573,
    574,575,577,578,580,581,582,583,584,585,586,587,589,590,591,592,593,594,595,598,
    599,600,601,602,603,604,605,606,607,608,609,611,612,614,615,616,619,620,621,623,
    628,629,630,631,632,633,634,635,636,637,638,640,641,642,643,644,645,646,647,648,
    649,650,651,652,653,656,657,658,659,660,662,663,664,665,666,670,671,672,673,674,
    676,678,679,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,
    698,699,700,701,702,703,704,705,706,707,708,709,711,712,713,715,716,717,718,719,
    720,724,725,726,727,728,729,730,731,732,733,734,735,736,737,739,741,742,743,744,
    745,746,748,749,750,751,752,753,754,755,756,757,758,759,760,761,764,766,767,768,
    769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,790,791,793,
    794,795,796,797,798,799,800,801,802,803,804,805,807,811,812,813,815,816,817,818,
    819,820,821,822,823,825,826,827,828,831,832,833,834,835,836,840,841,844,845,846,
    847,849,850,851,852,853,854,855,856,858,859,860,861,862,863,864,865,866,867,868,
    869,870,872,873,874,877,878,879,880,881,882,883,885,886,888,889,890,891,892,893,
    894,895,898,900,901,902,903,904,905,906,907,908,910,911,914,915,916,917,918,919,
    920,921,923,925,927,928,929,931,933,934,935,936,937,938,939,940,944,945,946,947,
    948,949,950,951,953,954,955,956,958,960,962,964,965,968,969,970,971,972,973,974,
    975,976,977,978,983,984,985,986,987,988,989,990,991,992,994,995,996,997,998,999,
    1000,1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1017,1020,1021,1022,1023,
    1024,1025,1026,1027,1028,1029,1031,1034,1035,1036,1037,1038,1039,1040,1043,1044,1046,1047,1048,1049,
    1050,1052,1053,1054,1055,1056,1057,1058,1059,1061,1063,1064,1065,1067,1068,1069,1070,1073,1074,1075,
    1077,1082,1084,1085,1087,1088,1089,1090,1093,1095,1096,1097,1099,1100,1101,1104,1105,1106,1107,1108,
    1111,1112,1114,1117,1118,1119,1120,1121,1122,1123,1124,1125,1126,1127,1128,1130,1131,1133,1136,1139,
    1140,1141,1142,1143,1144,1145,1146,1147,1148,1149,1150,1151,1152,1153,1154,1155,1156,1157,1158,1159,
    1160,1162,1163,1164,1165,1166,1167,1168,1169,1170,1172,1173,1174,1175,1176,1177,1179,1180,1181,1183,
    1184,1185,1187,1188,1190,1192,1193,1194,1195,1196,1197,1198,1199,1200,1206,1208,1210,1211,1212,1213,
    1214,1215,1216,1217,1218,1219,1220,1221,1222,1223,1224,1225,1226,1227,1231,1233,1234,1235,1236,1237,
    1238,1239,1240,1242,1243,1244,1245,1247,1248,1251,1252,1253,1256,1257,1258,1259,1262,1263,1265,1267,
    1270,1271,1272,1273,1274,1275,1279,1281,1282,1283,1285,1286,1287,1289,1291,1292,1293,1294,1296,1297,
    1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1313,1316,1317,1318,1320,1321,1322,
    1323,1324,1326,1328,1329,1330,1331,1332,1333,1334,1338,1340,1341,1344,1345,1347,1348,1349,1353,1354,
    1355,1356,1359,1361,1362,1363,1364,1365,1366,1367,1368,1369,1370,1371,1372,1373,1374,1376,1377,1378,
    1379,1381,1382,1383,1384,1386,1388,1393,1395,1397,1398,1399,1400,1410,1414,1415,1418,1419,1420,1421,
    1422,1423,1427,1428,1434,1441,1446,1457,1458,1460,1461,1469,1475,1480,1486,1503,1512,
]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    return s


SESSION = make_session()


def _get_csrf(event_id: int) -> tuple[str, str]:
    """Fetch event results page. Returns (csrf_token, page_title)."""
    r = SESSION.get(f"{BASE}/en/event/{event_id}/results", timeout=20)
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', r.text)
    csrf = m.group(1) if m else ""
    title_m = re.search(r'"title"\s*:\s*"([^"]+)"', r.text)
    title = title_m.group(1) if title_m else ""
    # Also try <title> tag
    if not title:
        tm = re.search(r'<title>(.*?)</title>', r.text)
        if tm:
            title = tm.group(1).replace(" - UAE Jiu Jitsu Federation", "").strip()
            # Strip "Results - " prefix
            title = re.sub(r'^Results\s*-\s*', '', title).strip()
    return csrf, title


def fetch_event_results(event_id: int) -> list[dict]:
    """POST getResults for one event. Returns list of result rows."""
    try:
        # Fetch results page — get CSRF, title, and event_start date in one shot
        page_r = SESSION.get(f"{BASE}/en/event/{event_id}/results", timeout=20)
        csrf_m = re.search(r'<meta name="csrf-token" content="([^"]+)"', page_r.text)
        if not csrf_m:
            log.warning("Event %d: no CSRF token", event_id)
            return []
        csrf = csrf_m.group(1)

        # Title from window.sc JSON blob
        title_m = re.search(r'"title"\s*:\s*"([^"]+)"', page_r.text)
        event_title = title_m.group(1) if title_m else ""
        if not event_title:
            tm = re.search(r'<title>(.*?)</title>', page_r.text)
            if tm:
                event_title = re.sub(r'^Results\s*[-–]\s*', '', tm.group(1)).replace(" - UAE Jiu Jitsu Federation", "").strip()

        # Date from window.sc
        date_m = re.search(r'"event_start"\s*:\s*"(\d{4}-\d{2}-\d{2})', page_r.text)
        event_date = date_m.group(1) if date_m else ""

        r = SESSION.post(
            f"{BASE}/en/event/{event_id}/results/getResults",
            data={"_token": csrf},
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-TOKEN": csrf,
                "Referer": f"{BASE}/en/event/{event_id}/results",
            },
            timeout=20,
        )
        if r.status_code != 200:
            log.warning("Event %d: HTTP %d", event_id, r.status_code)
            return []

        data = r.json()
        categories = data.get("eventResults", [])

        if not categories:
            log.info("Event %d (%s): no results published", event_id, event_title[:50])
            return []

        rows = []
        for cat in categories:
            division = (cat.get("group") or {}).get("name", "")
            if not division:
                continue

            placements = (cat.get("top3") or []) + (cat.get("after3") or [])
            for p in placements:
                place = p.get("placement")
                target = p.get("target") or {}
                club = p.get("club") or {}
                affiliation = p.get("affiliation") or {}

                name = (target.get("fullname") or "").strip()
                if not name:
                    name = f"{target.get('firstname','')} {target.get('lastname','')}".strip()
                if not name:
                    continue

                rows.append({
                    "source": SOURCE,
                    "event_id": event_id,
                    "event_title": event_title,
                    "event_date": event_date,
                    "division": division,
                    "placement": place,
                    "athlete_name": name,
                    "athlete_id": target.get("user_id"),
                    "club": club.get("name", ""),
                    "affiliation": affiliation.get("name", ""),
                    "country": target.get("country_human", ""),
                    "country_code": target.get("country", ""),
                })

        log.info("Event %d (%s): %d rows, %d divisions",
                 event_id, event_title[:45], len(rows), len(categories))
        return rows

    except Exception as e:
        log.error("Event %d: unexpected error: %s", event_id, e)
        import traceback; traceback.print_exc()
        return []


# ── Upload ─────────────────────────────────────────────────────────────────────

def upload_to_supabase(rows: list[dict]) -> None:
    supabase_url = os.environ.get("SUPABASE_URL", "")
    service_key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not service_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY not set")
        return
    try:
        from supabase import create_client
        client = create_client(supabase_url, service_key)
    except Exception as e:
        log.error("Supabase client init failed: %s", e)
        return
    BATCH = 500
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            client.table("tournament_results").upsert(
                batch, on_conflict="source,event_id,division,placement,athlete_name"
            ).execute()
            total += len(batch)
            log.info("Uploaded %d / %d rows", total, len(rows))
        except Exception as e:
            log.error("Upload batch %d failed: %s", i // BATCH, e)
    log.info("Upload complete: %d rows", total)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Scrape UAEJJF results from events.uaejjf.org")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--event-id", type=int, default=None)
    ap.add_argument("--save-local", default=DEFAULT_OUT)
    ap.add_argument("--search", default=None)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--delay", type=float, default=DELAY)
    args = ap.parse_args()

    out_path = Path(args.save_local)

    # Load existing for resume
    existing_rows: list[dict] = []
    existing_ids: set[int] = set()
    if args.resume and out_path.exists():
        try:
            existing_rows = json.loads(out_path.read_text(encoding="utf-8"))
            existing_ids = {r["event_id"] for r in existing_rows}
            log.info("Resume: %d existing rows, %d event IDs", len(existing_rows), len(existing_ids))
        except Exception as e:
            log.warning("Could not load existing: %s", e)

    # Determine which events to scrape
    if args.event_id:
        event_ids = [args.event_id]
    else:
        event_ids = ALL_EVENT_IDS

    all_rows = list(existing_rows)
    total_events = len(event_ids)

    for i, eid in enumerate(event_ids, 1):
        if eid in existing_ids:
            log.info("[%d/%d] Skipping (resume): event %d", i, total_events, eid)
            continue

        log.info("[%d/%d] Scraping event %d", i, total_events, eid)
        rows = fetch_event_results(eid)
        all_rows.extend(rows)

        if not args.dry_run and rows:
            out_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        time.sleep(args.delay)

    log.info("Done. Total rows: %d", len(all_rows))

    if args.search:
        q = args.search.lower()
        matches = [r for r in all_rows if q in r.get("athlete_name", "").lower()]
        print(f"\nSearch '{args.search}': {len(matches)} matches")
        for r in matches:
            print(f"  [{r['placement']}] {r['athlete_name']} | {r['division']} | {r['event_title']} ({r['event_date']})")

    if args.upload and not args.dry_run:
        upload_to_supabase(all_rows)


if __name__ == "__main__":
    main()
