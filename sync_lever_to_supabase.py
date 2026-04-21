"""
Sync Lever data to Supabase.
Run manually or via GitHub Actions cron.

Requires env vars:
  LEVER_API_KEY
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
"""

import os
import time
import urllib.parse
from datetime import datetime, timezone

import requests
import urllib3
from supabase import create_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ─────────────────────────────────────────────────────────────────────

LEVER_API_KEY   = os.environ["LEVER_API_KEY"]
SUPABASE_URL    = os.environ["SUPABASE_URL"]
SUPABASE_KEY    = os.environ["SUPABASE_SERVICE_KEY"]
BASE_URL        = "https://api.lever.co/v1"
LEVER_HIRE_URL  = "https://hire.lever.co/candidates/{}"
NOW_MS          = int(datetime.now(timezone.utc).timestamp() * 1000)

_COUNTRY_KEYWORDS = {
    "Brazil":    ["brazil", "brasil"],
    "Mexico":    ["mexico", "méxico"],
    "Colombia":  ["colombia"],
    "Argentina": ["argentina"],
    "USA":       ["usa", "united states"],
    "India":     ["india"],
    "Europe":    ["europe", "uk", "united kingdom"],
}

def _map_country(location: str) -> str:
    loc = (location or "").lower()
    for country, keywords in _COUNTRY_KEYWORDS.items():
        if any(kw in loc for kw in keywords):
            return country
    return "Other"

# ── Lever API ──────────────────────────────────────────────────────────────────

def _lever_session():
    s = requests.Session()
    s.headers["Connection"] = "close"
    s.verify = False
    if LEVER_API_KEY.startswith("eyJ"):
        s.headers["Authorization"] = f"Bearer {LEVER_API_KEY}"
    else:
        s.auth = (LEVER_API_KEY, "")
    return s

def _get(endpoint, params=None):
    results = []
    p = dict(params or {})
    p.setdefault("limit", 100)
    with _lever_session() as s:
        while True:
            for attempt in range(3):
                try:
                    resp = s.get(f"{BASE_URL}/{endpoint}", params=p, timeout=30)
                    if not resp.ok:
                        raise requests.HTTPError(
                            f"HTTP {resp.status_code} on {endpoint}: {resp.text[:300]}")
                    break
                except requests.exceptions.SSLError:
                    if attempt == 2:
                        raise
                    time.sleep(1)
            body = resp.json()
            results.extend(body.get("data", []))
            if not body.get("hasNext"):
                break
            p["offset"] = urllib.parse.unquote(body["next"])
    return results

def _get_single(endpoint):
    with _lever_session() as s:
        resp = s.get(f"{BASE_URL}/{endpoint}", timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", [])

# ── Fetch from Lever ───────────────────────────────────────────────────────────

def fetch_all():
    print("Fetching postings...")
    postings = _get("postings", {"state": "published"})
    posting_map = {}
    for p in postings:
        cat = p.get("categories") or {}
        loc = cat.get("location") or "Unknown"
        posting_map[p["id"]] = {
            "id":         p["id"],
            "title":      p.get("text", "Unknown"),
            "team":       cat.get("team") or "Unknown",
            "department": cat.get("department") or "Unknown",
            "location":   loc,
            "country":    _map_country(loc),
            "hm_id":      p.get("hiringManager") or "",
            "owner_id":   p.get("owner") or "",
        }
    print(f"  {len(posting_map)} postings")

    print("Fetching users...")
    users = _get("users")
    user_map = {u["id"]: u.get("name", "Unknown") for u in users}
    print(f"  {len(user_map)} users")

    print("Fetching stages...")
    stage_map = {s["id"]: s["text"] for s in _get_single("stages")}
    print(f"  {len(stage_map)} stages")

    print("Fetching active opportunities...")
    all_opps = _get("opportunities", {"archived": "false"})
    print(f"  {len(all_opps)} active opportunities")

    return posting_map, user_map, stage_map, all_opps

# ── Build rows ─────────────────────────────────────────────────────────────────

def build_candidate_rows(opps, posting_map, user_map, stage_map, archived=False):
    rows = []
    for opp in opps:
        # Resolve posting_id
        apps = opp.get("applications") or []
        pid = None
        if apps:
            first = apps[0]
            pid = first.get("posting") if isinstance(first, dict) else None
        if not pid:
            postings_field = opp.get("postings") or []
            pid = postings_field[0] if postings_field else None

        post = posting_map.get(pid, {})

        # Resolve stage
        stage_raw = opp.get("stage") or {}
        if isinstance(stage_raw, dict):
            stage_name = stage_raw.get("text") or stage_map.get(stage_raw.get("id"), "Unknown")
        else:
            stage_name = stage_map.get(stage_raw, "Unknown")

        changed_ms = opp.get("stageChangedAt") or opp.get("updatedAt") or NOW_MS
        days = max(0, round((NOW_MS - changed_ms) / 86_400_000))

        rows.append({
            "lever_id":       opp["id"],
            "candidate":      opp.get("name", "Unknown"),
            "profile_url":    LEVER_HIRE_URL.format(opp["id"]),
            "role":           post.get("title", "Unknown"),
            "team":           post.get("team", "Unknown"),
            "stage":          stage_name,
            "days_in_stage":  days,
            "recruiter":      user_map.get(opp.get("owner"), "Unknown"),
            "hiring_manager": user_map.get(post.get("hm_id"), "Unknown"),
            "director":       user_map.get(post.get("owner_id"), "Unknown"),
            "country":        post.get("country", "Other"),
            "archived":       archived,
            "synced_at":      datetime.now(timezone.utc).isoformat(),
        })
    return rows

# ── Write to Supabase ──────────────────────────────────────────────────────────

def sync_to_supabase(candidate_rows, posting_map):
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Upsert candidates (batch 500 at a time)
    print(f"Upserting {len(candidate_rows)} candidates...")
    batch_size = 500
    for i in range(0, len(candidate_rows), batch_size):
        batch = candidate_rows[i:i + batch_size]
        sb.table("candidates").upsert(batch, on_conflict="lever_id").execute()
        print(f"  {min(i + batch_size, len(candidate_rows))}/{len(candidate_rows)}")

    # Upsert postings
    posting_rows = list(posting_map.values())
    print(f"Upserting {len(posting_rows)} postings...")
    for i in range(0, len(posting_rows), batch_size):
        batch = posting_rows[i:i + batch_size]
        sb.table("postings").upsert(batch, on_conflict="id").execute()

    # Update sync log
    sb.table("sync_log").upsert({
        "id": 1,
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(candidate_rows),
    }, on_conflict="id").execute()

    print("Sync complete.")

# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    posting_map, user_map, stage_map, active_opps = fetch_all()
    rows = build_candidate_rows(active_opps, posting_map, user_map, stage_map, archived=False)
    sync_to_supabase(rows, posting_map)
