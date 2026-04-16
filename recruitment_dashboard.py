"""
Jeeves Recruitment Dashboard
Run: streamlit run recruitment_dashboard.py
Requires: LEVER_API_KEY environment variable
"""

import os
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import urllib3
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Secrets: support both Streamlit Cloud (st.secrets) and local env vars ────
def _get_lever_key():
    try:
        return st.secrets["LEVER_API_KEY"]
    except Exception:
        return os.environ.get("LEVER_API_KEY", "")


def _lever_session():
    """Session that closes connections after each use to avoid LibreSSL EOF errors."""
    s = requests.Session()
    s.headers["Connection"] = "close"
    s.verify = False
    key = _get_lever_key()
    # JWT tokens (start with 'eyJ') use Bearer auth; classic API keys use Basic auth
    if key.startswith("eyJ"):
        s.headers["Authorization"] = f"Bearer {key}"
    else:
        s.auth = (key, "")
    adapter = HTTPAdapter(max_retries=2)
    s.mount("https://", adapter)
    return s

# ── Config ───────────────────────────────────────────────────────────────────

LEVER_API_KEY = _get_lever_key()
BASE_URL = "https://api.lever.co/v1"
LEVER_HIRE_URL = "https://hire.lever.co/candidates/{}"
NOW_MS = int(datetime.now(timezone.utc).timestamp() * 1000)

STAGE_ORDER = [
    "New applicant", "New lead", "Reached out", "Responded",
    "Resume review", "Recruiter Interview", "Coding Exercise",
    "Hiring Manager Interview", "Panel Interview", "Additional Interview",
    "Case Study", "Final Interview", "Reference check",
    "Offer Approval", "Approval - Extend Offer", "Offer", "Offer Declined",
]

# ── API helpers ───────────────────────────────────────────────────────────────

def _get(endpoint, params=None):
    """Fetch all pages from a Lever endpoint.

    Lever's 'next' cursor is already URL-encoded, so we decode it before
    passing it to requests (which would otherwise double-encode it).
    Uses Connection: close per request to avoid LibreSSL EOF on keep-alive.
    """
    results = []
    p = dict(params or {})
    p.setdefault("limit", 100)
    while True:
        for attempt in range(3):
            try:
                with _lever_session() as s:
                    resp = s.get(f"{BASE_URL}/{endpoint}", params=p)
                if not resp.ok:
                    raise requests.exceptions.HTTPError(
                        f"HTTP {resp.status_code} on {endpoint}: {resp.text[:300]}",
                        response=resp,
                    )
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
        resp = s.get(f"{BASE_URL}/{endpoint}")
    resp.raise_for_status()
    return resp.json().get("data", [])


# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    # Open postings
    postings = _get("postings", {"state": "published"})
    posting_map = {}
    for p in postings:
        cat = p.get("categories") or {}
        posting_map[p["id"]] = {
            "title":      p.get("text", "Unknown"),
            "team":       cat.get("team") or "Unknown",
            "department": cat.get("department") or "Unknown",
            "location":   cat.get("location") or "Unknown",
            "hm_id":      p.get("hiringManager"),
        }

    # Users
    users = _get("users")
    user_map = {u["id"]: u.get("name", "Unknown") for u in users}

    # Stages
    stage_map = {s["id"]: s["text"] for s in _get_single("stages")}

    # Active candidates — fetch per posting (no expand, smaller payloads)
    # 3 threads avoids 429 while still being ~3x faster than sequential
    def _fetch_posting_opps(pid):
        opps = _get("opportunities", {"archived": "false", "posting_id": pid})
        for o in opps:
            o["_posting_id"] = pid
        return opps

    active = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(_fetch_posting_opps, pid): pid
                   for pid in posting_map}
        for fut in as_completed(futures):
            active.extend(fut.result())

    return postings, posting_map, user_map, stage_map, active


@st.cache_data(ttl=300, show_spinner=False)
def load_archived_for_posting(pid):
    """Fetch archived (inactive) candidates for a single posting on demand."""
    opps = _get("opportunities", {"archived": "true", "posting_id": pid})
    for o in opps:
        o["_posting_id"] = pid
    return opps


# ── Data transformations ──────────────────────────────────────────────────────

def build_pipeline_df(active, posting_map, user_map, stage_map):
    rows = []
    for opp in active:
        pid  = opp.get("_posting_id")
        post = posting_map.get(pid, {})

        # Stage: without expand, stage is a plain ID string
        stage_raw = opp.get("stage") or {}
        if isinstance(stage_raw, dict):
            stage_name = stage_raw.get("text") or stage_map.get(stage_raw.get("id"), "Unknown")
        else:
            stage_name = stage_map.get(stage_raw, "Unknown")

        changed_ms = opp.get("stageChangedAt") or opp.get("updatedAt") or NOW_MS
        days = max(0, round((NOW_MS - changed_ms) / 86_400_000))

        rows.append({
            "Candidate":      opp.get("name", "Unknown"),
            "Profile":        LEVER_HIRE_URL.format(opp["id"]),
            "Role":           post.get("title", "Unknown"),
            "Team":           post.get("team", "Unknown"),
            "Stage":          stage_name,
            "Days in Stage":  days,
            "Recruiter":      user_map.get(opp.get("owner"), "Unknown"),
            "Hiring Manager": user_map.get(post.get("hm_id"), "Unknown"),
            "Archived":       False,
        })

    cols = ["Candidate", "Profile", "Role", "Team", "Stage",
            "Days in Stage", "Recruiter", "Hiring Manager", "Archived"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


# ── App layout ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Jeeves Recruitment Dashboard",
    page_icon="🚀",
    layout="wide",
)

st.title("🚀 Jeeves Recruitment Dashboard")

if not LEVER_API_KEY:
    st.error("**LEVER_API_KEY** environment variable is not set.")
    st.stop()

# Sidebar
with st.sidebar:
    st.header("Controls")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Auto-refreshes every 5 minutes.")

with st.spinner("Loading data from Lever…"):
    postings, posting_map, user_map, stage_map, active = load_data()

pipeline_df = build_pipeline_df(active, posting_map, user_map, stage_map)

# Reverse maps: role title / HM name → list of posting IDs (for archived lookup)
_role_to_pids = defaultdict(list)
_hm_to_pids   = defaultdict(list)
for _pid, _pdata in posting_map.items():
    _role_to_pids[_pdata["title"]].append(_pid)
    _hm_name = user_map.get(_pdata.get("hm_id"), "Unknown")
    if _hm_name != "Unknown":
        _hm_to_pids[_hm_name].append(_pid)

st.caption(
    f"Last loaded: {datetime.now().strftime('%b %d, %Y %H:%M')} · "
    f"{len(active)} active candidates · {len(postings)} open reqs"
)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_pipeline, tab_kanban = st.tabs(["📊 Overview", "🔍 Candidate Pipeline", "📋 Kanban by Role"])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ════════════════════════════════════════════════════════════════════════════
with tab_overview:
    c1, c2 = st.columns(2)
    c1.metric("Open Reqs (Global)", len(postings))
    c2.metric("Active Candidates",  len(active))

    st.divider()
    st.subheader("Open Reqs by Team")

    team_data = defaultdict(int)
    for p in postings:
        t = (p.get("categories") or {}).get("team") or "Unknown"
        team_data[t] += 1

    team_df = pd.DataFrame([
        {"Team": t, "Open Reqs": c}
        for t, c in sorted(team_data.items(), key=lambda x: -x[1])
    ])
    st.dataframe(team_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Open Reqs by Department")

    dept_data = defaultdict(int)
    for p in postings:
        d = (p.get("categories") or {}).get("department") or "Unknown"
        dept_data[d] += 1

    dept_df = pd.DataFrame([
        {"Department": d, "Open Reqs": c}
        for d, c in sorted(dept_data.items(), key=lambda x: -x[1])
    ])
    st.dataframe(dept_df, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — CANDIDATE PIPELINE
# ════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    st.subheader("Candidate Pipeline")

    f1, f2, f3 = st.columns(3)
    teams_list      = ["All"] + sorted(pipeline_df["Team"].dropna().unique().tolist())
    recruiters_list = ["All"] + sorted(pipeline_df["Recruiter"].dropna().unique().tolist())
    hms_list        = ["All"] + sorted(pipeline_df["Hiring Manager"].dropna().unique().tolist())

    sel_team      = f1.selectbox("Filter by Team", teams_list)
    sel_recruiter = f2.selectbox("Filter by Recruiter", recruiters_list)
    sel_hm        = f3.selectbox("Filter by Hiring Manager", hms_list)

    fdf = pipeline_df.copy()
    if sel_team != "All":
        fdf = fdf[fdf["Team"] == sel_team]
    if sel_recruiter != "All":
        fdf = fdf[fdf["Recruiter"] == sel_recruiter]
    if sel_hm != "All":
        fdf = fdf[fdf["Hiring Manager"] == sel_hm]

    st.markdown(f"**{len(fdf)} candidates** matching filters")
    st.divider()

    if fdf.empty:
        st.info("No candidates match the selected filters.")
    else:
        present_stages = set(fdf["Stage"].unique())
        ordered_stages = [s for s in STAGE_ORDER if s in present_stages]
        ordered_stages += [s for s in present_stages if s not in STAGE_ORDER]

        for stage in ordered_stages:
            stage_df = fdf[fdf["Stage"] == stage].sort_values("Days in Stage", ascending=False)
            if stage_df.empty:
                continue

            stale = (stage_df["Days in Stage"] >= 15).sum()
            label = f"  🔴 {stale} stale" if stale else ""

            with st.expander(f"**{stage}** · {len(stage_df)} candidate(s){label}", expanded=True):
                st.dataframe(
                    stage_df[["Candidate", "Profile", "Role", "Team",
                               "Days in Stage", "Recruiter", "Hiring Manager"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Profile": st.column_config.LinkColumn(
                            "Profile",
                            display_text="Open in Lever ↗",
                        ),
                        "Days in Stage": st.column_config.NumberColumn(
                            "Days in Stage",
                            format="%d d",
                        ),
                    },
                )


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — KANBAN
# ════════════════════════════════════════════════════════════════════════════

_AVATAR_COLORS = ["#6366f1","#8b5cf6","#ec4899","#f43f5e",
                  "#f97316","#10b981","#0ea5e9","#14b8a6"]

def _initials(name):
    parts = str(name).strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return str(name)[:2].upper()

def _av_color(name):
    return _AVATAR_COLORS[hash(str(name)) % len(_AVATAR_COLORS)]

def _kanban_card(row):
    days        = int(row["Days in Stage"])
    is_archived = bool(row.get("Archived", False))

    if is_archived:
        bb, bt        = "#f3f4f6", "#9ca3af"
        card_bg       = "#fafafa"
        card_border   = "#d1d5db"
        name_color    = "#6b7280"
        extra_badge   = ("<span style='background:#e5e7eb;color:#6b7280;font-size:10px;"
                         "font-weight:500;padding:1px 6px;border-radius:99px;margin-left:4px;'>"
                         "Archived</span>")
        opacity_style = "opacity:0.75;"
    else:
        card_bg = "#fff"; card_border = "#e5e7eb"
        name_color = "#111827"; extra_badge = ""; opacity_style = ""
        if days >= 15:
            bb, bt = "#fee2e2", "#dc2626"
        elif days >= 7:
            bb, bt = "#fef3c7", "#d97706"
        else:
            bb, bt = "#dcfce7", "#16a34a"

    ini = _initials(row["Candidate"])
    avc = _av_color(row["Candidate"])
    role = row["Role"] if row["Role"] != "Unknown" else ""
    rec  = row["Recruiter"] if row["Recruiter"] not in ("Unknown", "") else ""
    sub  = role or rec
    return f"""
<div onclick="window.open('{row['Profile']}','_blank')"
     style="background:{card_bg};border:1px solid {card_border};border-radius:10px;
            padding:12px;margin-bottom:8px;cursor:pointer;{opacity_style}
            box-shadow:0 1px 2px rgba(0,0,0,.05);transition:box-shadow .15s;"
     onmouseover="this.style.boxShadow='0 4px 12px rgba(0,0,0,.12)'"
     onmouseout="this.style.boxShadow='0 1px 2px rgba(0,0,0,.05)'">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
    <div style="width:36px;height:36px;border-radius:50%;background:{avc};
                flex-shrink:0;display:flex;align-items:center;
                justify-content:center;font-weight:700;color:#fff;font-size:13px;">
      {ini}
    </div>
    <div style="overflow:hidden;min-width:0;">
      <div style="font-weight:600;color:{name_color};font-size:13px;
                  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
        {row['Candidate']}{extra_badge}
      </div>
      {"<div style='font-size:11px;color:#6b7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>" + sub + "</div>" if sub else ""}
    </div>
  </div>
  <span style="background:{bb};color:{bt};font-size:11px;font-weight:500;
               padding:2px 8px;border-radius:99px;">⏱ {days}d</span>
</div>"""

def _render_kanban(kdf):
    if kdf.empty:
        st.info("No candidates match the selected filters.")
        return
    present = set(kdf["Stage"].unique())
    stages  = [s for s in STAGE_ORDER if s in present]
    stages += [s for s in present if s not in STAGE_ORDER]

    st.caption(f"**{len(kdf)} candidates** · {len(stages)} stages")

    CARD_W = 240
    cols_html = ""
    for stage in stages:
        sdf   = kdf[kdf["Stage"] == stage].sort_values("Days in Stage", ascending=False)
        stale = int((sdf["Days in Stage"] >= 15).sum())
        stale_html = f"<span style='color:#dc2626;font-size:11px;margin-left:4px;'>🔴 {stale}</span>" if stale else ""
        cards = "".join(_kanban_card(r) for _, r in sdf.iterrows())
        cols_html += f"""
<div style="min-width:{CARD_W}px;flex-shrink:0;background:#f9fafb;
            border-radius:12px;padding:12px;">
  <div style="display:flex;align-items:center;gap:6px;margin-bottom:12px;">
    <span style="font-weight:700;color:#374151;font-size:13px;">{stage}</span>
    <span style="background:#e5e7eb;color:#6b7280;border-radius:99px;
                 padding:1px 7px;font-size:12px;">{len(sdf)}</span>
    {stale_html}
  </div>
  {cards}
</div>"""

    max_cards  = max((len(kdf[kdf["Stage"] == s]) for s in stages), default=1)
    est_height = min(900, 100 + max_cards * 90)
    components.html(
        f"""<!DOCTYPE html><html>
        <body style="margin:0;padding:4px;
                     font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
          <div style="display:flex;gap:12px;overflow-x:auto;
                      padding-bottom:8px;align-items:flex-start;">
            {cols_html}
          </div>
        </body></html>""",
        height=est_height,
        scrolling=True,
    )

_SOURCING_STAGES = {
    "New applicant", "New lead", "Reached out", "Responded", "Resume review",
}

with tab_kanban:
    st.subheader("Kanban Board")

    # ── View mode & filters ───────────────────────────────────────────────
    k_col1, k_col2, k_col3 = st.columns([1, 1, 1])
    with k_col1:
        view_by = st.radio("View by", ["Role / Vaga", "Hiring Manager"],
                           horizontal=True)
    with k_col2:
        show_sourcing = st.checkbox(
            "Show sourcing stages",
            value=False,
            help="Shows early stages: New applicant, New lead, Reached out, Responded, Resume review.",
        )
    with k_col3:
        show_archived = st.checkbox(
            "Include archived (inactive) candidates",
            value=False,
            help="Fetches archived candidates from Lever for the selected role/HM.",
        )

    if view_by == "Role / Vaga":
        roles_list = sorted(r for r in pipeline_df["Role"].dropna().unique() if r != "Unknown")
        sel_role   = st.selectbox("Select a Role", roles_list)
        kdf = pipeline_df[pipeline_df["Role"] == sel_role].copy()
        if show_archived:
            arch_opps = []
            for pid in _role_to_pids.get(sel_role, []):
                arch_opps.extend(load_archived_for_posting(pid))
            if arch_opps:
                arch_df = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                arch_df["Archived"] = True
                kdf = pd.concat([kdf, arch_df[arch_df["Role"] == sel_role]], ignore_index=True)
    else:
        hms_list = sorted(h for h in pipeline_df["Hiring Manager"].dropna().unique() if h != "Unknown")
        sel_hm   = st.selectbox("Select a Hiring Manager", hms_list)
        kdf = pipeline_df[pipeline_df["Hiring Manager"] == sel_hm].copy()
        if show_archived:
            arch_opps = []
            for pid in _hm_to_pids.get(sel_hm, []):
                arch_opps.extend(load_archived_for_posting(pid))
            if arch_opps:
                arch_df = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                arch_df["Archived"] = True
                kdf = pd.concat([kdf, arch_df[arch_df["Hiring Manager"] == sel_hm]], ignore_index=True)

    if not show_sourcing:
        kdf = kdf[~kdf["Stage"].isin(_SOURCING_STAGES)]

    _render_kanban(kdf)
