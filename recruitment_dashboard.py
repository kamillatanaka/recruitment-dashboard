"""
Jeeves Recruitment Dashboard
Run: streamlit run recruitment_dashboard.py
Requires: LEVER_API_KEY secret (Streamlit Cloud) or environment variable
"""

import base64
import os
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import urllib3
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Secrets ───────────────────────────────────────────────────────────────────

def _get_lever_key():
    try:
        return st.secrets["LEVER_API_KEY"]
    except Exception:
        return os.environ.get("LEVER_API_KEY", "")

# ── Config ────────────────────────────────────────────────────────────────────

LEVER_API_KEY = _get_lever_key()

def _lever_session():
    s = requests.Session()
    s.headers["Connection"] = "close"
    s.verify = False
    if LEVER_API_KEY.startswith("eyJ"):
        s.headers["Authorization"] = f"Bearer {LEVER_API_KEY}"
    else:
        s.auth = (LEVER_API_KEY, "")
    adapter = HTTPAdapter(max_retries=2)
    s.mount("https://", adapter)
    return s

BASE_URL       = "https://api.lever.co/v1"
LEVER_HIRE_URL = "https://hire.lever.co/candidates/{}"
NOW_MS         = int(datetime.now(timezone.utc).timestamp() * 1000)

STAGE_ORDER = [
    "New applicant", "New lead", "Reached out", "Responded",
    "Resume review", "Recruiter Interview", "Coding Exercise",
    "Hiring Manager Interview", "Panel Interview", "Additional Interview",
    "Case Study", "Final Interview", "Reference check",
    "Offer Approval", "Approval - Extend Offer", "Offer", "Offer Declined",
]

INTERVIEW_STAGES = {
    "Recruiter Interview", "Coding Exercise", "Hiring Manager Interview",
    "Panel Interview", "Additional Interview", "Case Study", "Final Interview",
}
OFFER_STAGES = {"Reference check", "Offer Approval", "Approval - Extend Offer", "Offer"}
SOURCING_STAGES = {"New applicant", "New lead", "Reached out", "Responded", "Resume review"}

FUNNEL_GROUPS = [
    ("📥 Applied",      {"New applicant", "New lead", "Reached out", "Responded", "Resume review"}),
    ("🔍 Screening",    {"Recruiter Interview", "Coding Exercise"}),
    ("🎯 Interviewing", {"Hiring Manager Interview", "Panel Interview",
                         "Additional Interview", "Case Study", "Final Interview"}),
    ("📋 Offer",        {"Reference check", "Offer Approval",
                         "Approval - Extend Offer", "Offer", "Offer Declined"}),
]

# ── Brand colors ──────────────────────────────────────────────────────────────
GOLD   = "#C9A45A"
TEAL   = "#0097A7"
BLACK  = "#212121"
WHITE  = "#FFFFFF"
BORDER = "#E8D9B5"

# ── Country mapping ───────────────────────────────────────────────────────────

_COUNTRY_KEYWORDS = {
    "🇧🇷 Brazil":    ["brazil", "brasil"],
    "🇲🇽 Mexico":    ["mexico", "méxico"],
    "🇨🇴 Colombia":  ["colombia"],
    "🇦🇷 Argentina": ["argentina"],
    "🇺🇸 USA":       ["usa", "united states"],
    "🇮🇳 India":     ["india"],
    "🌍 Europe":     ["europe", "uk", "united kingdom"],
}

def _map_country(location: str) -> str:
    loc = (location or "").lower()
    for country, keywords in _COUNTRY_KEYWORDS.items():
        if any(kw in loc for kw in keywords):
            return country
    return "🌐 Other"

# ── API helpers ───────────────────────────────────────────────────────────────

def _get(endpoint, params=None):
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
    postings = _get("postings", {"state": "published"})
    posting_map = {}
    for p in postings:
        cat = p.get("categories") or {}
        loc = cat.get("location") or "Unknown"
        posting_map[p["id"]] = {
            "title":      p.get("text", "Unknown"),
            "team":       cat.get("team") or "Unknown",
            "department": cat.get("department") or "Unknown",
            "location":   loc,
            "country":    _map_country(loc),
            "hm_id":      p.get("hiringManager"),
            "owner_id":   p.get("owner"),
        }

    users     = _get("users")
    user_map  = {u["id"]: u.get("name", "Unknown") for u in users}
    stage_map = {s["id"]: s["text"] for s in _get_single("stages")}

    # Fetch ALL active opportunities in a single paginated call
    all_opps = _get("opportunities", {"archived": "false"})
    active = []
    for o in all_opps:
        # Extract posting_id from first application (Lever returns app objects or IDs)
        apps = o.get("applications") or []
        pid = None
        if apps:
            first = apps[0]
            pid = first.get("posting") if isinstance(first, dict) else None
        if not pid:
            # Some Lever configs expose posting directly on the opportunity
            postings_field = o.get("postings") or []
            pid = postings_field[0] if postings_field else None
        o["_posting_id"] = pid
        active.append(o)

    return postings, posting_map, user_map, stage_map, active

@st.cache_data(ttl=300, show_spinner=False)
def load_archived_for_posting(pid):
    try:
        opps = _get("opportunities", {"archived": "true", "posting_id": pid})
    except Exception:
        opps = []
    for o in opps:
        o["_posting_id"] = pid
    return opps

# ── Data transformations ──────────────────────────────────────────────────────

def build_pipeline_df(active, posting_map, user_map, stage_map):
    rows = []
    for opp in active:
        pid  = opp.get("_posting_id")
        post = posting_map.get(pid, {})

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
            "Director":       user_map.get(post.get("owner_id"), "Unknown"),
            "Country":        post.get("country", "🌐 Other"),
            "Archived":       False,
        })

    cols = ["Candidate", "Profile", "Role", "Team", "Stage",
            "Days in Stage", "Recruiter", "Hiring Manager", "Director", "Country", "Archived"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

# ── UI helpers ────────────────────────────────────────────────────────────────

def _load_logo_b64():
    logo_path = Path(__file__).parent / "jeeves_logo.png"
    if logo_path.exists():
        return base64.b64encode(logo_path.read_bytes()).decode()
    return None

def _section_label(text):
    st.markdown(
        f'<div style="font-size:11px;font-weight:700;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{GOLD};margin:20px 0 10px;">{text}</div>',
        unsafe_allow_html=True,
    )

def _kpi_card(label, value, subtitle="", color=GOLD):
    sub_html = (f'<div style="font-size:12px;color:#888;margin-top:4px;">{subtitle}</div>'
                if subtitle else "")
    return (
        f'<div style="background:{WHITE};border:1.5px solid {BORDER};border-radius:14px;'
        f'padding:20px 24px;min-height:108px;">'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:0.1em;'
        f'text-transform:uppercase;color:#999;">{label}</div>'
        f'<div style="font-size:34px;font-weight:800;color:{color};line-height:1;margin-top:8px;">{value}</div>'
        f'{sub_html}</div>'
    )

def _bottleneck(df):
    """Stage with most candidates stuck ≥7 days."""
    if df.empty:
        return "—", 0, 0
    stale = df[df["Days in Stage"] >= 7]
    if stale.empty:
        return "—", 0, 0
    grp = stale.groupby("Stage").agg(count=("Candidate", "count"),
                                      avg=("Days in Stage", "mean"))
    worst = grp["count"].idxmax()
    return worst, int(grp.loc[worst, "count"]), round(grp.loc[worst, "avg"])

# ── Kanban helpers ────────────────────────────────────────────────────────────

_AVATAR_COLORS = [GOLD, TEAL, "#8b5cf6", "#ec4899",
                  "#f43f5e", "#f97316", "#10b981", "#14b8a6"]

def _initials(name):
    parts = str(name).strip().split()
    return (parts[0][0] + parts[-1][0]).upper() if len(parts) >= 2 else str(name)[:2].upper()

def _av_color(name):
    return _AVATAR_COLORS[hash(str(name)) % len(_AVATAR_COLORS)]

def _kanban_card(row):
    days        = int(row["Days in Stage"])
    is_archived = bool(row.get("Archived", False))

    if is_archived:
        card_bg = "#fafafa"; card_border = "#d1d5db"
        name_color = "#6b7280"; opacity = "opacity:0.75;"
        day_bg, day_fg = "#e5e7eb", "#6b7280"
        extra_badge = ("<span style='background:#e5e7eb;color:#6b7280;font-size:10px;"
                       "font-weight:600;padding:1px 6px;border-radius:99px;margin-left:4px;'>"
                       "Archived</span>")
    else:
        card_bg = WHITE; card_border = BORDER
        name_color = BLACK; opacity = ""; extra_badge = ""
        if days >= 15:   day_bg, day_fg = "#fee2e2", "#dc2626"
        elif days >= 7:  day_bg, day_fg = "#fef3c7", "#d97706"
        else:            day_bg, day_fg = "#dcfce7", "#16a34a"

    ini  = _initials(row["Candidate"])
    avc  = _av_color(row["Candidate"])
    role = row["Role"] if row["Role"] != "Unknown" else ""
    rec  = row["Recruiter"] if row["Recruiter"] not in ("Unknown", "") else ""
    sub  = role or rec

    return (
        f'<div onclick="window.open(\'{row["Profile"]}\',\'_blank\')" '
        f'style="background:{card_bg};border:1px solid {card_border};border-radius:10px;'
        f'padding:12px;margin-bottom:8px;cursor:pointer;{opacity}'
        f'box-shadow:0 1px 3px rgba(0,0,0,.06);transition:box-shadow .15s;" '
        f'onmouseover="this.style.boxShadow=\'0 4px 16px rgba(201,164,90,.2)\'" '
        f'onmouseout="this.style.boxShadow=\'0 1px 3px rgba(0,0,0,.06)\'">'
        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">'
        f'<div style="width:34px;height:34px;border-radius:50%;background:{avc};flex-shrink:0;'
        f'display:flex;align-items:center;justify-content:center;'
        f'font-weight:700;color:{WHITE};font-size:12px;">{ini}</div>'
        f'<div style="overflow:hidden;min-width:0;">'
        f'<div style="font-weight:700;color:{name_color};font-size:13px;'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
        f'{row["Candidate"]}{extra_badge}</div>'
        + (f'<div style="font-size:11px;color:#888;white-space:nowrap;'
           f'overflow:hidden;text-overflow:ellipsis;">{sub}</div>' if sub else "")
        + f'</div></div>'
        f'<span style="background:{day_bg};color:{day_fg};font-size:11px;'
        f'font-weight:600;padding:2px 8px;border-radius:99px;">⏱ {days}d</span>'
        f'</div>'
    )

def _render_kanban(kdf):
    if kdf.empty:
        st.info("No candidates match the selected filters.")
        return
    present = set(kdf["Stage"].unique())
    stages  = [s for s in STAGE_ORDER if s in present]
    stages += [s for s in present if s not in STAGE_ORDER]

    st.caption(f"**{len(kdf)} candidates** · {len(stages)} stages")

    CARD_W    = 240
    cols_html = ""
    for stage in stages:
        sdf   = kdf[kdf["Stage"] == stage].sort_values("Days in Stage", ascending=False)
        stale = int((sdf["Days in Stage"] >= 15).sum())
        stale_html = (f"<span style='color:#dc2626;font-size:11px;margin-left:4px;'>"
                      f"🔴 {stale}</span>" if stale else "")
        cards = "".join(_kanban_card(r) for _, r in sdf.iterrows())
        cols_html += (
            f'<div style="min-width:{CARD_W}px;flex-shrink:0;background:#FAFAF8;'
            f'border:1px solid {BORDER};border-radius:12px;padding:12px;">'
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:12px;">'
            f'<span style="font-weight:700;color:{BLACK};font-size:13px;">{stage}</span>'
            f'<span style="background:{BORDER};color:#888;border-radius:99px;'
            f'padding:1px 7px;font-size:12px;">{len(sdf)}</span>'
            f'{stale_html}</div>{cards}</div>'
        )

    max_cards  = max((len(kdf[kdf["Stage"] == s]) for s in stages), default=1)
    est_height = min(900, 100 + max_cards * 90)
    components.html(
        f'<!DOCTYPE html><html><body style="margin:0;padding:4px;'
        f'font-family:\'Urbanist\',-apple-system,BlinkMacSystemFont,sans-serif;">'
        f'<div style="display:flex;gap:12px;overflow-x:auto;'
        f'padding-bottom:8px;align-items:flex-start;">{cols_html}</div>'
        f'</body></html>',
        height=est_height,
        scrolling=True,
    )

# ═════════════════════════════════════════════════════════════════════════════
# APP
# ═════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Jeeves Recruitment Dashboard",
    page_icon="🦁",
    layout="wide",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Urbanist:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Urbanist', sans-serif !important;
    background-color: #FFFFFF !important;
}
section[data-testid="stSidebar"] { display: none !important; }
header[data-testid="stHeader"]   { display: none !important; }
.block-container { padding-top: 1.5rem !important; }

.stTabs [data-baseweb="tab-list"] { border-bottom: 2px solid #E8D9B5; gap: 4px; }
.stTabs [data-baseweb="tab"] {
    font-family: 'Urbanist', sans-serif;
    font-weight: 600;
    color: #888;
    padding: 8px 20px;
}
.stTabs [aria-selected="true"] {
    color: #C9A45A !important;
    border-bottom: 2px solid #C9A45A !important;
    background: transparent !important;
}
.streamlit-expanderHeader {
    font-family: 'Urbanist', sans-serif !important;
    font-weight: 700 !important;
    color: #212121 !important;
}
hr { border-color: #E8D9B5 !important; }
</style>
""", unsafe_allow_html=True)

# ── Auth ──────────────────────────────────────────────────────────────────────
if not LEVER_API_KEY:
    st.error("⚠️ **LEVER_API_KEY** is not configured.")
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("🦁 Loading data from Lever…"):
    try:
        postings, posting_map, user_map, stage_map, active = load_data()
    except Exception as e:
        st.error(f"**API Error:** {e}")
        st.stop()

pipeline_df = build_pipeline_df(active, posting_map, user_map, stage_map)

_role_to_pids     = defaultdict(list)
_hm_to_pids       = defaultdict(list)
_director_to_pids = defaultdict(list)
for _pid, _pdata in posting_map.items():
    _role_to_pids[_pdata["title"]].append(_pid)
    _hm_name = user_map.get(_pdata.get("hm_id"), "Unknown")
    if _hm_name != "Unknown":
        _hm_to_pids[_hm_name].append(_pid)
    _dir_name = user_map.get(_pdata.get("owner_id"), "Unknown")
    if _dir_name != "Unknown":
        _director_to_pids[_dir_name].append(_pid)

roles_all     = sorted(r for r in pipeline_df["Role"].dropna().unique() if r != "Unknown")
hms_all       = sorted(h for h in pipeline_df["Hiring Manager"].dropna().unique() if h != "Unknown")
recs_all      = sorted(r for r in pipeline_df["Recruiter"].dropna().unique() if r != "Unknown")
directors_all = sorted(d for d in pipeline_df["Director"].dropna().unique() if d != "Unknown")

# ── Header ────────────────────────────────────────────────────────────────────
logo_b64  = _load_logo_b64()
logo_html = (f'<img src="data:image/png;base64,{logo_b64}" width="44" '
             f'style="border-radius:50%;">' if logo_b64 else '<span style="font-size:32px;">🦁</span>')

hcol1, hcol2 = st.columns([7, 1])
with hcol1:
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:14px;padding:4px 0;">'
        f'{logo_html}'
        f'<div>'
        f'<div style="font-size:10px;font-weight:700;letter-spacing:0.18em;'
        f'text-transform:uppercase;color:{GOLD};">Jeeves</div>'
        f'<div style="font-size:22px;font-weight:800;color:{BLACK};line-height:1.1;">'
        f'Recruitment Dashboard</div>'
        f'</div></div>'
        f'<div style="border-bottom:2px solid {GOLD};margin:10px 0 2px;"></div>',
        unsafe_allow_html=True,
    )
with hcol2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.caption(
    f"🕐 Last updated: {datetime.now().strftime('%b %d, %Y · %H:%M')} &nbsp;·&nbsp; "
    f"**{len(active)}** active candidates &nbsp;·&nbsp; **{len(postings)}** open reqs"
)

# ── View Selector ─────────────────────────────────────────────────────────────
_section_label("📊 View")
vc1, vc2 = st.columns([2, 3])
with vc1:
    view_mode = st.radio(
        "View",
        ["🌐 All Positions", "💼 By Role", "👤 By Hiring Manager",
         "👩‍💼 By Recruiter", "🎖️ By Director"],
        horizontal=True,
        label_visibility="collapsed",
    )

sel_role, sel_hm, sel_rec, sel_director = None, None, None, None
with vc2:
    if view_mode == "💼 By Role":
        sel_role = st.selectbox("Role", roles_all, label_visibility="collapsed")
    elif view_mode == "👤 By Hiring Manager":
        sel_hm = st.selectbox("Hiring Manager", hms_all, label_visibility="collapsed")
    elif view_mode == "👩‍💼 By Recruiter":
        sel_rec = st.selectbox("Recruiter", recs_all, label_visibility="collapsed")
    elif view_mode == "🎖️ By Director":
        sel_director = st.selectbox("Director", directors_all, label_visibility="collapsed")

# ── Country filter ─────────────────────────────────────────────────────────────
_all_countries = list(_COUNTRY_KEYWORDS.keys()) + ["🌐 Other"]
sel_countries = st.multiselect(
    "🌍 Filter by Country",
    options=_all_countries,
    default=[],
    placeholder="All countries",
    label_visibility="collapsed",
)

# Apply country filter to pipeline_df before view-specific filtering
_base_df = (pipeline_df[pipeline_df["Country"].isin(sel_countries)].copy()
            if sel_countries else pipeline_df.copy())

# ── Filter ────────────────────────────────────────────────────────────────────
_country_postings = (
    [p for p in postings if posting_map.get(p["id"], {}).get("country") in sel_countries]
    if sel_countries else postings
)

if view_mode == "🌐 All Positions":
    vdf        = _base_df.copy()
    v_postings = _country_postings
elif view_mode == "💼 By Role" and sel_role:
    vdf        = _base_df[_base_df["Role"] == sel_role].copy()
    v_postings = [p for p in _country_postings
                  if posting_map.get(p["id"], {}).get("title") == sel_role]
elif view_mode == "👤 By Hiring Manager" and sel_hm:
    vdf        = _base_df[_base_df["Hiring Manager"] == sel_hm].copy()
    v_postings = [p for p in _country_postings
                  if user_map.get(posting_map.get(p["id"], {}).get("hm_id")) == sel_hm]
elif view_mode == "👩‍💼 By Recruiter" and sel_rec:
    vdf        = _base_df[_base_df["Recruiter"] == sel_rec].copy()
    v_postings = _country_postings
elif view_mode == "🎖️ By Director" and sel_director:
    vdf        = _base_df[_base_df["Director"] == sel_director].copy()
    v_postings = [p for p in _country_postings
                  if user_map.get(posting_map.get(p["id"], {}).get("owner_id")) == sel_director]
else:
    vdf        = _base_df.copy()
    v_postings = _country_postings

# ── KPI Cards ─────────────────────────────────────────────────────────────────
_section_label("📈 Key Metrics")

in_interview = int((vdf["Stage"].isin(INTERVIEW_STAGES)).sum())
in_offer     = int((vdf["Stage"].isin(OFFER_STAGES)).sum())
stale_count  = int((vdf["Days in Stage"] >= 15).sum())
b_stage, b_count, b_days = _bottleneck(vdf)

if view_mode == "🌐 All Positions":
    cols = st.columns(5)
    cards = [
        _kpi_card("📂 Open Positions",    str(len(v_postings))),
        _kpi_card("👥 Active Candidates", str(len(vdf))),
        _kpi_card("🎯 In Interviews",     str(in_interview)),
        _kpi_card("📋 At Offer Stage",    str(in_offer)),
        _kpi_card("🔴 Stale (>15d)",      str(stale_count), color="#dc2626"),
    ]
else:
    cols = st.columns(4)
    b_sub = f"{b_count} candidates · avg {b_days}d" if b_stage != "—" else "All clear ✅"
    cards = [
        _kpi_card("👥 Active Candidates", str(len(vdf))),
        _kpi_card("🎯 In Interviews",     str(in_interview)),
        _kpi_card("🔴 Stale (>15d)",      str(stale_count), color="#dc2626"),
        _kpi_card("🔥 Biggest Bottleneck", b_stage, subtitle=b_sub, color=TEAL),
    ]

for col, html in zip(cols, cards):
    col.markdown(html, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Charts ────────────────────────────────────────────────────────────────────
ch1, ch2 = st.columns(2)

with ch1:
    _section_label("📊 Recruitment Funnel")
    funnel_rows = [{"Stage": name, "Candidates": int((vdf["Stage"].isin(stages)).sum())}
                   for name, stages in FUNNEL_GROUPS]
    funnel_chart = pd.DataFrame(funnel_rows)
    if funnel_chart["Candidates"].sum() > 0:
        st.bar_chart(funnel_chart.set_index("Stage"), color=GOLD, height=220)
    else:
        st.info("No candidates in current selection.")

with ch2:
    if view_mode == "🌐 All Positions":
        _section_label("🏢 Pipeline by Team")
        team_counts = (vdf.groupby("Team").size().reset_index(name="Candidates")
                       .pipe(lambda d: d[d["Team"] != "Unknown"])
                       .sort_values("Candidates", ascending=False))
        if not team_counts.empty:
            st.bar_chart(team_counts.set_index("Team"), color=TEAL, height=220)
        else:
            st.info("No team data available.")
    else:
        _section_label("📅 Avg Days by Stage")
        if not vdf.empty:
            stage_avg = (vdf.groupby("Stage")["Days in Stage"]
                         .mean().reset_index(name="Avg Days")
                         .assign(order=lambda d: d["Stage"].apply(
                             lambda s: STAGE_ORDER.index(s) if s in STAGE_ORDER else 999))
                         .sort_values("order").drop("order", axis=1)
                         .pipe(lambda d: d[~d["Stage"].isin(SOURCING_STAGES)]))
            if not stage_avg.empty:
                st.bar_chart(stage_avg.set_index("Stage"), color=TEAL, height=220)
        else:
            st.info("No candidates in current selection.")

st.markdown(f'<div style="border-bottom:1px solid {BORDER};margin:24px 0 8px;"></div>',
            unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_pipeline, tab_kanban = st.tabs(["🔍 Candidate Pipeline", "🗂️ Kanban"])

# ── Pipeline Tab ──────────────────────────────────────────────────────────────
with tab_pipeline:
    _section_label("👥 Candidates")

    if view_mode == "🌐 All Positions":
        f1, f2, f3 = st.columns(3)
        sel_team_p = f1.selectbox(
            "Team", ["All Teams"] + sorted(vdf["Team"].dropna().unique().tolist()),
            label_visibility="collapsed")
        sel_rec_p = f2.selectbox(
            "Recruiter", ["All Recruiters"] + sorted(vdf["Recruiter"].dropna().unique().tolist()),
            label_visibility="collapsed")
        sel_hm_p = f3.selectbox(
            "Hiring Manager", ["All HMs"] + sorted(vdf["Hiring Manager"].dropna().unique().tolist()),
            label_visibility="collapsed")
        fdf = vdf.copy()
        if sel_team_p != "All Teams":       fdf = fdf[fdf["Team"] == sel_team_p]
        if sel_rec_p != "All Recruiters":   fdf = fdf[fdf["Recruiter"] == sel_rec_p]
        if sel_hm_p != "All HMs":           fdf = fdf[fdf["Hiring Manager"] == sel_hm_p]
    else:
        fdf = vdf.copy()

    st.markdown(
        f'<div style="color:#888;font-size:13px;margin-bottom:12px;">'
        f'<b>{len(fdf)}</b> candidates</div>',
        unsafe_allow_html=True,
    )

    # Lazy load
    if "pipeline_loaded" not in st.session_state:
        st.session_state.pipeline_loaded = False

    if not st.session_state.pipeline_loaded:
        if st.button("📂 Load Candidate Pipeline", type="primary"):
            st.session_state.pipeline_loaded = True
            st.rerun()
        st.caption("Click to expand the full candidate list by stage.")
    else:
        if fdf.empty:
            st.info("No candidates match the selected filters.")
        else:
            present_stages = set(fdf["Stage"].unique())
            ordered_stages = [s for s in STAGE_ORDER if s in present_stages]
            ordered_stages += [s for s in present_stages if s not in STAGE_ORDER]

            for stage in ordered_stages:
                sdf = fdf[fdf["Stage"] == stage].sort_values("Days in Stage", ascending=False)
                if sdf.empty:
                    continue
                stale = int((sdf["Days in Stage"] >= 15).sum())
                badge = f"  🔴 {stale} stale" if stale else ""
                with st.expander(f"**{stage}** · {len(sdf)} candidate(s){badge}", expanded=True):
                    st.dataframe(
                        sdf[["Candidate", "Profile", "Role", "Team",
                             "Days in Stage", "Recruiter", "Hiring Manager", "Director", "Country"]],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Profile": st.column_config.LinkColumn(
                                "Profile", display_text="Open in Lever ↗"),
                            "Days in Stage": st.column_config.NumberColumn(
                                "Days in Stage", format="%d d"),
                        },
                    )

# ── Kanban Tab ────────────────────────────────────────────────────────────────
with tab_kanban:
    _section_label("🗂️ Kanban Board")

    ctrl1, ctrl2 = st.columns(2)
    show_sourcing = ctrl1.checkbox(
        "Show sourcing stages", value=False,
        help="Includes: New applicant, New lead, Reached out, Responded, Resume review")
    show_archived = ctrl2.checkbox("Include archived candidates", value=False)

    if view_mode == "💼 By Role" and sel_role:
        kdf = vdf.copy()
        if show_archived:
            arch_opps = []
            for pid in _role_to_pids.get(sel_role, []):
                arch_opps.extend(load_archived_for_posting(pid))
            if arch_opps:
                adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                adf["Archived"] = True
                kdf = pd.concat([kdf, adf[adf["Role"] == sel_role]], ignore_index=True)

    elif view_mode == "👤 By Hiring Manager" and sel_hm:
        kdf = vdf.copy()
        if show_archived:
            arch_opps = []
            for pid in _hm_to_pids.get(sel_hm, []):
                arch_opps.extend(load_archived_for_posting(pid))
            if arch_opps:
                adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                adf["Archived"] = True
                kdf = pd.concat([kdf, adf[adf["Hiring Manager"] == sel_hm]], ignore_index=True)

    elif view_mode == "👩‍💼 By Recruiter" and sel_rec:
        kdf = vdf.copy()

    elif view_mode == "🎖️ By Director" and sel_director:
        kdf = vdf.copy()
        if show_archived:
            arch_opps = []
            for pid in _director_to_pids.get(sel_director, []):
                arch_opps.extend(load_archived_for_posting(pid))
            if arch_opps:
                adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                adf["Archived"] = True
                kdf = pd.concat([kdf, adf[adf["Director"] == sel_director]], ignore_index=True)

    else:  # All Positions — needs sub-selector for kanban
        k_view = st.radio(
            "Kanban view",
            ["💼 By Role", "👤 By Hiring Manager", "👩‍💼 By Recruiter", "🎖️ By Director"],
            horizontal=True,
        )
        if k_view == "💼 By Role":
            k_role = st.selectbox("Select Role", roles_all, key="kanban_role")
            kdf = _base_df[_base_df["Role"] == k_role].copy()
            if show_archived:
                arch_opps = []
                for pid in _role_to_pids.get(k_role, []):
                    arch_opps.extend(load_archived_for_posting(pid))
                if arch_opps:
                    adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                    adf["Archived"] = True
                    kdf = pd.concat([kdf, adf[adf["Role"] == k_role]], ignore_index=True)
        elif k_view == "👤 By Hiring Manager":
            k_hm = st.selectbox("Select Hiring Manager", hms_all, key="kanban_hm")
            kdf = _base_df[_base_df["Hiring Manager"] == k_hm].copy()
            if show_archived:
                arch_opps = []
                for pid in _hm_to_pids.get(k_hm, []):
                    arch_opps.extend(load_archived_for_posting(pid))
                if arch_opps:
                    adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                    adf["Archived"] = True
                    kdf = pd.concat([kdf, adf[adf["Hiring Manager"] == k_hm]], ignore_index=True)
        elif k_view == "👩‍💼 By Recruiter":
            k_rec = st.selectbox("Select Recruiter", recs_all, key="kanban_rec")
            kdf = _base_df[_base_df["Recruiter"] == k_rec].copy()
        else:
            k_dir = st.selectbox("Select Director", directors_all, key="kanban_dir")
            kdf = _base_df[_base_df["Director"] == k_dir].copy()
            if show_archived:
                arch_opps = []
                for pid in _director_to_pids.get(k_dir, []):
                    arch_opps.extend(load_archived_for_posting(pid))
                if arch_opps:
                    adf = build_pipeline_df(arch_opps, posting_map, user_map, stage_map)
                    adf["Archived"] = True
                    kdf = pd.concat([kdf, adf[adf["Director"] == k_dir]], ignore_index=True)

    if not show_sourcing:
        kdf = kdf[~kdf["Stage"].isin(SOURCING_STAGES)]

    _render_kanban(kdf)
