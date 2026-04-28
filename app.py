import os, re
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium

st.set_page_config(page_title="Digital Village · Farm Intelligence", layout="wide", page_icon="🌾")

st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background: #0f1117 !important; color: #e6edf3 !important;
}
[data-testid="stSidebar"] { background: #161b22 !important; border-right: 1px solid #21262d; }
[data-testid="stSidebar"] * { color: #e6edf3 !important; }
section.main > div { padding-top: 1rem; }
[data-testid="manage-app-button"] { display: none !important; }
.stDeployButton { display: none !important; }
footer { display: none !important; }
#MainMenu { display: none !important; }
.proj-header { background:linear-gradient(135deg,#0d2137 0%,#0a3d1f 100%); border-radius:14px; padding:22px 28px; margin-bottom:18px; border:1px solid #1e4d2b; }
.proj-header h1 { margin:0; font-size:1.6rem; color:#fff; font-weight:800; }
.proj-header .sub { color:#7ec8a0; font-size:0.88rem; margin-top:4px; }
.proj-header .credits { color:#a0c4b0; font-size:0.8rem; margin-top:10px; }
.stat-row { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:16px; }
.stat-chip { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:10px 16px; flex:1; min-width:90px; text-align:center; }
.stat-chip .num { font-size:1.5rem; font-weight:800; color:#58a6ff; }
.stat-chip .lbl { font-size:0.72rem; color:#8b949e; text-transform:uppercase; letter-spacing:.5px; margin-top:2px; }
.farm-header { background:linear-gradient(135deg,#1a3a1a,#1e4620); border:1px solid #2ea043; border-radius:12px; padding:16px 20px; margin-bottom:14px; }
.farm-header .fid { font-size:0.75rem; color:#7ec8a0; letter-spacing:1px; text-transform:uppercase; }
.farm-header h2 { margin:3px 0; font-size:1.25rem; color:#fff; font-weight:700; }
.farm-header .meta { color:#a0c4b0; font-size:0.85rem; margin-top:4px; }
.card { background:#161b22; border:1px solid #21262d; border-radius:10px; padding:14px 16px; margin-bottom:10px; }
.card .card-title { font-size:0.72rem; color:#8b949e; text-transform:uppercase; letter-spacing:.8px; font-weight:600; margin-bottom:10px; padding-bottom:6px; border-bottom:1px solid #21262d; }
.kv { display:flex; justify-content:space-between; padding:5px 0; border-bottom:1px solid #0d1117; align-items:center; }
.kv:last-child { border-bottom:none; }
.kv .k { color:#8b949e; font-size:0.84rem; flex-shrink:0; }
.kv .val { color:#e6edf3; font-size:0.84rem; font-weight:500; text-align:right; margin-left:12px; word-break:break-word; }
.badge-y { background:#1a3a1a; color:#3fb950; border:1px solid #2ea043; padding:2px 8px; border-radius:20px; font-size:0.75rem; font-weight:600; }
.badge-n { background:#3a1a1a; color:#f85149; border:1px solid #b22222; padding:2px 8px; border-radius:20px; font-size:0.75rem; font-weight:600; }
.badge-na { background:#21262d; color:#8b949e; padding:2px 8px; border-radius:20px; font-size:0.75rem; }
.dev-card { background:#0d1117; border:1px solid #30363d; border-radius:8px; padding:10px 14px; margin:6px 0; }
.dev-title { color:#58a6ff; font-size:0.82rem; font-weight:700; margin-bottom:6px; }
.dkv { display:flex; gap:8px; align-items:flex-start; font-size:0.8rem; margin:3px 0; }
.dkv .dk { color:#8b949e; min-width:60px; flex-shrink:0; }
.dkv .dv { color:#e6edf3; word-break:break-all; }
.dkv code { background:#21262d; color:#79c0ff; padding:1px 6px; border-radius:4px; font-size:0.78rem; word-break:break-all; }
.sec { font-size:0.78rem; color:#7ec8a0; text-transform:uppercase; letter-spacing:1px; font-weight:700; margin:14px 0 6px; display:flex; align-items:center; gap:6px; }
.sec::after { content:''; flex:1; height:1px; background:#21262d; }
.sb-proj { background:linear-gradient(135deg,#0d2137,#0a3d1f); border-radius:10px; padding:12px 14px; margin-bottom:12px; border:1px solid #1e4d2b; }
.stTextInput input, .stSelectbox select, div[data-baseweb="select"] {
    background:#161b22 !important; color:#e6edf3 !important; border-color:#30363d !important;
}
p, label, .stMarkdown { color:#e6edf3 !important; }
.error-box { background:#3a1a1a; border:1px solid #b22222; border-radius:10px; padding:14px 18px; margin:10px 0; color:#f85149; font-size:0.85rem; line-height:1.8; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLISHED URLs — exact /e/2PACX-... format from "Publish to web" dialog.
# These work without any authentication. Do NOT use /d/... private sheet IDs.
# ─────────────────────────────────────────────────────────────────────────────

# AWD Farmers — "AWD Offers: Agreed And Refused" tab (GID 1066902470)
AWD_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRk1lqKuks4K3MXtX8cZA0SR-ESLK3D8NvTuKRVpWzNVunYYaUgqsBrasiSOKWl49LfPM2uTZwaW3UD"
    "/pub?gid=1066902470&single=true&output=csv"
)

# Libertalia — "master_control" tab (GID 2142859508)
LIBERTALIA_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSmv6Sq_o-w_zH7gQQxQakTbjxbO95ORl995an6iEYEwN-SiLHEsTyUHMAjmaIT9NZGX5qndns_bQA3"
    "/pub?gid=2142859508&single=true&output=csv"
)


@st.cache_data(ttl=60, show_spinner="🌾 Loading farm data…")
def load_data():
    """
    Load AWD farmers list + Libertalia polygons/tubewell data and merge them.
    Uses /e/2PACX-... published URLs — no auth, no 401 errors.
    TTL = 60s → live updates every minute.
    """
    import requests
    from io import StringIO

    req_hdrs = {
        "User-Agent": "Mozilla/5.0 (compatible; FarmDashboard/1.0)",
        "Accept":     "text/csv,text/plain,*/*",
    }

    def fetch_csv(url, label):
        for attempt in range(3):
            try:
                r = requests.get(url, headers=req_hdrs, timeout=30, allow_redirects=True)
                if r.status_code == 200 and len(r.text.strip()) > 50:
                    df = pd.read_csv(StringIO(r.text), dtype=str, keep_default_na=False)
                    df.columns = [str(c).strip() for c in df.columns]
                    return df
                if attempt == 2:
                    raise Exception(
                        f"HTTP {r.status_code} — is the sheet still published? "
                        "Go to File → Share → Publish to web."
                    )
            except Exception as e:
                if attempt == 2:
                    raise Exception(f"Could not load '{label}': {e}")
        raise Exception(f"Could not load '{label}'")

    # ── AWD Farmers List ──────────────────────────────────────────────────────
    df_awd = fetch_csv(AWD_URL, "AWD Farmers List")

    # Find Farm ID column — handles "Kharif 25 Farm ID", "Kharif 25 Farm Id", etc.
    farm_id_col = None
    for col in df_awd.columns:
        if re.search(r'farm.{0,3}id', col, re.I):
            farm_id_col = col
            break
    if farm_id_col is None:
        raise Exception(
            f"AWD sheet loaded ({df_awd.shape[0]} rows x {df_awd.shape[1]} cols) "
            f"but no Farm ID column found.\n"
            f"First 15 columns: {list(df_awd.columns[:15])}"
        )

    df_awd = df_awd.rename(columns={farm_id_col: 'Farm ID'})
    df_awd['Farm ID'] = df_awd['Farm ID'].astype(str).str.strip()
    df_awd = df_awd[df_awd['Farm ID'].str.len() > 3]
    df_awd = df_awd[~df_awd['Farm ID'].isin(['nan', 'None', 'Farm ID', ''])]

    if df_awd.empty:
        raise Exception("AWD sheet has no valid Farm ID rows after cleaning.")

    # ── Libertalia master_control ─────────────────────────────────────────────
    df_lib = fetch_csv(LIBERTALIA_URL, "Libertalia master_control")

    col_map = {}
    for col in df_lib.columns:
        cl = col.lower().strip()
        if not col_map.get('Farm ID') and (('plot' in cl and 'code' in cl) or cl == 'farm id'):
            col_map['Farm ID'] = col
        if not col_map.get('polygons') and 'polygon' in cl:
            col_map['polygons'] = col
        if not col_map.get('tw location') and 'tw' in cl and 'loc' in cl:
            col_map['tw location'] = col
    # exact name fallbacks
    for src, dst in [('Plot code','Farm ID'), ('polygons','polygons'), ('tw location','tw location')]:
        if dst not in col_map and src in df_lib.columns:
            col_map[dst] = src

    missing = [k for k in ['Farm ID','polygons','tw location'] if k not in col_map]
    if missing:
        raise Exception(
            f"Libertalia sheet missing: {missing}\n"
            f"Columns (first 20): {list(df_lib.columns[:20])}"
        )

    df_lib = df_lib[[col_map['Farm ID'], col_map['polygons'], col_map['tw location']]].copy()
    df_lib.columns = ['Farm ID', 'polygons', 'tw location']
    df_lib['Farm ID'] = df_lib['Farm ID'].astype(str).str.strip()
    df_lib = df_lib[df_lib['Farm ID'].str.len() > 3]
    df_lib = df_lib[~df_lib['Farm ID'].isin(['nan', 'None', ''])]

    # ── Merge ─────────────────────────────────────────────────────────────────
    df_merged = pd.merge(df_awd, df_lib, on='Farm ID', how='inner')
    return df_merged, df_awd


# ── Load — show helpful error if it fails ────────────────────────────────────
try:
    df, df_all = load_data()
    if df.empty:
        st.error(
            "⚠️ Both sheets loaded but the merge produced 0 rows.\n\n"
            "Farm IDs in the AWD sheet don't match IDs in Libertalia master_control.\n"
            "Confirm that Farm IDs like `KH_BH_PA_GURVI_PLT_10426` appear in both sheets."
        )
        st.stop()
except Exception as e:
    st.markdown(f"""
    <div class="error-box">
      <b>❌ Could not load data from Google Sheets</b><br><br>
      <b>Error:</b> {e}<br><br>
      <b>Steps to fix:</b><br>
      1. Open the AWD sheet → File → Share → Publish to web → confirm "Published"<br>
      2. Open the Libertalia sheet → same steps<br>
      3. Tick "Automatically republish when changes are made"<br>
      4. Click the 🔄 Reload button in the sidebar
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _find_col(row_dict, *candidates):
    for c in candidates:
        val = str(row_dict.get(c, '')).strip()
        if val not in ('', 'nan', 'None', 'NaN'):
            return val
    return ''

def v(row, col):
    x = str(row.get(col, '')).strip()
    return '' if x in ('nan','None','NaN','') else x

def clean_date(s):
    if not s: return ''
    s = re.sub(r'\s+00:00:00.*', '', s).strip()
    s = re.sub(r'(\d{4})-(\d{2})-(\d{2})', lambda m: f"{m.group(3)}/{m.group(2)}/{m.group(1)}", s)
    return s

def parse_polygon(raw):
    coords = []
    for seg in raw.strip().split(';'):
        p = seg.strip().split()
        if len(p) >= 2:
            try: coords.append((float(p[0]), float(p[1])))
            except: pass
    return coords

def parse_decimal(raw):
    p = raw.strip().split(';')[0].split()
    if len(p) >= 2:
        try: return float(p[0]), float(p[1])
        except: pass
    return None

def parse_dms(s):
    s2 = s.replace('\xb0','°').replace('\u2019',"'").replace('\u201d','"').replace('\u2033','"')
    m = re.findall(r"(\d+)°(\d+)'([\d.]+)\"?([NSEW])", s2)
    if len(m) >= 2:
        def dd(d, mn, sc, di):
            val = float(d) + float(mn)/60 + float(sc)/3600
            return -val if di in ('S','W') else val
        return dd(*m[0]), dd(*m[1])
    return None

def parse_any(raw):
    if not raw or str(raw).strip() in ('nan','None',''): return None
    return parse_decimal(raw) or parse_dms(raw)

def badge(val):
    u = str(val).strip().upper()
    if u in ('Y','YES','1'): return '<span class="badge-y">✓ Yes</span>'
    if u in ('N','NO','0'):  return '<span class="badge-n">✗ No</span>'
    return f'<span class="badge-na">{val}</span>' if val not in ('','nan','None') else '<span class="badge-na">—</span>'

def kv_row(key, val_html):
    return f'<div class="kv"><span class="k">{key}</span><span class="val">{val_html}</span></div>'

def dkv_row(key, content, code=False):
    inner = f'<code>{content}</code>' if code else content
    return f'<div class="dkv"><span class="dk">{key}</span><span class="dv">{inner}</span></div>'

def has_active_meter(row): return any(v(row,f'Kharif 25 Meter active / {i} (Y/N)').upper() in('Y','YES','1') for i in[1,2])
def has_meter(row):        return any(v(row,f'Kharif 25 Meter serial number / {i}') for i in[1,2])
def has_pipe(row):         return any(v(row,f'Kharif 25 PVC Pipe location / {i}') for i in[1,2,3,4,5])

def get_farmer_name(row):  return _find_col(row,'Kharif 25 Farmer Name','Farmer Name','Name','farmer name')
def get_farmer_phone(row): return _find_col(row,'Kharif 25 Farmer Phone Number','Kharif 25 Farmer Phone','Phone','Farmer Phone','Phone Number')
def get_village(row):      return _find_col(row,'Kharif 25 Village','Village','village')
def get_block(row):        return _find_col(row,'Kharif 25 Block','Block','block')


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-proj">
      <div style="font-size:0.88rem;color:#fff;font-weight:700">🌍 Digital Village Project</div>
      <div style="font-size:0.76rem;color:#7ec8a0;margin-top:3px">Tel Aviv University · Thapar University, Patiala</div>
      <div style="font-size:0.74rem;color:#8b949e;margin-top:8px;line-height:1.5">
        <b style="color:#c9d1d9">Research Lead:</b> Dan Uriel Etgar<br>
        <b style="color:#c9d1d9">Dashboard:</b> Satyam Yadav<br>
        <span style="opacity:.7">Lead Research Assistant</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    col_r1, _ = st.columns([1, 3])
    with col_r1:
        if st.button("🔄", help="Reload data from Google Sheets"):
            st.cache_data.clear()
            st.rerun()

    st.caption(f"✅ {len(df_all)} farms loaded · {len(df)} with map data · auto-refresh 60s")

    st.markdown("#### 🔍 Find a Farm")
    search = st.text_input("Search farm", placeholder="ID, name, village…", label_visibility="collapsed")

    st.markdown("---")
    st.markdown("#### ⚙️ Filter by Equipment")
    filter_opt = st.radio("Filter options", [
        "🌾 All farms","💧 Has water meter","🚫 No water meter",
        "✅ Active meter","⛔ Inactive meter","🔧 Has PVC pipes","🪣 No PVC pipes",
    ], label_visibility="collapsed")

    all_kharif_list = df_all.to_dict('records')
    fmap = {
        "💧 Has water meter":  lambda r: has_meter(r),
        "🚫 No water meter":   lambda r: not has_meter(r),
        "✅ Active meter":     lambda r: has_active_meter(r),
        "⛔ Inactive meter":   lambda r: has_meter(r) and not has_active_meter(r),
        "🔧 Has PVC pipes":    lambda r: has_pipe(r),
        "🪣 No PVC pipes":     lambda r: not has_pipe(r),
    }
    pool = [r for r in all_kharif_list if fmap[filter_opt](r)] if filter_opt in fmap else all_kharif_list
    pool_ids = sorted(set(r['Farm ID'] for r in pool))

    if search:
        q = search.strip().upper()
        filtered_ids = sorted(set(
            [i for i in pool_ids if q in i.upper()] +
            [r['Farm ID'] for r in pool if q in get_farmer_name(r).upper()] +
            [r['Farm ID'] for r in pool if q in get_village(r).upper()]
        ))
    else:
        filtered_ids = pool_ids

    st.caption(f"**{len(filtered_ids)}** farms match")
    if not filtered_ids:
        st.warning("No farms match."); st.stop()

    selected_id = st.selectbox("Select farm", filtered_ids, index=0, label_visibility="collapsed")


if not selected_id:
    st.info("No farms found."); st.stop()

# ── Resolve row ───────────────────────────────────────────────────────────────
df_match = df[df['Farm ID'] == selected_id]
if len(df_match) > 0:
    row = df_match.iloc[0].to_dict()
else:
    df_all_match = df_all[df_all['Farm ID'] == selected_id]
    if len(df_all_match) > 0:
        row = df_all_match.iloc[0].to_dict()
        row['polygons'] = ''
        row['tw location'] = ''
    else:
        st.warning("Farm not found."); st.stop()

farmer_name  = get_farmer_name(row)
farmer_phone = get_farmer_phone(row)
village      = get_village(row)
block        = get_block(row)

# ── Stats ─────────────────────────────────────────────────────────────────────
all_kharif = df_all.to_dict('records')
c_all      = len(all_kharif)
vcol = next((c for c in df_all.columns if 'village' in c.lower()), None)
bcol = next((c for c in df_all.columns if 'block'   in c.lower()), None)
c_villages = df_all[vcol].nunique() if vcol else 0
c_blocks   = df_all[bcol].nunique() if bcol else 0
c_meter    = sum(1 for r in all_kharif if has_meter(r))
c_no_meter = sum(1 for r in all_kharif if not has_meter(r))
c_active   = sum(1 for r in all_kharif if has_active_meter(r))
c_inactive = sum(1 for r in all_kharif if has_meter(r) and not has_active_meter(r))
c_pipe     = sum(1 for r in all_kharif if has_pipe(r))
c_no_pipe  = sum(1 for r in all_kharif if not has_pipe(r))
pool_df       = df_all[df_all['Farm ID'].isin(pool_ids)]
pool_villages = pool_df[vcol].nunique() if vcol else 0
pool_blocks   = pool_df[bcol].nunique() if bcol else 0

chip_configs = {
    "🌾 All farms":       (c_all,      c_villages,    c_blocks,    c_meter,    "With Meter"),
    "💧 Has water meter": (c_meter,    pool_villages, pool_blocks, c_active,   "Active Meter"),
    "🚫 No water meter":  (c_no_meter, pool_villages, pool_blocks, c_no_meter, "No Meter"),
    "✅ Active meter":    (c_active,   pool_villages, pool_blocks, c_active,   "Active"),
    "⛔ Inactive meter":  (c_inactive, pool_villages, pool_blocks, c_inactive, "Inactive"),
    "🔧 Has PVC pipes":   (c_pipe,     pool_villages, pool_blocks, c_pipe,     "With Pipes"),
    "🪣 No PVC pipes":    (c_no_pipe,  pool_villages, pool_blocks, c_no_pipe,  "No Pipes"),
}
chip1,chip2,chip3,chip4_num,chip4_lbl = chip_configs.get(filter_opt,(c_all,c_villages,c_blocks,c_meter,"With Meter"))
chip1_lbl = {"🌾 All farms":"Total Farms","💧 Has water meter":"With Meter","🚫 No water meter":"No Meter",
             "✅ Active meter":"Active Meter","⛔ Inactive meter":"Inactive Meter",
             "🔧 Has PVC pipes":"With Pipes","🪣 No PVC pipes":"No Pipes"}.get(filter_opt,"Total Farms")

st.markdown(f"""
<div class="proj-header">
  <h1>🌾 Digital Village Project</h1>
  <div class="sub">Kharif 2025 Farm Intelligence &nbsp;·&nbsp; Tel Aviv University | Thapar University, Patiala</div>
  <div class="credits">Research Lead: <b>Dan Uriel Etgar</b> &nbsp;·&nbsp; Dashboard: <b>Satyam Yadav</b> (Lead Research Assistant)</div>
</div>
<div class="stat-row">
  <div class="stat-chip"><div class="num">{chip1}</div><div class="lbl">{chip1_lbl}</div></div>
  <div class="stat-chip"><div class="num">{chip2}</div><div class="lbl">Villages</div></div>
  <div class="stat-chip"><div class="num">{chip3}</div><div class="lbl">Blocks</div></div>
  <div class="stat-chip"><div class="num" style="color:#3fb950">{chip4_num}</div><div class="lbl">{chip4_lbl}</div></div>
</div>
<div class="farm-header">
  <div class="fid">Selected Farm</div>
  <h2>{v(row,'Farm ID')}</h2>
  <div class="meta">👤 {farmer_name or '—'} &nbsp;·&nbsp; 📞 {farmer_phone or '—'} &nbsp;·&nbsp; 🏘️ {village or '—'}, {block or '—'}</div>
</div>
""", unsafe_allow_html=True)

col1, col2 = st.columns([1.05, 1], gap="medium")

# ── LEFT COLUMN ───────────────────────────────────────────────────────────────
with col1:

    st.markdown('<div class="sec">📋 Farm Details</div>', unsafe_allow_html=True)
    rows = [kv_row("Farm ID", v(row,'Farm ID')),
            kv_row("Farmer",  farmer_name or '—'),
            kv_row("Phone",   farmer_phone or '—'),
            kv_row("Village", village or '—'),
            kv_row("Block",   block or '—')]
    grp  = _find_col(row,'Groups','Kharif 25 Groups','Study Group','Group')
    zone = _find_col(row,'Kharif 25 Study Zone','Study Zone','Zone')
    if grp:  rows.append(kv_row("Group", grp))
    if zone: rows.append(kv_row("Zone",  zone))
    st.markdown(f'<div class="card"><div class="card-title">Basic Information</div>{"".join(rows)}</div>', unsafe_allow_html=True)

    st.markdown('<div class="sec">🌿 Season & Compliance</div>', unsafe_allow_html=True)
    rows = []
    acres = _find_col(row,'Kharif 25 Acres farm / farmer reporting','Acres','Farm Size')
    if acres: rows.append(kv_row("Acres", acres))
    paddy = _find_col(row,'Kharif 25 Paddy Rice (Y/N)','Paddy Rice (Y/N)','Paddy Rice')
    if paddy: rows.append(kv_row("Paddy Rice", badge(paddy)))
    nursery    = clean_date(_find_col(row,'Kharif 25 Nursary sowing (TPR) and DSR sowing - Date','Nursery Date'))
    transplant = clean_date(_find_col(row,'Kharif 25 Paddy transplanting date (TPR)','Transplanting Date'))
    harvest    = clean_date(_find_col(row,'Kharif 25 Paddy Harvest date','Harvest Date','Harvest'))
    if nursery:    rows.append(kv_row("Nursery/Sowing", nursery))
    if transplant: rows.append(kv_row("Transplanting",  transplant))
    if harvest:    rows.append(kv_row("Harvest",        harvest))
    complied  = _find_col(row,'AWD Complied (Y/N)','Complied','AWD complied')
    agreed    = _find_col(row,'Agreed to join the study','Agreed')
    f2f       = _find_col(row,'Offered to join the study - Face to Face meeting (Y/N)','Face to Face meeting (Y/N)')
    awd_train = _find_col(row,'AWD Training Given (Y/N)','AWD Training')
    pvc_sign  = _find_col(row,'Farmer Signed registration form (Y/N)','Signed form')
    if complied:  rows.append(kv_row("AWD Complied",   badge(complied)))
    if agreed:    rows.append(kv_row("Agreed to Study", badge(agreed)))
    if f2f:       rows.append(kv_row("F2F Meeting",     badge(f2f)))
    if awd_train: rows.append(kv_row("AWD Training",    badge(awd_train)))
    if pvc_sign:  rows.append(kv_row("Signed Form",     badge(pvc_sign)))
    if rows:
        st.markdown(f'<div class="card"><div class="card-title">Season & Compliance</div>{"".join(rows)}</div>', unsafe_allow_html=True)

    # Studies (these columns may not exist in AWD sheet, shown only if present)
    studies_rows = []
    for label, col in [('TPR Study','Kharif 25 / TPR Group Study (Y/N)'),
                        ('DSR Study','Kharif 25 / DSR farm Study (Y/N)'),
                        ('RC Study', 'Kharif 25 / Remote Controllers study (Y/N)'),
                        ('AWD Study','Kharif 25 / AWD Study (Y/N)')]:
        val = v(row, col)
        if val: studies_rows.append(kv_row(label, badge(val)))
    if studies_rows:
        st.markdown(f'<div class="card"><div class="card-title">Research Groups</div>{"".join(studies_rows)}</div>', unsafe_allow_html=True)

    # Water meters
    inst = clean_date(v(row,'Kharif 25 meter installation date'))
    rem  = clean_date(v(row,'Kharif 25 meter remove date'))
    if has_meter(row):
        st.markdown('<div class="sec">💧 Water Meters</div>', unsafe_allow_html=True)
        rows = [kv_row("Monitoring", badge(v(row,'Kharif 25 Meter monitoring (Y/N) was done at any stage of the season')))]
        if inst: rows.append(kv_row("Installed", inst))
        if rem:  rows.append(kv_row("Removed",   rem))
        for i in [1, 2]:
            ser  = v(row,f'Kharif 25 Meter serial number / {i}')
            locm = v(row,f'Kharif 25 Meter location / {i}')
            act  = v(row,f'Kharif 25 Meter active / {i} (Y/N)')
            if ser or locm:
                drows = [dkv_row("Active", badge(act))]
                if ser:  drows.append(dkv_row("Serial",   ser,  code=True))
                if locm: drows.append(dkv_row("Location", locm, code=True))
                st.markdown(f'<div class="dev-card"><div class="dev-title">💧 Water Meter {i}</div>{"".join(drows)}</div>', unsafe_allow_html=True)

    # PVC pipes
    any_pipe_loc = any(v(row,f'Kharif 25 PVC Pipe location / {i}') for i in[1,2,3,4,5])
    if any_pipe_loc:
        st.markdown('<div class="sec">🔧 PVC Pipes</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="margin-bottom:6px;font-size:0.82rem;color:#8b949e">Monitoring: {badge(v(row,"Kharif 25 PVC Pipe monitoring (Y/N)"))}</div>', unsafe_allow_html=True)
        for i in [1,2,3,4,5]:
            code  = v(row,f'Kharif 25 PVC Pipe code / {i}')
            loc_p = v(row,f'Kharif 25 PVC Pipe location / {i}')
            if loc_p:
                drows = [dkv_row("Farm",v(row,'Farm ID'),code=True),dkv_row("Farmer",farmer_name),dkv_row("Phone",farmer_phone)]
                if code: drows.append(dkv_row("Code",code,code=True))
                drows.append(dkv_row("Location",loc_p,code=True))
                st.markdown(f'<div class="dev-card"><div class="dev-title">🔧 Pipe {i}</div>{"".join(drows)}</div>', unsafe_allow_html=True)


# ── MAP ───────────────────────────────────────────────────────────────────────
with col2:
    st.markdown('<div class="sec">🗺️ Farm Map</div>', unsafe_allow_html=True)

    polygon_raw  = str(row.get('polygons','')).strip()
    tubewell_raw = str(row.get('tw location','')).strip()
    polygon_coords = parse_polygon(polygon_raw) if polygon_raw not in ('','nan','None') else []
    tubewell_coord = parse_any(tubewell_raw)    if tubewell_raw not in ('','nan','None') else None

    def snap_inside(pt, poly):
        if not poly or not pt: return pt
        lat,lon = pt
        best = min(poly, key=lambda p: (p[0]-lat)**2+(p[1]-lon)**2)
        cl = sum(p[0] for p in poly)/len(poly)
        cn = sum(p[1] for p in poly)/len(poly)
        return (best[0]+(cl-best[0])*0.15, best[1]+(cn-best[1])*0.15)

    def in_poly(pt, poly):
        if not poly or not pt: return False
        lat,lon=pt; n=len(poly); inside=False; j=n-1
        for i in range(n):
            xi,yi=poly[i]; xj,yj=poly[j]
            if ((yi>lon)!=(yj>lon)) and (lat<(xj-xi)*(lon-yi)/(yj-yi+1e-10)+xi):
                inside=not inside
            j=i
        return inside

    meter_locations = {}
    for i in [1,2]:
        mser=v(row,f'Kharif 25 Meter serial number / {i}')
        mloc=v(row,f'Kharif 25 Meter location / {i}')
        mact=v(row,f'Kharif 25 Meter active / {i} (Y/N)')
        if mser:
            meter_locations[i] = {'coord':parse_any(mloc) if mloc else None,'serial':mser,'active':mact}
    mappable_meters = {i:d for i,d in meter_locations.items() if d['coord']}

    if polygon_coords:
        clat=sum(c[0] for c in polygon_coords)/len(polygon_coords)
        clon=sum(c[1] for c in polygon_coords)/len(polygon_coords)
    elif mappable_meters:
        clat,clon=list(mappable_meters.values())[0]['coord']
    elif tubewell_coord:
        clat,clon=tubewell_coord
    else:
        clat,clon=30.41,76.42

    if not polygon_coords and not tubewell_coord and not mappable_meters:
        st.markdown("""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:12px;
             padding:40px 24px;text-align:center;margin-top:10px">
          <div style="font-size:2.5rem;margin-bottom:12px">📭</div>
          <div style="color:#e6edf3;font-size:1.05rem;font-weight:600;margin-bottom:8px">Location Data Unavailable</div>
          <div style="color:#8b949e;font-size:0.84rem;line-height:1.6">
            Farm exists in AWD sheet but has no polygon/tubewell data in Libertalia.<br><br>
            <span style="color:#58a6ff">Farm details on the left are still complete.</span>
          </div>
        </div>""", unsafe_allow_html=True)
    else:
        m = folium.Map(location=[clat,clon], zoom_start=16, tiles="OpenStreetMap")
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Esri', name='Satellite', overlay=False, control=True
        ).add_to(m)
        try:
            from folium.plugins import LocateControl
            LocateControl(position='topleft',flyTo=True,locateOptions={"enableHighAccuracy":True}).add_to(m)
        except: pass

        css = "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:13px;min-width:210px;color:#24292f"
        def nav_btn(lat,lon):
            return (f'<a href="https://www.google.com/maps/dir/?api=1&destination={lat},{lon}" target="_blank" '
                    f'style="display:inline-block;background:#238636;color:#fff;padding:5px 12px;border-radius:6px;'
                    f'text-decoration:none;font-size:12px;font-weight:600;margin-top:8px">🧭 Navigate here</a>')
        def pr(k,val):
            return (f'<tr><td style="color:#6e7781;padding:3px 4px 3px 0;white-space:nowrap;vertical-align:top">{k}</td>'
                    f'<td style="padding:3px 0 3px 10px;font-weight:500;text-align:right">{val}</td></tr>')
        def mk_popup(hcolor,htitle,rh,lat,lon,mw=290):
            html=(f'<div style="{css}"><div style="background:{hcolor};color:#fff;padding:8px 10px;'
                  f'border-radius:6px 6px 0 0;margin:-9px -9px 10px;font-weight:700">{htitle}</div>'
                  f'<table style="width:100%;border-collapse:collapse">{rh}</table>{nav_btn(lat,lon)}</div>')
            return folium.Popup(html, max_width=mw)

        if polygon_coords:
            rh  = pr("Farmer",farmer_name)+pr("Phone",farmer_phone)+pr("Village",village)+pr("Block",block)
            ac  = _find_col(row,'Kharif 25 Acres farm / farmer reporting','Acres')
            if ac: rh += pr("Acres",ac)
            folium.Polygon(locations=polygon_coords,color='#2ea043',fill=True,
                fill_color='#3fb950',fill_opacity=0.25,weight=2.5,
                tooltip="🌾 Click for farm details",
                popup=mk_popup('#0a3d1f',f'🌾 {v(row,"Farm ID")}',rh,clat,clon)).add_to(m)

        if tubewell_coord and not mappable_meters:
            rh = pr("Farm",v(row,'Farm ID'))+pr("Farmer",farmer_name)+pr("Lat",f"{tubewell_coord[0]:.6f}°N")+pr("Lon",f"{tubewell_coord[1]:.6f}°E")
            folium.Marker(location=tubewell_coord,tooltip="💧 Tubewell",
                popup=mk_popup('#0d2137','💧 Tubewell',rh,*tubewell_coord),
                icon=folium.Icon(color='blue',icon='tint',prefix='fa')).add_to(m)

        for i,mdata in mappable_meters.items():
            mc=mdata['coord']
            mc_d=snap_inside(mc,polygon_coords) if polygon_coords and not in_poly(mc,polygon_coords) else mc
            rh=pr("Serial",f'<b>{mdata["serial"]}</b>')+pr("Active",mdata['active'])+pr("Farmer",farmer_name)
            folium.Marker(location=mc_d,tooltip=f"🟣 Meter {i}: {mdata['serial']}",
                popup=mk_popup('#4a0080',f'🟣 Water Meter {i}',rh,*mc_d,mw=260),
                icon=folium.Icon(color='purple',icon='tint',prefix='fa')).add_to(m)

        for i in [1,2,3,4,5]:
            ploc=v(row,f'Kharif 25 PVC Pipe location / {i}')
            pcode=v(row,f'Kharif 25 PVC Pipe code / {i}')
            if ploc:
                pc=parse_any(ploc)
                if pc:
                    pc_d=snap_inside(pc,polygon_coords) if polygon_coords and not in_poly(pc,polygon_coords) else pc
                    rh=pr("Code",f'<b>{pcode}</b>')+pr("Farmer",farmer_name)
                    folium.Marker(location=pc_d,tooltip=f"🔴 Pipe {i}: {pcode}",
                        popup=mk_popup('#7b0000',f'🔴 PVC Pipe {i}',rh,*pc_d,mw=230),
                        icon=folium.DivIcon(
                            html='<div style="background:#c0392b;color:white;border-radius:50%;width:26px;height:26px;display:flex;align-items:center;justify-content:center;font-size:13px;border:2px solid #fff;box-shadow:0 2px 4px rgba(0,0,0,0.5)">🪧</div>',
                            icon_size=(26,26),icon_anchor=(13,13))).add_to(m)

        folium.LayerControl().add_to(m)
        st_folium(m, width=None, height=490, returned_objects=[])

        parts=[]
        if polygon_coords: parts.append('<span style="color:#3fb950">■</span> Farm Polygon')
        if tubewell_coord and not mappable_meters: parts.append('<span style="color:#58a6ff">●</span> Tubewell')
        if mappable_meters: parts.append('<span style="color:#9b59b6">●</span> Water Meter')
        if any(parse_any(v(row,f'Kharif 25 PVC Pipe location / {i}')) for i in[1,2,3,4,5]):
            parts.append('<span style="color:#f85149">🪧</span> PVC Pipe')
        if parts:
            st.markdown(f'<div style="background:#161b22;border:1px solid #21262d;border-radius:8px;padding:8px 14px;font-size:0.8rem;color:#8b949e;margin-top:6px">{"&nbsp;&nbsp;".join(parts)}</div>', unsafe_allow_html=True)

        nav_link=(f'<a href="https://www.google.com/maps/dir/?api=1&destination={clat},{clon}" '
                  f'target="_blank" style="background:#238636;color:#fff;padding:6px 14px;border-radius:6px;'
                  f'text-decoration:none;font-size:0.82rem;font-weight:600;display:inline-block;margin-top:8px">'
                  f'🧭 Open in Google Maps</a>')
        st.markdown(f'<div class="card" style="margin-top:8px"><div class="card-title">Coordinates &amp; Navigation</div>'
                    f'{kv_row("Center",f"{clat:.5f}°N, {clon:.5f}°E")}{nav_link}</div>', unsafe_allow_html=True)

st.markdown(f'<hr style="border-color:#21262d;margin:20px 0"><p style="color:#484f58;font-size:0.76rem;text-align:center">'
            f'🌍 Digital Village Project &nbsp;·&nbsp; Tel Aviv University &amp; Thapar University, Patiala &nbsp;·&nbsp; '
            f'Research Lead: Dan Uriel Etgar &nbsp;·&nbsp; Dashboard: Satyam Yadav &nbsp;·&nbsp; '
            f'{len(df_all)} farms</p>', unsafe_allow_html=True)
