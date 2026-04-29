from flask import Flask, render_template, jsonify, request, session
import csv, io, os, re, time, threading, logging, requests, hashlib, json

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'awd-dashboard-secret-2025')
logging.basicConfig(level=logging.INFO)

# ── Google Sheets IDs ─────────────────────────────────────────────────────────
AWD_SHEET_ID = "180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k"
LIB_SHEET_ID = "14ah-7Ah690oeOXE5vT8p701LYv7PiEMx_xZycNOOrSA"

AWD_GID = "1066902470"
LIB_GID = "2142859508"

AWD_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRk1lqKuks4K3MXtX8cZA0SR-ESLK3D8NvTuKRVpWzNVunYYaUgqsBrasiSOKWl49LfPM2uTZwaW3UD"
    f"/pub?gid={AWD_GID}&single=true&output=csv"
)
LIB_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSmv6Sq_o-w_zH7gQQxQakTbjxbO95ORl995an6iEYEwN-SiLHEsTyUHMAjmaIT9NZGX5qndns_bQA3"
    f"/pub?gid={LIB_GID}&single=true&output=csv"
)

# ── Dev credentials ───────────────────────────────────────────────────────────
def _h(s): return hashlib.sha256(s.encode()).hexdigest()
DEV_USERS = {
    "Satyamyadav19125": _h("finnwolfhard@666"),
    "Danetgar":         _h("Etgardan"),
}

# ── gspread client (lazy init) ────────────────────────────────────────────────
_gs_client = None

def get_gs_client():
    global _gs_client
    if _gs_client is not None:
        return _gs_client
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        # Load from env var (JSON string) or fallback file
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if creds_json:
            info = json.loads(creds_json)
        else:
            # Local dev fallback: place JSON next to app.py
            with open('google_credentials.json') as f:
                info = json.load(f)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        _gs_client = gspread.authorize(creds)
        logging.info("gspread client initialized")
        return _gs_client
    except Exception as e:
        logging.error(f"gspread init failed: {e}")
        return None

def get_worksheet(sheet_id, gid):
    """Return gspread worksheet by sheet_id + numeric gid."""
    gc = get_gs_client()
    if not gc:
        return None
    try:
        sh = gc.open_by_key(sheet_id)
        for ws in sh.worksheets():
            if str(ws.id) == str(gid):
                return ws
        return sh.get_worksheet(0)
    except Exception as e:
        logging.error(f"get_worksheet error: {e}")
        return None

# ── Caches ────────────────────────────────────────────────────────────────────
_cache     = {"data": None, "ts": 0}
_raw_cache = {"awd": None, "lib": None, "ts": 0}
CACHE_TTL  = 60

# ── CSV helpers ───────────────────────────────────────────────────────────────
def fetch_csv(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; AWDDashboard/1.0)"}
    r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))

def find_col(row_dict, keywords):
    for k in keywords:
        for col in row_dict:
            if k.lower() in col.lower():
                return col
    return None

def extract_fallback_id(s):
    if not s: return None
    s = str(s).strip()
    if not s.startswith('='):
        return s if s else None
    m = re.search(r'"([A-Z]{2}_[A-Z]{2}_[A-Z]{2}_\w+_PLT_[\w+]+)"\)', s)
    return m.group(1) if m else None

def parse_wkt_geometry(wkt):
    if not wkt: return None, None, []
    wkt = str(wkt).strip()
    if wkt.startswith('POINT'):
        c = re.findall(r'([\d.]+)\s+([\d.]+)', wkt)
        if c: return float(c[0][1]), float(c[0][0]), []
    if wkt.startswith('POLYGON'):
        c = re.findall(r'([\d.]+)\s+([\d.]+)\s+[\d.]+', wkt)
        if c:
            poly = [[float(x[1]), float(x[0])] for x in c]
            lats = [p[0] for p in poly]; lons = [p[1] for p in poly]
            return sum(lats)/len(lats), sum(lons)/len(lons), poly
    return None, None, []

def parse_simplified_polygon(s):
    if not s: return []
    coords = []
    for seg in str(s).split(';'):
        parts = seg.strip().split()
        if len(parts) >= 2:
            try: coords.append([float(parts[0]), float(parts[1])])
            except: pass
    return coords

def parse_tw_location(raw):
    if not raw or str(raw).strip() in ('', 'nan', 'None'): return None, None
    raw = str(raw).strip()
    parts = raw.split()
    if len(parts) >= 2:
        try: return float(parts[0]), float(parts[1])
        except: pass
    raw2 = raw.replace('\xb0','°').replace('\u2019',"'").replace('\u201d','"').replace('\u2033','"')
    m = re.findall(r"(\d+)°(\d+)'([\d.]+)\"?([NSEW])", raw2)
    if len(m) >= 2:
        def dd(d, mn, sc, di):
            val = float(d)+float(mn)/60+float(sc)/3600
            return -val if di in ('S','W') else val
        return dd(*m[0]), dd(*m[1])
    return None, None

def groupkey(g):
    g = str(g).strip().upper()
    if 'GROUP A' in g or g == 'A': return 'A'
    if 'GROUP B' in g or g == 'B': return 'B'
    return 'C'

def safe_float(v):
    try: return float(v) if v and str(v).strip() not in ('','nan','N. A','N.A') else 0
    except: return 0

def normalise_comply(v):
    v = str(v).strip()
    if v in ('1','1.0','yes','Yes','YES','Y','y'): return '1.0'
    if v in ('0','0.0','no','No','NO','N','n'): return '0.0'
    return v

# ── Main data processing ──────────────────────────────────────────────────────
def fetch_and_process():
    logging.info("Fetching from Google Sheets...")
    try:
        awd_rows = fetch_csv(AWD_URL)
        if not awd_rows: return None
        awd_rows = [{k.strip(): v for k, v in row.items()} for row in awd_rows]

        lib_rows = fetch_csv(LIB_URL)
        lib_rows = [{k.strip(): v for k, v in row.items()} for row in lib_rows]

        s = awd_rows[0]
        awd_id_col  = find_col(s, ['farm id','farm_id'])
        farmer_col  = find_col(s, ['farmer name','farmer_name'])
        village_col = find_col(s, ['village'])
        group_col   = find_col(s, ['groups','group'])
        zone_col    = find_col(s, ['study zone','zone'])
        acres_col   = find_col(s, ['farm acres','acres'])
        inc_col     = find_col(s, ['incentive acres'])
        phone_col   = find_col(s, ['phone'])
        comply_col  = find_col(s, ['bank details verified','consent signed','complied'])

        ls = lib_rows[0] if lib_rows else {}
        lib_keys = list(ls.keys())
        lib_id_col   = find_col(ls, ['plot code','farm id','farm_id']) or (lib_keys[0] if lib_keys else None)
        lib_poly_col = find_col(ls, ['polygon','polygons'])
        lib_tw_col   = find_col(ls, ['tw location','tw_location'])
        lib_wkt_col  = find_col(ls, ['wkt'])
        lib_simp_col = find_col(ls, ['simplified'])
        if not lib_wkt_col  and len(lib_keys) > 1: lib_wkt_col  = lib_keys[1]
        if not lib_simp_col and len(lib_keys) > 2: lib_simp_col = lib_keys[2]

        lib_index = {}
        for row in lib_rows:
            poly = []; tw_lat, tw_lon = None, None
            if lib_poly_col: poly = parse_simplified_polygon(row.get(lib_poly_col, ''))
            if lib_tw_col:   tw_lat, tw_lon = parse_tw_location(row.get(lib_tw_col, ''))
            if tw_lat is None and lib_wkt_col:
                wkt = row.get(lib_wkt_col, '') or ''
                tw_lat, tw_lon, wkt_poly = parse_wkt_geometry(wkt)
                if not poly: poly = wkt_poly
            if not poly and lib_simp_col:
                poly = parse_simplified_polygon(row.get(lib_simp_col, ''))
            if tw_lat is None: continue
            if poly:
                lats = [p[0] for p in poly]; lons = [p[1] for p in poly]
                tw_lat = sum(lats)/len(lats); tw_lon = sum(lons)/len(lons)
            geo = {'tw_lat': tw_lat, 'tw_lon': tw_lon, 'polygon': poly}
            fid1 = str(row.get(lib_id_col, '') or '').strip()
            if fid1 and not fid1.startswith('='): lib_index[fid1] = geo
            lib_id2_col = lib_keys[10] if len(lib_keys) > 10 else None
            fid2 = extract_fallback_id(row.get(lib_id2_col, '')) if lib_id2_col else None
            if fid2 and fid2 != fid1: lib_index[fid2] = geo

        records = []
        skipped = 0
        for row in awd_rows:
            fid = str(row.get(awd_id_col, '') or '').strip()
            if not fid: continue
            if fid not in lib_index: skipped += 1; continue
            geo = lib_index[fid]
            group_val = row.get(group_col, '') if group_col else ''
            records.append({
                'farm_id':         fid,
                'farmer_name':     row.get(farmer_col, '')  if farmer_col  else '',
                'village':         row.get(village_col, '') if village_col else '',
                'group':           group_val,
                'group_key':       groupkey(group_val),
                'zone':            row.get(zone_col, '')    if zone_col    else '',
                'acres':           safe_float(row.get(acres_col, 0)  if acres_col else 0),
                'incentive_acres': safe_float(row.get(inc_col, 0)    if inc_col   else 0),
                'phone':           row.get(phone_col, '')   if phone_col   else '',
                'complied':        normalise_comply(row.get(comply_col, '') if comply_col else ''),
                'total_payment':   0,
                'tw_lat':          geo['tw_lat'],
                'tw_lon':          geo['tw_lon'],
                'polygon':         geo['polygon'],
            })

        village_map = {}
        for f in records:
            v = f['village']
            if v not in village_map:
                village_map[v] = {'village':v,'total':0,'group_a':0,'group_b':0,'group_c':0,'total_acres':0,'incentive_acres':0}
            village_map[v]['total'] += 1
            village_map[v]['group_'+f['group_key'].lower()] += 1
            village_map[v]['total_acres']     += f['acres']
            village_map[v]['incentive_acres'] += f['incentive_acres']
        villages = sorted(village_map.values(), key=lambda x: -x['total'])

        total    = len(records)
        group_a  = sum(1 for f in records if f['group_key']=='A')
        group_b  = sum(1 for f in records if f['group_key']=='B')
        group_c  = sum(1 for f in records if f['group_key']=='C')
        complied = sum(1 for f in records if f['complied']=='1.0')
        not_comp = sum(1 for f in records if f['complied']=='0.0')
        t_acres  = round(sum(f['acres'] for f in records), 1)
        i_acres  = round(sum(f['incentive_acres'] for f in records), 1)

        stats = {
            'total':total,'group_a':group_a,'group_b':group_b,'group_c':group_c,
            'complied':complied,'not_complied':not_comp,
            'total_acres':t_acres,'incentive_acres':i_acres,
            'total_payment':0,'villages':len(villages),
            'last_updated':time.strftime('%d %b %Y, %H:%M UTC',time.gmtime()),
        }
        return {'farms':records,'villages':villages,'stats':stats}
    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback; traceback.print_exc()
        return None

def get_data():
    now = time.time()
    if _cache['data'] is None or (now - _cache['ts']) > CACHE_TTL:
        result = fetch_and_process()
        if result:
            _cache['data'] = result
            _cache['ts']   = now
    return _cache['data']

def fetch_raw_tables():
    """Fetch full raw tables via gspread for accurate row indices."""
    now = time.time()
    if _raw_cache['awd'] and (now - _raw_cache['ts']) < 120:
        return _raw_cache['awd'], _raw_cache['lib']

    def via_gspread(sheet_id, gid):
        ws = get_worksheet(sheet_id, gid)
        if not ws:
            return None
        all_vals = ws.get_all_values()
        if not all_vals:
            return None
        headers = all_vals[0]
        rows = []
        for i, row in enumerate(all_vals[1:], start=2):  # row index 2 = sheet row 2
            padded = row + [''] * (len(headers) - len(row))
            rows.append({'__row__': i, **dict(zip(headers, padded))})
        return {'columns': headers, 'rows': rows, 'count': len(rows)}

    def via_csv(url):
        try:
            raw = fetch_csv(url)
            raw = [{k.strip(): v for k, v in r.items()} for r in raw]
            if not raw: return None
            cols = list(raw[0].keys())
            for i, r in enumerate(raw, start=2):
                r['__row__'] = i
            return {'columns': cols, 'rows': raw, 'count': len(raw)}
        except Exception as e:
            logging.error(f"CSV fallback error: {e}")
            return None

    awd = via_gspread(AWD_SHEET_ID, AWD_GID) or via_csv(AWD_URL)
    lib = via_gspread(LIB_SHEET_ID, LIB_GID) or via_csv(LIB_URL)

    _raw_cache['awd'] = awd
    _raw_cache['lib'] = lib
    _raw_cache['ts']  = now
    return awd, lib

def warmup():
    time.sleep(2)
    get_data()
    get_gs_client()

threading.Thread(target=warmup, daemon=True).start()

def is_dev():
    return session.get('dev_user') in DEV_USERS

# ── Public routes ─────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/farms')
def farms():
    d = get_data(); return jsonify(d['farms'] if d else [])

@app.route('/api/villages')
def villages():
    d = get_data(); return jsonify(d['villages'] if d else [])

@app.route('/api/stats')
def stats():
    d = get_data(); return jsonify(d['stats'] if d else {})

@app.route('/api/refresh')
def refresh():
    _cache['ts'] = 0
    _raw_cache['ts'] = 0
    d = get_data()
    return jsonify({'ok':True,'farms':len(d['farms']) if d else 0})

@app.route('/health')
def health(): return jsonify({'status':'ok','ts':time.time()})

# ── Auth routes ───────────────────────────────────────────────────────────────
@app.route('/api/dev/login', methods=['POST'])
def dev_login():
    data = request.get_json(force=True)
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    if username in DEV_USERS and DEV_USERS[username] == _h(password):
        session['dev_user'] = username
        session.permanent = True
        return jsonify({'ok': True, 'username': username})
    return jsonify({'ok': False, 'error': 'Invalid credentials'}), 401

@app.route('/api/dev/logout', methods=['POST'])
def dev_logout():
    session.pop('dev_user', None)
    return jsonify({'ok': True})

@app.route('/api/dev/me')
def dev_me():
    if is_dev():
        return jsonify({'ok': True, 'username': session['dev_user']})
    return jsonify({'ok': False}), 401

# ── Raw table (read) ──────────────────────────────────────────────────────────
@app.route('/api/dev/raw_tables')
def dev_raw_tables():
    if not is_dev():
        return jsonify({'error': 'Unauthorized'}), 401
    awd, lib = fetch_raw_tables()
    return jsonify({
        'awd': awd or {'columns':[],'rows':[],'count':0},
        'lib': lib or {'columns':[],'rows':[],'count':0},
    })

# ── Cell update (write) ───────────────────────────────────────────────────────
@app.route('/api/dev/update_cell', methods=['POST'])
def update_cell():
    """Update a single cell in a Google Sheet.
    Body: { sheet: 'awd'|'lib', row: <int>, col: <str column name>, value: <str> }
    """
    if not is_dev():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    body = request.get_json(force=True)
    which   = body.get('sheet')     # 'awd' or 'lib'
    row_idx = body.get('row')       # 1-based sheet row number
    col_name= body.get('col')       # column header name
    value   = body.get('value', '')

    if which not in ('awd', 'lib') or not row_idx or not col_name:
        return jsonify({'ok': False, 'error': 'Missing params'}), 400

    sheet_id = AWD_SHEET_ID if which == 'awd' else LIB_SHEET_ID
    gid      = AWD_GID      if which == 'awd' else LIB_GID

    ws = get_worksheet(sheet_id, gid)
    if not ws:
        return jsonify({'ok': False, 'error': 'Could not connect to Google Sheets'}), 500

    try:
        # Find column letter by matching header
        headers = ws.row_values(1)
        if col_name not in headers:
            return jsonify({'ok': False, 'error': f'Column "{col_name}" not found'}), 400
        col_idx = headers.index(col_name) + 1  # 1-based

        ws.update_cell(row_idx, col_idx, value)

        # Bust caches
        _cache['ts']     = 0
        _raw_cache['ts'] = 0

        logging.info(f"Updated [{which}] row={row_idx} col={col_name} → '{value}' by {session.get('dev_user')}")
        return jsonify({'ok': True})
    except Exception as e:
        logging.error(f"update_cell error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── Add new row ───────────────────────────────────────────────────────────────
@app.route('/api/dev/add_row', methods=['POST'])
def add_row():
    """Append a new row to a sheet.
    Body: { sheet: 'awd'|'lib', data: { col_name: value, ... } }
    """
    if not is_dev():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    body  = request.get_json(force=True)
    which = body.get('sheet')
    data  = body.get('data', {})

    if which not in ('awd', 'lib'):
        return jsonify({'ok': False, 'error': 'Missing params'}), 400

    sheet_id = AWD_SHEET_ID if which == 'awd' else LIB_SHEET_ID
    gid      = AWD_GID      if which == 'awd' else LIB_GID

    ws = get_worksheet(sheet_id, gid)
    if not ws:
        return jsonify({'ok': False, 'error': 'Could not connect to Google Sheets'}), 500

    try:
        headers = ws.row_values(1)
        new_row = [data.get(h, '') for h in headers]
        ws.append_row(new_row, value_input_option='USER_ENTERED')
        _cache['ts']     = 0
        _raw_cache['ts'] = 0
        logging.info(f"Added row to [{which}] by {session.get('dev_user')}")
        return jsonify({'ok': True})
    except Exception as e:
        logging.error(f"add_row error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── Delete row ────────────────────────────────────────────────────────────────
@app.route('/api/dev/delete_row', methods=['POST'])
def delete_row():
    """Delete a row by its 1-based sheet row index.
    Body: { sheet: 'awd'|'lib', row: <int> }
    """
    if not is_dev():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    body    = request.get_json(force=True)
    which   = body.get('sheet')
    row_idx = body.get('row')

    if which not in ('awd', 'lib') or not row_idx:
        return jsonify({'ok': False, 'error': 'Missing params'}), 400

    sheet_id = AWD_SHEET_ID if which == 'awd' else LIB_SHEET_ID
    gid      = AWD_GID      if which == 'awd' else LIB_GID

    ws = get_worksheet(sheet_id, gid)
    if not ws:
        return jsonify({'ok': False, 'error': 'Could not connect to Google Sheets'}), 500

    try:
        ws.delete_rows(row_idx)
        _cache['ts']     = 0
        _raw_cache['ts'] = 0
        logging.info(f"Deleted row {row_idx} from [{which}] by {session.get('dev_user')}")
        return jsonify({'ok': True})
    except Exception as e:
        logging.error(f"delete_row error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
