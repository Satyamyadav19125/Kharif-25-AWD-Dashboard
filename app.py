from flask import Flask, render_template, jsonify, request, session
import csv, io, os, re, time, threading, logging, requests, hashlib

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'awd-dashboard-secret-2025')
logging.basicConfig(level=logging.INFO)

# ── Published /e/2PACX-... URLs ───────────────────────────────────────────────
AWD_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRk1lqKuks4K3MXtX8cZA0SR-ESLK3D8NvTuKRVpWzNVunYYaUgqsBrasiSOKWl49LfPM2uTZwaW3UD"
    "/pub?gid=1066902470&single=true&output=csv"
)
LIB_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSmv6Sq_o-w_zH7gQQxQakTbjxbO95ORl995an6iEYEwN-SiLHEsTyUHMAjmaIT9NZGX5qndns_bQA3"
    "/pub?gid=2142859508&single=true&output=csv"
)

# ── Dev credentials (SHA-256 hashed) ─────────────────────────────────────────
def _h(s): return hashlib.sha256(s.encode()).hexdigest()

DEV_USERS = {
    "Satyamyadav19125": _h("finnwolfhard@666"),
    "Danetgar":         _h("Etgardan"),
}

_cache     = {"data": None, "ts": 0}
_raw_cache = {"awd": None, "lib": None, "ts": 0}
CACHE_TTL  = 60

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

def fetch_and_process():
    logging.info("Fetching from Google Sheets...")
    try:
        awd_rows = fetch_csv(AWD_URL)
        if not awd_rows: return None
        awd_rows = [{k.strip(): v for k, v in row.items()} for row in awd_rows]
        logging.info(f"AWD rows: {len(awd_rows)}")

        lib_rows = fetch_csv(LIB_URL)
        lib_rows = [{k.strip(): v for k, v in row.items()} for row in lib_rows]
        logging.info(f"Libertalia rows: {len(lib_rows)}")

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
        logging.info(f"AWD cols → id={awd_id_col} group={group_col} comply={comply_col}")

        ls = lib_rows[0] if lib_rows else {}
        lib_keys = list(ls.keys())
        lib_id_col   = find_col(ls, ['plot code','farm id','farm_id']) or (lib_keys[0] if lib_keys else None)
        lib_poly_col = find_col(ls, ['polygon','polygons'])
        lib_tw_col   = find_col(ls, ['tw location','tw_location'])
        lib_wkt_col  = find_col(ls, ['wkt'])
        lib_simp_col = find_col(ls, ['simplified'])
        if not lib_wkt_col  and len(lib_keys) > 1: lib_wkt_col  = lib_keys[1]
        if not lib_simp_col and len(lib_keys) > 2: lib_simp_col = lib_keys[2]
        logging.info(f"Lib cols → id={lib_id_col} poly={lib_poly_col} tw={lib_tw_col}")

        lib_index = {}
        for row in lib_rows:
            poly = []
            tw_lat, tw_lon = None, None
            if lib_poly_col:
                poly = parse_simplified_polygon(row.get(lib_poly_col, ''))
            if lib_tw_col:
                tw_lat, tw_lon = parse_tw_location(row.get(lib_tw_col, ''))
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
            if fid1 and not fid1.startswith('='):
                lib_index[fid1] = geo
            lib_id2_col = lib_keys[10] if len(lib_keys) > 10 else None
            fid2 = extract_fallback_id(row.get(lib_id2_col, '')) if lib_id2_col else None
            if fid2 and fid2 != fid1:
                lib_index[fid2] = geo

        logging.info(f"Geo index: {len(lib_index)} farms")

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

        logging.info(f"Records: {len(records)}, skipped: {skipped}")

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
        t_acres  = round(sum(f['acres']           for f in records), 1)
        i_acres  = round(sum(f['incentive_acres'] for f in records), 1)

        stats = {
            'total':total,'group_a':group_a,'group_b':group_b,'group_c':group_c,
            'complied':complied,'not_complied':not_comp,
            'total_acres':t_acres,'incentive_acres':i_acres,
            'total_payment':0,'villages':len(villages),
            'last_updated':time.strftime('%d %b %Y, %H:%M UTC',time.gmtime()),
        }
        logging.info(f"Done: {total} farms | A={group_a} B={group_b} C={group_c}")
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
    """Fetch full raw CSV tables for developer view."""
    now = time.time()
    if _raw_cache['awd'] and (now - _raw_cache['ts']) < 300:
        return _raw_cache['awd'], _raw_cache['lib']
    try:
        awd_rows = fetch_csv(AWD_URL)
        awd_rows = [{k.strip(): v for k, v in row.items()} for row in awd_rows]
    except Exception as e:
        logging.error(f"Raw AWD fetch: {e}"); awd_rows = []
    try:
        lib_rows = fetch_csv(LIB_URL)
        lib_rows = [{k.strip(): v for k, v in row.items()} for row in lib_rows]
    except Exception as e:
        logging.error(f"Raw LIB fetch: {e}"); lib_rows = []
    _raw_cache['awd'] = awd_rows
    _raw_cache['lib'] = lib_rows
    _raw_cache['ts']  = now
    return awd_rows, lib_rows

def warmup():
    time.sleep(2)
    get_data()

threading.Thread(target=warmup, daemon=True).start()

# ── Auth helpers ──────────────────────────────────────────────────────────────
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
    d = get_data()
    return jsonify({'ok':True,'farms':len(d['farms']) if d else 0})

@app.route('/health')
def health(): return jsonify({'status':'ok','ts':time.time()})

# ── Developer auth routes ─────────────────────────────────────────────────────
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

@app.route('/api/dev/raw_tables')
def dev_raw_tables():
    if not is_dev():
        return jsonify({'error': 'Unauthorized'}), 401
    awd_rows, lib_rows = fetch_raw_tables()
    awd_cols = list(awd_rows[0].keys()) if awd_rows else []
    lib_cols = list(lib_rows[0].keys()) if lib_rows else []
    return jsonify({
        'awd': {'columns': awd_cols, 'rows': awd_rows, 'count': len(awd_rows)},
        'lib': {'columns': lib_cols, 'rows': lib_rows, 'count': len(lib_rows)},
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
