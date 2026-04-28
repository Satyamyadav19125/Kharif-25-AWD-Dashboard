from flask import Flask, render_template, jsonify
import csv, io, os, re, time, threading, logging, requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Published CSV URLs (no auth required)
AWD_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk1lqKuks4K3MXtX8cZAOSR-ESLK3D8NvTuKRVpWzNVunYYaUgqsBrasiSOKWl49LfPM2uTZwaW3UD/pub?gid=1066902470&single=true&output=csv"
LIB_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSmv6Sq_o-w_zH7gQQxQakTbjxbO95ORl995an6iEYEwN-SiLHEsTyUHMAjmaIT9NZGX5qndns_bQA3/pub?gid=2142859508&single=true&output=csv"

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

def fetch_csv(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, timeout=30, headers=headers)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))

def find_col(row_dict, keywords):
    for k in keywords:
        for col in row_dict:
            if k.lower() in col.lower():
                return col
    return None

def parse_simplified(s):
    """Parse 'lat lon alt 0.0; lat lon alt 0.0; ...' into [[lat,lon], ...]"""
    if not s: return []
    coords = []
    for seg in str(s).split(';'):
        parts = seg.strip().split()
        if len(parts) >= 2:
            try: coords.append([float(parts[0]), float(parts[1])])
            except: pass
    return coords

def groupkey(g):
    g = str(g).strip().upper()
    if 'GROUP A' in g or g == 'A': return 'A'
    if 'GROUP B' in g or g == 'B': return 'B'
    if 'GROUP C' in g or g == 'C': return 'C'
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
        # ── Fetch AWD sheet ──────────────────────────────────────────────
        awd_rows = fetch_csv(AWD_URL)
        if not awd_rows: return None
        awd_rows = [{k.strip(): v for k, v in row.items()} for row in awd_rows]
        logging.info(f"AWD rows: {len(awd_rows)}")

        # ── Fetch Libertalia master_control ──────────────────────────────
        lib_rows = fetch_csv(LIB_URL)
        if not lib_rows: return None
        lib_rows = [{k.strip(): v for k, v in row.items()} for row in lib_rows]
        logging.info(f"Libertalia rows: {len(lib_rows)}")
        logging.info(f"Libertalia columns: {list(lib_rows[0].keys())[:10]}")

        # ── Find AWD columns ─────────────────────────────────────────────
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
        logging.info(f"AWD cols: id={awd_id_col}, group={group_col}, comply={comply_col}")

        # ── Find Libertalia columns ──────────────────────────────────────
        ls = lib_rows[0]
        lib_id_col   = find_col(ls, ['plot code','farm id','farm_id'])
        lib_poly_col = find_col(ls, ['polygon','polygons'])
        lib_tw_col   = find_col(ls, ['tw location','tubewell location','tw_location'])
        logging.info(f"Lib cols: id={lib_id_col}, poly={lib_poly_col}, tw={lib_tw_col}")

        # ── Build geo index from master_control ──────────────────────────
        lib_index = {}
        for row in lib_rows:
            fid = str(row.get(lib_id_col, '') or '').strip()
            if not fid or fid.startswith('='): continue

            poly_str = str(row.get(lib_poly_col, '') or '')
            tw_str   = str(row.get(lib_tw_col, '') or '')

            poly = parse_simplified(poly_str)
            tw   = parse_simplified(tw_str)

            # TW location for map marker; fall back to polygon centroid
            if tw:
                tw_lat, tw_lon = tw[0][0], tw[0][1]
            elif poly:
                lats = [p[0] for p in poly]
                lons = [p[1] for p in poly]
                tw_lat = sum(lats)/len(lats)
                tw_lon = sum(lons)/len(lons)
            else:
                continue

            lib_index[fid] = {'tw_lat': tw_lat, 'tw_lon': tw_lon, 'polygon': poly}

        logging.info(f"Geo index: {len(lib_index)} farms")

        # ── Build farm records ───────────────────────────────────────────
        records = []
        skipped = 0
        for row in awd_rows:
            fid = str(row.get(awd_id_col, '') or '').strip()
            if not fid: continue
            if fid not in lib_index:
                skipped += 1
                continue
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

        # ── Village stats ────────────────────────────────────────────────
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

        # ── Stats ────────────────────────────────────────────────────────
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
        logging.info(f"Done: {total} farms | A={group_a} B={group_b} C={group_c} | complied={complied}")
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

def warmup():
    time.sleep(2)
    get_data()

threading.Thread(target=warmup, daemon=True).start()

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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
