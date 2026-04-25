from flask import Flask, render_template, jsonify
import csv, io, os, re, time, threading, logging, requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# AWD sheet - main farmer data (GID 1066902470)
AWD_URL = "https://docs.google.com/spreadsheets/d/180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k/export?format=csv&gid=1066902470"

# Libertalia rawfarm sheet - has WKT polygons + Kharif 25 Farm ID join key
# GID for rawfarm sheet (check Libertalia sheet tabs - rawfarm is the data sheet)
LIB_URL = "https://docs.google.com/spreadsheets/d/14ah-7Ah690oeOXE5vT8p701LYv7PiEMx_xZycNOOrSA/export?format=csv&gid=825105755"

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

def fetch_csv(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    return [row for row in reader]

def parse_wkt_polygon(wkt):
    """Parse POLYGON Z WKT into [[lat, lon], ...] list."""
    if not wkt or not wkt.strip().startswith('POLYGON'):
        return []
    coords = re.findall(r'([\d.]+)\s+([\d.]+)\s+[\d.]+', wkt)
    if not coords:
        return []
    return [[float(lat), float(lon)] for lon, lat in coords]

def centroid(polygon):
    """Return centroid lat, lon of a polygon."""
    if not polygon:
        return None, None
    lats = [p[0] for p in polygon]
    lons = [p[1] for p in polygon]
    return sum(lats) / len(lats), sum(lons) / len(lons)

def groupkey(g):
    g = g.strip().upper()
    if 'GROUP A' in g or g == 'A': return 'A'
    if 'GROUP B' in g or g == 'B': return 'B'
    if 'GROUP C' in g or g == 'C': return 'C'
    # fallback: look for standalone A/B/C
    if ' A ' in g or g.endswith(' A'): return 'A'
    if ' B ' in g or g.endswith(' B'): return 'B'
    return 'C'

def safe_float(v):
    try:
        return float(v) if v and str(v).strip() not in ('', 'nan', 'N. A', 'N.A') else 0
    except:
        return 0

def find_col(row_dict, keywords):
    """Find column key matching any keyword (case-insensitive)."""
    for k in keywords:
        for col in row_dict.keys():
            if k.lower() in col.lower():
                return col
    return None

def fetch_and_process():
    logging.info("Fetching from Google Sheets...")
    try:
        # ── Fetch AWD sheet ──────────────────────────────────────────────
        awd_rows = fetch_csv(AWD_URL)
        if not awd_rows:
            logging.error("AWD sheet returned no rows")
            return None
        logging.info(f"AWD rows fetched: {len(awd_rows)}")

        # Strip all column names (AWD sheet has leading spaces in Farm ID col)
        awd_rows = [{k.strip(): v for k, v in row.items()} for row in awd_rows]

        # ── Fetch Libertalia rawfarm sheet ───────────────────────────────
        lib_rows = fetch_csv(LIB_URL)
        logging.info(f"Libertalia rows fetched: {len(lib_rows)}")

        # ── Find column names in AWD ─────────────────────────────────────
        sample = awd_rows[0]
        awd_id_col      = find_col(sample, ['farm id', 'farm_id'])
        farmer_col      = find_col(sample, ['farmer name', 'farmer_name'])
        village_col     = find_col(sample, ['village'])
        group_col       = find_col(sample, ['groups', 'group'])
        zone_col        = find_col(sample, ['study zone', 'zone'])
        acres_col       = find_col(sample, ['farm acres', 'acres'])
        inc_col         = find_col(sample, ['incentive acres'])
        phone_col       = find_col(sample, ['phone'])
        comply_col      = find_col(sample, ['bank details verified', 'complied', 'consent signed'])

        logging.info(f"AWD cols: id={awd_id_col}, farmer={farmer_col}, village={village_col}, "
                     f"group={group_col}, acres={acres_col}, comply={comply_col}")

        # ── Build Libertalia index: FarmID → polygon + centroid ──────────
        lib_sample = lib_rows[0] if lib_rows else {}
        lib_id_col  = find_col(lib_sample, ['kharif 25 farm id', 'farm id', 'farm_id'])
        lib_wkt_col = find_col(lib_sample, ['wkt', 'polygon z', 'polygon'])

        # Fallback: if column names not found, use positional (col 4 = Farm ID, col 1 = WKT)
        if not lib_id_col:
            keys = list(lib_sample.keys())
            lib_id_col = keys[4] if len(keys) > 4 else None
        if not lib_wkt_col:
            keys = list(lib_sample.keys())
            lib_wkt_col = keys[1] if len(keys) > 1 else None

        logging.info(f"Libertalia cols: id={lib_id_col}, wkt={lib_wkt_col}")

        lib_index = {}
        for row in lib_rows:
            fid = row.get(lib_id_col, '').strip() if lib_id_col else ''
            wkt = row.get(lib_wkt_col, '').strip() if lib_wkt_col else ''
            if not fid or fid.startswith('='):
                continue
            poly = parse_wkt_polygon(wkt)
            if not poly:
                continue
            clat, clon = centroid(poly)
            if clat is None:
                continue
            lib_index[fid] = {'polygon': poly, 'tw_lat': clat, 'tw_lon': clon}

        logging.info(f"Libertalia farms indexed: {len(lib_index)}")

        # ── Build farm records ───────────────────────────────────────────
        records = []
        skipped_no_match = 0
        for row in awd_rows:
            fid = row.get(awd_id_col, '').strip() if awd_id_col else ''
            if not fid:
                continue
            if fid not in lib_index:
                skipped_no_match += 1
                continue

            geo = lib_index[fid]
            group_val = row.get(group_col, '') if group_col else ''
            comply_val = row.get(comply_col, '') if comply_col else ''

            # Normalise compliance: 1.0 / 0.0 / ''
            if comply_val.strip() in ('1', '1.0', 'yes', 'Yes', 'YES', 'Y', 'y'):
                comply_norm = '1.0'
            elif comply_val.strip() in ('0', '0.0', 'no', 'No', 'NO', 'N', 'n'):
                comply_norm = '0.0'
            else:
                comply_norm = comply_val.strip()

            records.append({
                'farm_id':         fid,
                'farmer_name':     row.get(farmer_col, '') if farmer_col else '',
                'village':         row.get(village_col, '') if village_col else '',
                'group':           group_val,
                'group_key':       groupkey(group_val),
                'zone':            row.get(zone_col, '') if zone_col else '',
                'acres':           safe_float(row.get(acres_col, 0) if acres_col else 0),
                'incentive_acres': safe_float(row.get(inc_col, 0) if inc_col else 0),
                'phone':           row.get(phone_col, '') if phone_col else '',
                'complied':        comply_norm,
                'total_payment':   0,
                'tw_lat':          geo['tw_lat'],
                'tw_lon':          geo['tw_lon'],
                'polygon':         geo['polygon'],
            })

        logging.info(f"Records built: {len(records)}, skipped (no geo match): {skipped_no_match}")

        # ── Village stats ────────────────────────────────────────────────
        village_map = {}
        for f in records:
            v = f['village']
            if v not in village_map:
                village_map[v] = {
                    'village': v, 'total': 0,
                    'group_a': 0, 'group_b': 0, 'group_c': 0,
                    'total_acres': 0, 'incentive_acres': 0
                }
            village_map[v]['total'] += 1
            village_map[v]['group_' + f['group_key'].lower()] += 1
            village_map[v]['total_acres'] += f['acres']
            village_map[v]['incentive_acres'] += f['incentive_acres']
        villages = sorted(village_map.values(), key=lambda x: -x['total'])

        # ── Stats ────────────────────────────────────────────────────────
        total    = len(records)
        group_a  = sum(1 for f in records if f['group_key'] == 'A')
        group_b  = sum(1 for f in records if f['group_key'] == 'B')
        group_c  = sum(1 for f in records if f['group_key'] == 'C')
        complied = sum(1 for f in records if f['complied'] == '1.0')
        not_comp = sum(1 for f in records if f['complied'] == '0.0')
        t_acres  = round(sum(f['acres'] for f in records), 1)
        i_acres  = round(sum(f['incentive_acres'] for f in records), 1)

        stats = {
            'total': total, 'group_a': group_a, 'group_b': group_b, 'group_c': group_c,
            'complied': complied, 'not_complied': not_comp,
            'total_acres': t_acres, 'incentive_acres': i_acres,
            'total_payment': 0, 'villages': len(villages),
            'last_updated': time.strftime('%d %b %Y, %H:%M UTC', time.gmtime())
        }

        logging.info(f"Done — {total} farms, {len(villages)} villages, {complied} complied")
        return {'farms': records, 'villages': villages, 'stats': stats}

    except Exception as e:
        logging.error(f"Error in fetch_and_process: {e}")
        import traceback; traceback.print_exc()
        return None

# ── Cache + warmup ───────────────────────────────────────────────────────────
def get_data():
    now = time.time()
    if _cache['data'] is None or (now - _cache['ts']) > CACHE_TTL:
        result = fetch_and_process()
        if result:
            _cache['data'] = result
            _cache['ts'] = now
    return _cache['data']

def warmup():
    time.sleep(2)
    get_data()

threading.Thread(target=warmup, daemon=True).start()

# ── Routes ───────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/farms')
def farms():
    d = get_data()
    return jsonify(d['farms'] if d else [])

@app.route('/api/villages')
def villages():
    d = get_data()
    return jsonify(d['villages'] if d else [])

@app.route('/api/stats')
def stats():
    d = get_data()
    return jsonify(d['stats'] if d else {})

@app.route('/api/refresh')
def refresh():
    _cache['ts'] = 0
    d = get_data()
    return jsonify({'ok': True, 'farms': len(d['farms']) if d else 0})

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'ts': time.time()})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
