from flask import Flask, render_template, jsonify
import csv, io, os, time, threading, logging, requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

AWD_URL    = "https://docs.google.com/spreadsheets/d/180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k/export?format=csv&gid=1066902470"
AWD_UG_URL = "https://docs.google.com/spreadsheets/d/180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k/export?format=csv&gid=0"
LIB_URL    = "https://docs.google.com/spreadsheets/d/14ah-7Ah690oeOXE5vT8p701LYv7PiEMx_xZycNOOrSA/export?format=csv&gid=825105755"

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

def fetch_csv(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    return [row for row in reader]

def find_col(row, keywords):
    for k in keywords:
        for col in row.keys():
            if k.lower() in col.lower():
                return col
    return None

def parse_tw(s):
    if not s or s.strip() == '': return None, None
    parts = s.strip().split()
    try: return float(parts[0]), float(parts[1])
    except: return None, None

def parse_polygon(s):
    if not s: return []
    coords = []
    for seg in s.split(';'):
        seg = seg.strip()
        if not seg: continue
        parts = seg.split()
        if len(parts) >= 2:
            try: coords.append([float(parts[0]), float(parts[1])])
            except: pass
    return coords

def groupkey(g):
    if 'Group A' in g: return 'A'
    if 'Group B' in g: return 'B'
    return 'C'

def safe_float(v):
    try: return float(v) if v and v.strip() not in ('', 'nan', 'N. A', 'N.A') else 0
    except: return 0

def fetch_and_process():
    logging.info("Fetching from Google Sheets...")
    try:
        awd_rows = fetch_csv(AWD_URL)
        if not awd_rows: return None

        try: ug_rows = fetch_csv(AWD_UG_URL)
        except: ug_rows = []

        lib_rows = fetch_csv(LIB_URL)

        # Index libertalia by plot code
        sample = lib_rows[0] if lib_rows else {}
        plot_col = find_col(sample, ['plot code','plot_code','Plot code']) or list(sample.keys())[0]
        poly_col = find_col(sample, ['polygon','polygons'])
        tw_col   = find_col(sample, ['tw location','tw_location'])
        if not poly_col: poly_col = list(sample.keys())[-1]
        if not tw_col:   tw_col   = list(sample.keys())[-2]

        lib_index = {}
        for row in lib_rows:
            pid = row.get(plot_col, '').strip()
            if pid: lib_index[pid] = row

        # Index United Groups by farm id
        ug_index = {}
        if ug_rows:
            sample_ug = ug_rows[0]
            ug_id_col = find_col(sample_ug, ['farm id','farm_id'])
            comply_col_ug = find_col(sample_ug, ['complied'])
            pay_col_ug    = find_col(sample_ug, ['total payment','TOTAL Payment'])
            if ug_id_col:
                for row in ug_rows:
                    uid = row.get(ug_id_col, '').strip()
                    if uid: ug_index[uid] = row

        # Find AWD columns
        sample_awd = awd_rows[0]
        awd_id_col     = find_col(sample_awd, ['farm id','farm_id']) or list(sample_awd.keys())[2]
        farmer_col     = find_col(sample_awd, ['farmer name','farmer_name'])
        village_col    = find_col(sample_awd, ['village'])
        group_col      = find_col(sample_awd, ['groups','group'])
        zone_col       = find_col(sample_awd, ['study zone','zone'])
        acres_col      = find_col(sample_awd, ['farm acres','acres'])
        inc_col        = find_col(sample_awd, ['incentive acres'])
        phone_col      = find_col(sample_awd, ['phone'])

        records = []
        for row in awd_rows:
            fid = row.get(awd_id_col, '').strip()
            if not fid or fid not in lib_index: continue

            lrow = lib_index[fid]
            tw_lat, tw_lon = parse_tw(lrow.get(tw_col, ''))
            if tw_lat is None: continue

            ug = ug_index.get(fid, {})
            comply_col_name = find_col(ug, ['complied']) if ug else None
            pay_col_name    = find_col(ug, ['total payment']) if ug else None

            records.append({
                'farm_id':         fid,
                'farmer_name':     row.get(farmer_col, '') if farmer_col else '',
                'village':         row.get(village_col, '') if village_col else '',
                'group':           row.get(group_col, '') if group_col else '',
                'group_key':       groupkey(row.get(group_col, '') if group_col else ''),
                'zone':            row.get(zone_col, '') if zone_col else '',
                'acres':           safe_float(row.get(acres_col, 0) if acres_col else 0),
                'incentive_acres': safe_float(row.get(inc_col, 0) if inc_col else 0),
                'phone':           row.get(phone_col, '') if phone_col else '',
                'complied':        str(ug.get(comply_col_name, '')) if comply_col_name else '',
                'total_payment':   safe_float(ug.get(pay_col_name, 0) if pay_col_name else 0),
                'tw_lat':          tw_lat,
                'tw_lon':          tw_lon,
                'polygon':         parse_polygon(lrow.get(poly_col, ''))
            })

        # Village stats
        village_map = {}
        for f in records:
            v = f['village']
            if v not in village_map:
                village_map[v] = {'village':v,'total':0,'group_a':0,'group_b':0,'group_c':0,'total_acres':0,'incentive_acres':0}
            village_map[v]['total'] += 1
            village_map[v]['group_'+f['group_key'].lower()] += 1
            village_map[v]['total_acres'] += f['acres']
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
        n_vill   = len(villages)

        stats = {
            'total': total, 'group_a': group_a, 'group_b': group_b, 'group_c': group_c,
            'complied': complied, 'not_complied': not_comp,
            'total_acres': t_acres, 'incentive_acres': i_acres,
            'total_payment': 0, 'villages': n_vill,
            'last_updated': time.strftime('%d %b %Y, %H:%M UTC', time.gmtime())
        }

        logging.info(f"Loaded {total} farms, {n_vill} villages")
        return {'farms': records, 'villages': villages, 'stats': stats}

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
            _cache['ts'] = now
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
    return jsonify({'ok': True, 'farms': len(d['farms']) if d else 0})

@app.route('/health')
def health(): return jsonify({'status': 'ok', 'ts': time.time()})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
