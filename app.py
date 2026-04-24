from flask import Flask, render_template, jsonify
import pandas as pd
import io, os, time, threading, logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ── Google Sheets export URLs ──────────────────────────────────────────
# Make sure both sheets are set to "Anyone with the link can view"
AWD_URL      = "https://docs.google.com/spreadsheets/d/180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k/export?format=csv&gid=1669614800"
AWD_UG_URL   = "https://docs.google.com/spreadsheets/d/180I8ouxANsp9uszgHrKNfxMscA0LmHYlaOJzJsFn75k/export?format=csv&gid=0"
LIB_URL      = "https://docs.google.com/spreadsheets/d/14ah-7Ah690oeOXE5vT8p701LYv7PiEMx_xZycNOOrSA/export?format=csv&gid=0"

# ── Cache ──────────────────────────────────────────────────────────────
_cache = {"data": None, "ts": 0}
CACHE_TTL = 300  # seconds — refresh data every 5 minutes

def parse_tw(s):
    if pd.isna(s): return None, None
    parts = str(s).strip().split()
    try: return float(parts[0]), float(parts[1])
    except: return None, None

def parse_polygon(s):
    if pd.isna(s): return []
    coords = []
    for seg in str(s).split(';'):
        seg = seg.strip()
        if not seg: continue
        parts = seg.split()
        if len(parts) >= 2:
            try: coords.append([float(parts[0]), float(parts[1])])
            except: pass
    return coords

def groupkey(g):
    g = str(g)
    if 'Group A' in g: return 'A'
    if 'Group B' in g: return 'B'
    return 'C'

def fetch_and_process():
    logging.info("Fetching fresh data from Google Sheets...")
    try:
        awd = pd.read_csv(AWD_URL)
        awd.columns = [c.strip() for c in awd.columns]

        try:
            ug = pd.read_csv(AWD_UG_URL)
            ug.columns = [c.strip() for c in ug.columns]
        except Exception as e:
            logging.warning(f"Could not load United Groups: {e}")
            ug = pd.DataFrame()

        lib = pd.read_csv(LIB_URL, header=0)
        lib.columns = [c.strip() for c in lib.columns]

        # Identify Farm ID column in libertalia
        plot_col = None
        for c in lib.columns:
            if 'plot' in c.lower() or 'code' in c.lower():
                plot_col = c; break
        if not plot_col:
            plot_col = lib.columns[0]

        # Identify polygon and tw columns
        poly_col = None
        tw_col = None
        for c in lib.columns:
            cl = c.lower()
            if 'polygon' in cl: poly_col = c
            if 'tw' in cl and 'location' in cl: tw_col = c
        if not poly_col:
            # fallback: last columns
            poly_col = lib.columns[-1]
            tw_col   = lib.columns[-2]

        lib[plot_col] = lib[plot_col].astype(str).str.strip()

        # Find Farm ID column in AWD
        awd_id_col = None
        for c in awd.columns:
            if 'farm id' in c.lower() or 'farm_id' in c.lower():
                awd_id_col = c; break
        if not awd_id_col:
            awd_id_col = awd.columns[2]  # fallback position

        awd[awd_id_col] = awd[awd_id_col].astype(str).str.strip()

        merged = awd.merge(
            lib[[plot_col, poly_col, tw_col]],
            left_on=awd_id_col, right_on=plot_col, how='inner'
        )

        # Merge compliance from United Groups if available
        if not ug.empty:
            ug_id_col = None
            for c in ug.columns:
                if 'farm id' in c.lower(): ug_id_col = c; break
            if ug_id_col:
                ug[ug_id_col] = ug[ug_id_col].astype(str).str.strip()
                ug_cols = [ug_id_col]
                for c in ug.columns:
                    if 'complied' in c.lower() or 'total payment' in c.lower():
                        ug_cols.append(c)
                merged = merged.merge(ug[list(set(ug_cols))], left_on=awd_id_col, right_on=ug_id_col, how='left')

        merged['tw_lat'], merged['tw_lon'] = zip(*merged[tw_col].apply(parse_tw))
        merged['polygon_coords'] = merged[poly_col].apply(parse_polygon)
        merged = merged[merged['tw_lat'].notna()]

        # Find column names dynamically
        def find_col(df, keywords):
            for k in keywords:
                for c in df.columns:
                    if k.lower() in c.lower(): return c
            return None

        farmer_col   = find_col(merged, ['farmer name','farmer_name']) or merged.columns[3]
        village_col  = find_col(merged, ['village']) or merged.columns[4]
        group_col    = find_col(merged, ['groups','group']) or merged.columns[1]
        zone_col     = find_col(merged, ['study zone','zone'])
        acres_col    = find_col(merged, ['farm acres','acres'])
        inc_col      = find_col(merged, ['incentive acres'])
        phone_col    = find_col(merged, ['phone'])
        comply_col   = find_col(merged, ['complied'])
        payment_col  = find_col(merged, ['total payment'])

        records = []
        for _, r in merged.iterrows():
            records.append({
                'farm_id':        str(r[awd_id_col]),
                'farmer_name':    str(r[farmer_col]) if farmer_col else '',
                'village':        str(r[village_col]) if village_col else '',
                'group':          str(r[group_col]) if group_col else '',
                'group_key':      groupkey(r[group_col] if group_col else ''),
                'zone':           str(r[zone_col]) if zone_col and pd.notna(r.get(zone_col)) else '',
                'acres':          float(r[acres_col]) if acres_col and pd.notna(r.get(acres_col)) else 0,
                'incentive_acres':float(r[inc_col]) if inc_col and pd.notna(r.get(inc_col)) else 0,
                'phone':          str(r[phone_col]) if phone_col and pd.notna(r.get(phone_col)) else '',
                'complied':       str(r[comply_col]) if comply_col and pd.notna(r.get(comply_col)) else '',
                'total_payment':  float(r[payment_col]) if payment_col and pd.notna(r.get(payment_col)) else 0,
                'tw_lat':         r['tw_lat'],
                'tw_lon':         r['tw_lon'],
                'polygon':        r['polygon_coords']
            })

        # Village stats
        df = pd.DataFrame(records)
        village_stats = df.groupby('village').agg(
            total=('farm_id','count'),
            group_a=('group_key', lambda x: (x=='A').sum()),
            group_b=('group_key', lambda x: (x=='B').sum()),
            group_c=('group_key', lambda x: (x=='C').sum()),
            total_acres=('acres','sum'),
            incentive_acres=('incentive_acres','sum'),
        ).reset_index().sort_values('total', ascending=False)

        villages = village_stats.to_dict(orient='records')

        # Stats
        total     = len(records)
        group_a   = sum(1 for f in records if f['group_key'] == 'A')
        group_b   = sum(1 for f in records if f['group_key'] == 'B')
        group_c   = sum(1 for f in records if f['group_key'] == 'C')
        complied  = sum(1 for f in records if str(f.get('complied','')) == '1.0')
        not_comp  = sum(1 for f in records if str(f.get('complied','')) == '0.0')
        t_acres   = round(sum(f.get('acres',0) for f in records), 1)
        i_acres   = round(sum(f.get('incentive_acres',0) for f in records), 1)
        t_payment = round(sum(f.get('total_payment',0) for f in records), 0)
        n_villages= len(set(f['village'] for f in records))

        stats = {
            'total': total, 'group_a': group_a, 'group_b': group_b, 'group_c': group_c,
            'complied': complied, 'not_complied': not_comp,
            'total_acres': t_acres, 'incentive_acres': i_acres,
            'total_payment': t_payment, 'villages': n_villages,
            'last_updated': time.strftime('%d %b %Y, %H:%M UTC', time.gmtime())
        }

        logging.info(f"Data loaded: {total} farms, {n_villages} villages")
        return {'farms': records, 'villages': villages, 'stats': stats}

    except Exception as e:
        logging.error(f"Data fetch error: {e}")
        return None

def get_data():
    now = time.time()
    if _cache['data'] is None or (now - _cache['ts']) > CACHE_TTL:
        result = fetch_and_process()
        if result:
            _cache['data'] = result
            _cache['ts'] = now
    return _cache['data']

# Pre-warm cache on startup
def warmup():
    time.sleep(2)
    get_data()

threading.Thread(target=warmup, daemon=True).start()

# ── Routes ─────────────────────────────────────────────────────────────
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
    """Force a data refresh from Google Sheets"""
    _cache['ts'] = 0
    d = get_data()
    return jsonify({'ok': True, 'farms': len(d['farms']) if d else 0})

@app.route('/health')
def health():
    """Health check endpoint for UptimeRobot"""
    return jsonify({'status': 'ok', 'ts': time.time()})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
