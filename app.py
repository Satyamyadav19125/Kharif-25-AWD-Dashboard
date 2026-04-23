from flask import Flask, render_template, jsonify
import json, os

app = Flask(__name__)

def load_json(name):
    path = os.path.join(os.path.dirname(__file__), 'static', name)
    with open(path, encoding='utf-8') as f:
        return json.load(f)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/farms')
def farms():
    return jsonify(load_json('farm_records.json'))

@app.route('/api/villages')
def villages():
    return jsonify(load_json('village_stats.json'))

@app.route('/api/stats')
def stats():
    farms = load_json('farm_records.json')
    total = len(farms)
    group_a = sum(1 for f in farms if f['group_key'] == 'A')
    group_b = sum(1 for f in farms if f['group_key'] == 'B')
    group_c = sum(1 for f in farms if f['group_key'] == 'C')
    complied = sum(1 for f in farms if str(f.get('complied','')) == '1.0')
    not_complied = sum(1 for f in farms if str(f.get('complied','')) == '0.0')
    total_acres = sum(f.get('acres', 0) for f in farms)
    incentive_acres = sum(f.get('incentive_acres', 0) for f in farms)
    total_payment = sum(f.get('total_payment', 0) for f in farms)
    villages = len(set(f['village'] for f in farms))
    return jsonify({
        'total': total, 'group_a': group_a, 'group_b': group_b, 'group_c': group_c,
        'complied': complied, 'not_complied': not_complied,
        'total_acres': round(total_acres, 1),
        'incentive_acres': round(incentive_acres, 1),
        'total_payment': round(total_payment, 0),
        'villages': villages
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
