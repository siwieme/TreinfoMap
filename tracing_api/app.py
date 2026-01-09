import os
import math
import heapq
import json
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_
from flask_cors import CORS

# --- USER IMPORTS (Assumed to exist in the target environment) ---
# from src.samenstelling import get_train_composition
# from src.lijn import get_lijnsectie_from_to, get_ptcarid_from_name, get_neighbouring_stations, get_location_from_station_name
# from src import gtfs
# -----------------------------------------------------------------

app = Flask(__name__)
# Enable CORS for everything
CORS(app, resources={r"/*": {"origins": "*"}})

# Configuration for the New Tracing Logic (Postgres)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
    'DATABASE_URL', 
    'postgresql://infrabel_user:infrabel_password@localhost:5432/infrabel_kaart'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
RAILWAY_GRAPH = {}

# ==========================================
# MODELS (New Tracing)
# ==========================================
class Train(db.Model):
    __tablename__ = 'trains'
    id = db.Column(db.Integer, primary_key=True)
    train_number = db.Column(db.String(20), index=True)
    stops = db.relationship('TrainStop', backref='train', lazy=True, order_by="TrainStop.stop_sequence")

class TrainStop(db.Model):
    __tablename__ = 'train_stops'
    id = db.Column(db.Integer, primary_key=True)
    train_id = db.Column(db.Integer, db.ForeignKey('trains.id'), nullable=False)
    stop_id = db.Column(db.String(50), index=True)
    stop_name = db.Column(db.String(200), index=True)
    stop_sequence = db.Column(db.Integer, index=True)

class InfrabelOperationalPoint(db.Model):
    __tablename__ = 'infrabel_operational_points'
    id = db.Column(db.String(50), primary_key=True)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)

class InfrabelStationToStation(db.Model):
    __tablename__ = 'infrabel_station_to_station'
    id = db.Column(db.Integer, primary_key=True)
    stationfrom_id = db.Column(db.String(50))
    stationto_id = db.Column(db.String(50))
    length = db.Column(db.Float)
    geom_wkt = db.Column(db.String) 

class StationMapping(db.Model):
    __tablename__ = 'station_mapping'
    sncb_id = db.Column(db.String(100), primary_key=True) 
    infrabel_id = db.Column(db.String(50), index=True) 

class StopTranslation(db.Model):
    __tablename__ = 'stop_translations'
    id = db.Column(db.Integer, primary_key=True)
    stop_id = db.Column(db.String(50), index=True)
    field_value = db.Column(db.String(200), index=True)
    translation = db.Column(db.String(200))

# ==========================================
# HELPERS (New Tracing)
# ==========================================
def get_infrabel_id(sncb_stop_id, stop_name=None):
    if not sncb_stop_id: return None
    
    if stop_name:
        lname = stop_name.lower()
        if 'kortrijk' in lname: return 'LK'
        if 'harelbeke' in lname: return 'FHL'
        if 'lokeren' in lname: return 'FLK'
        if 'zwijndrecht' in lname: return 'FZW'
        if 'bevers' in lname: return 'FBV'
        if 'antwerpen-centraal' in lname: return 'FN'
        if 'berchem' in lname: return 'FCV'
        if 'sint-niklaas' in lname: return 'FSN'
        if 'gent-sint-pieters' in lname: return 'FGSP'
        if 'gent-dampoort' in lname: return 'FGDM'
        if 'gentbrugge' in lname: return 'FUGE'

    cand = StationMapping.query.filter_by(sncb_id=sncb_stop_id).first()
    if cand: return cand.infrabel_id
    
    alt_id = sncb_stop_id[1:] if sncb_stop_id.startswith('S') else f"S{sncb_stop_id}"
    cand = StationMapping.query.filter_by(sncb_id=alt_id).first()
    if cand: return cand.infrabel_id
    
    clean_numeric = ''.join(filter(str.isdigit, sncb_stop_id))
    if len(clean_numeric) > 4:
        cand = StationMapping.query.filter(StationMapping.sncb_id.like(f"%{clean_numeric}%")).first()
        if cand: return cand.infrabel_id

    if stop_name:
        clean_name = stop_name.lower().replace('-', ' ').replace('/', ' ')
        trans = StopTranslation.query.filter(
            or_(
                StopTranslation.translation.ilike(f"%{clean_name}%"),
                StopTranslation.field_value.ilike(f"%{clean_name}%")
            )
        ).first()
        if trans and trans.stop_id != sncb_stop_id:
            return get_infrabel_id(trans.stop_id, None)
            
    return None

def build_railway_graph():
    global RAILWAY_GRAPH
    try:
        with app.app_context():
            print("🛠️  Building railway graph...")
            segments = InfrabelStationToStation.query.all()
            new_graph = {}
            for seg in segments:
                u, v, w = seg.stationfrom_id, seg.stationto_id, seg.length
                if w is None: w = 1.0 
                if u not in new_graph: new_graph[u] = []
                if v not in new_graph: new_graph[v] = []
                new_graph[u].append((v, w, seg.id))
                new_graph[v].append((u, w, seg.id))
            RAILWAY_GRAPH = new_graph
            print(f"✅ Graph built with {len(RAILWAY_GRAPH)} nodes.")
    except Exception as e:
        print(f"⚠️ Error building graph: {e}")

def find_path(start_node, end_node):
    if not RAILWAY_GRAPH: return None
    if start_node == end_node: return []
    if start_node not in RAILWAY_GRAPH or end_node not in RAILWAY_GRAPH: return None
    
    queue = [(0, start_node, [])]
    visited = {}
    
    while queue:
        (cost, node, path_segments) = heapq.heappop(queue)
        
        if node in visited and visited[node] <= cost:
            continue
        visited[node] = cost
        
        if node == end_node:
            return path_segments
            
        for (neighbor, weight, seg_id) in RAILWAY_GRAPH.get(node, []):
            if neighbor not in visited or visited[neighbor] > cost + weight:
                heapq.heappush(queue, (cost + weight, neighbor, path_segments + [seg_id]))
                
    return None

def get_pt_coords(pt_id):
    op = InfrabelOperationalPoint.query.filter_by(id=pt_id).first()
    if op and op.latitude and op.longitude:
        return [float(op.longitude), float(op.latitude)]
    return None

def get_trace_geometry(train_id, start_stop_id=None, end_stop_id=None):
    global RAILWAY_GRAPH
    if not RAILWAY_GRAPH:
        build_railway_graph()
        
    stops = TrainStop.query.filter_by(train_id=train_id).order_by(TrainStop.stop_sequence).all()
    if not stops: return None
    
    if start_stop_id or end_stop_id:
        filtered_stops = []
        in_range = False if start_stop_id else True
        for s in stops:
            if start_stop_id and s.stop_id == start_stop_id: in_range = True
            if in_range: filtered_stops.append(s)
            if end_stop_id and s.stop_id == end_stop_id: break
        stops = filtered_stops

    if len(stops) < 2: return None

    features = []
    
    resolved_stops = []
    for s in stops:
        inf_id = get_infrabel_id(s.stop_id, s.stop_name)
        if inf_id and inf_id in RAILWAY_GRAPH: 
            resolved_stops.append({"inf_id": inf_id, "stop": s})

    for i in range(len(resolved_stops) - 1):
        id1 = resolved_stops[i]["inf_id"]
        id2 = resolved_stops[i+1]["inf_id"]
        
        path_seg_ids = find_path(id1, id2)
        if path_seg_ids:
            for seg_id in path_seg_ids:
                seg = db.session.get(InfrabelStationToStation, seg_id)
                if seg:
                    try:
                        geom = json.loads(seg.geom_wkt)
                        features.append({
                            "type": "Feature",
                            "geometry": geom,
                            "properties": { "from_id": seg.stationfrom_id, "to_id": seg.stationto_id }
                        })
                    except: continue
        else:
            p1 = get_pt_coords(id1)
            p2 = get_pt_coords(id2)
            if p1 and p2:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [p1, p2]
                    },
                    "properties": { "from_id": id1, "to_id": id2, "fallback": True }
                })
        
    return {
        "type": "FeatureCollection",
        "features": features
    }

# ==========================================
# EXISTING ENDPOINTS (From User)
# ==========================================
@app.route('/logs', methods=['GET'])
def get_logs():
    with open('data/debug.log', 'r') as file:
        logs = file.readlines()
    return jsonify({"logs": [log.strip() for log in logs]}), 200

@app.route('/api/trein/samenstelling/', methods=['POST'])
def trein_samenstelling():
    data = request.get_json()
    from_station = data.get('from_station') if data else None
    to_station = data.get('to_station') if data else None
    trainnumber = data.get('trainnumber') if data else None
    date = data.get('date') if data else None
    lang = data.get('lang', 'en') if data else 'en'
    if not from_station or not to_station or not trainnumber or not date:
        return jsonify({"error": "Missing required parameters"}), 400
    
    # Placeholder for actual call
    try:
        from src.samenstelling import get_train_composition
        result = get_train_composition(from_station, to_station, trainnumber, date, lang)
        if result is None:
            return jsonify({"error": "Failed to retrieve train composition"}), 500
        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Source modules not found"}), 500

@app.route('/api/lijnsecties/', methods=['POST'])
def lijnsecties():
    data = request.get_json()
    from_station = data.get('from_station') if data else None
    to_station = data.get('to_station') if data else None
    if not from_station or not to_station:
        return jsonify({"error": "Missing required parameters"}), 400
    
    try:
        from src.lijn import get_ptcarid_from_name, get_lijnsectie_from_to
        ptcarfrom = get_ptcarid_from_name(from_station)
        ptcarto = get_ptcarid_from_name(to_station)
        if ptcarfrom is None or ptcarto is None:
            return jsonify({"error": "Invalid station names"}), 400
        result = get_lijnsectie_from_to(ptcarfrom, ptcarto)
        if result is None:
            return jsonify({"error": "Failed to retrieve lijnsecties"}), 500
        return jsonify(result)
    except ImportError:
         return jsonify({"error": "Source modules not found"}), 500

@app.route('/api/lijnsecties/stations/', methods=['POST'])
def lijnsecties_stations():
    data = request.get_json()
    station_name = data.get('station_name') if data else None
    if not station_name:
        return jsonify({"error": "Missing required parameter station_name"}), 400
    
    try:
        from src.lijn import get_location_from_station_name
        result = get_location_from_station_name(station_name)
        if result is None:
            return jsonify({"error": "Failed to retrieve location and neighbouring stations"}), 500
        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Source modules not found"}), 500

@app.route('/api/lijnsecties/ptcarid/', methods=['POST'])
def lijnsecties_ptcarid():
    data = request.get_json()
    ptcarid = data.get('ptcarid') if data else None
    if not ptcarid:
        return jsonify({"error": "Missing required parameter ptcarid"}), 400
    
    try:
        from src.lijn import get_neighbouring_stations
        result = get_neighbouring_stations(ptcarid)
        if result is None:
            return jsonify({"error": "Failed to retrieve neighbouring edges"}), 500
        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Source modules not found"}), 500

@app.route('/gtfs/static/', methods=['GET'])
def gtfs_static_endpoint():
    try:
        from src import gtfs
        gtfs.download_gtfs_static_met_perrons()
        gtfs.extract_gtfs_static_met_perrons()
        gtfs.load_gtfs_static_met_perrons()
        return jsonify({"status": "OK"}), 200
    except ImportError:
        return jsonify({"error": "Source modules not found"}), 500

# ==========================================
# NEW TRACING ENDPOINT
# ==========================================
@app.route('/api/trace/<int:train_id>')
def api_trace(train_id):
    """
    Returns the visual geometry (trace) for a train, using the PostGIS/internal graph logic.
    """
    start_stop = request.args.get('start')
    end_stop = request.args.get('end')
    try:
        geom = get_trace_geometry(train_id, start_stop, end_stop)
        if geom:
            return jsonify(geom)
        return jsonify({"error": "No trace data"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=9160)
