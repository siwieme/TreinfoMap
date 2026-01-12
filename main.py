import os
import base64
import threading
import re
import time
import pandas as pd
import requests
import zipfile
import io
import locale
from datetime import datetime, timedelta
import shutil
import sqlite3
from flask import Flask, render_template, render_template_string, request, redirect, url_for, session, jsonify, make_response, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, and_, event, Engine, UniqueConstraint, text
from sqlalchemy.orm import aliased
from google.transit import gtfs_realtime_pb2
import json
import heapq
import werkzeug.security
import math
import secrets
import uuid
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

RAILWAY_GRAPH = {}

# ==========================================
# CONFIGURATIE
# ==========================================
DATA_FOLDER = "gtfs_data" 
BACKUP_FOLDER = "rawdata_TC_backup"
REALTIME_URL = "https://sncb-opendata.hafas.de/gtfs/realtime/d22ad6759ee25bg84ddb6c818g4dc4de_TC"
STATIC_URL = "https://sncb-opendata.hafas.de/gtfs/static/d22ad6759ee25bg84ddb6c818g4dc4de_TC"

API_DELAY_SECONDS = 1.5  
MAX_API_CALLS_PER_RUN = 100 

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'trein_secret_key_v10')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
    'DATABASE_URL', 
    'postgresql://infrabel_user:infrabel_password@localhost:5432/infrabel_kaart'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Session Policy
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=False, # Set to True if using HTTPS
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
    SESSION_COOKIE_NAME='treinfo_session'
)

db = SQLAlchemy(app)

static_data = {}
LAST_UPDATE_TIMESTAMP = None 

# Per-train lock for composition fetching to avoid SQL Errors during concurrent requests
COMPOSITION_LOCKS = {}
COMPOSITION_LOCKS_LOCK = threading.Lock()

# ==========================================
# 1. VERTALINGEN (i18n)
# ==========================================
from translations import TRANSLATIONS

def get_locale():
    if 'lang' in session: return session['lang']
    return request.accept_languages.best_match(['nl', 'fr', 'en', 'de']) or 'nl'

def t(key):
    lang = get_locale()
    return TRANSLATIONS.get(lang, TRANSLATIONS['nl']).get(key, key)

@app.context_processor
def inject_translations():
    return {'t': t, 'get_locale': get_locale}

# ==========================================
# 2. DATABASE MODELLEN
# ==========================================
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)
    reset_token = db.Column(db.String(100), unique=True, nullable=True)
    reset_expiry = db.Column(db.DateTime, nullable=True)
    is_verified = db.Column(db.Boolean, default=False)
    verification_token = db.Column(db.String(100), unique=True, nullable=True)

class Train(db.Model):
    __tablename__ = 'trains'
    id = db.Column(db.Integer, primary_key=True)
    train_number = db.Column(db.String(20), index=True)
    date = db.Column(db.String(20), index=True) 
    trip_id = db.Column(db.String(100), index=True)
    route_name = db.Column(db.String(200))
    destination = db.Column(db.String(100))
    departure_time = db.Column(db.String(20))
    arrival_time = db.Column(db.String(20))
    status = db.Column(db.String(20), default="SCHEDULED")
    delay = db.Column(db.Integer, default=0)
    realtime_trip_id = db.Column(db.String(100), index=True, nullable=True)
    composition_fetched = db.Column(db.Boolean, default=False)
    has_composition_data = db.Column(db.Boolean, default=False)
    
    units = db.relationship('TrainUnit', backref='train', lazy=True, cascade="all, delete-orphan", order_by="TrainUnit.position")
    stops = db.relationship('TrainStop', backref='train', lazy=True, cascade="all, delete-orphan", order_by="TrainStop.stop_sequence")

    __table_args__ = (UniqueConstraint('train_number', 'date', name='_train_number_date_uc'),)

class TrainUnit(db.Model):
    __tablename__ = 'train_units'
    id = db.Column(db.Integer, primary_key=True)
    train_id = db.Column(db.Integer, db.ForeignKey('trains.id'), nullable=False)
    position = db.Column(db.String(10))
    material_type = db.Column(db.String(50), index=True)
    material_number = db.Column(db.String(50), index=True)
    parent_type = db.Column(db.String(50))
    sub_type = db.Column(db.String(10))
    traction_type = db.Column(db.String(10)) # tractionType from iRail (e.g. HLE)
    orientation = db.Column(db.String(20)) # LEFT, RIGHT
    idx_in_seg = db.Column(db.Integer, default=0)
    has_bike = db.Column(db.Boolean, default=False)
    has_airco = db.Column(db.Boolean, default=False)

class TrainStop(db.Model):
    __tablename__ = 'train_stops'
    id = db.Column(db.Integer, primary_key=True)
    train_id = db.Column(db.Integer, db.ForeignKey('trains.id'), nullable=False)
    stop_id = db.Column(db.String(50), index=True)
    stop_name = db.Column(db.String(200), index=True)
    stop_type = db.Column(db.String(20)) # STOP, VERTREK, AANKOMST, DOORRIT
    arrival_time = db.Column(db.String(20))
    departure_time = db.Column(db.String(20))
    stop_sequence = db.Column(db.Integer, index=True)

    __table_args__ = (
        UniqueConstraint('train_id', 'stop_sequence', name='_train_stop_seq_uc'),
    )

class Journey(db.Model):
    __tablename__ = 'journeys'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    train_id = db.Column(db.Integer, db.ForeignKey('trains.id'))
    
    # Selection 1
    unit_id = db.Column(db.Integer, db.ForeignKey('train_units.id', ondelete='SET NULL'), nullable=True)
    material_type = db.Column(db.String(50))
    material_number = db.Column(db.String(50))
    parent_type = db.Column(db.String(50))
    sub_type = db.Column(db.String(10))
    orientation = db.Column(db.String(20))

    # Selection 2
    unit_id_2 = db.Column(db.Integer, db.ForeignKey('train_units.id', ondelete='SET NULL'), nullable=True)
    material_type_2 = db.Column(db.String(50))
    material_number_2 = db.Column(db.String(50))
    parent_type_2 = db.Column(db.String(50))
    sub_type_2 = db.Column(db.String(10))
    orientation_2 = db.Column(db.String(20))

    # Selection 3
    unit_id_3 = db.Column(db.Integer, db.ForeignKey('train_units.id', ondelete='SET NULL'), nullable=True)
    material_type_3 = db.Column(db.String(50))
    material_number_3 = db.Column(db.String(50))
    parent_type_3 = db.Column(db.String(50))
    sub_type_3 = db.Column(db.String(10))
    orientation_3 = db.Column(db.String(20))

    # Selection 4
    unit_id_4 = db.Column(db.Integer, db.ForeignKey('train_units.id', ondelete='SET NULL'), nullable=True)
    material_type_4 = db.Column(db.String(50))
    material_number_4 = db.Column(db.String(50))
    parent_type_4 = db.Column(db.String(50))
    sub_type_4 = db.Column(db.String(10))
    orientation_4 = db.Column(db.String(20))

    start_stop_id = db.Column(db.String(50))
    end_stop_id = db.Column(db.String(50))
    train_number = db.Column(db.String(20))
    destination = db.Column(db.String(100))
    distance_km = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class InfrabelOperationalPoint(db.Model):
    __tablename__ = 'infrabel_operational_points'
    id = db.Column(db.String(50), primary_key=True)
    ptcar_id = db.Column(db.String(20), index=True) # The numeric ID used in segments
    name_nl = db.Column(db.String(200))
    name_fr = db.Column(db.String(200))
    bts_code = db.Column(db.String(20))
    taf_tap_code = db.Column(db.String(20))
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
    name = db.Column(db.String(200))

class SystemStatus(db.Model):
    __tablename__ = 'system_status'
    key = db.Column(db.String(50), primary_key=True)
    updated_at = db.Column(db.DateTime)

class StopTranslation(db.Model):
    __tablename__ = 'stop_translations'
    id = db.Column(db.Integer, primary_key=True)
    stop_id = db.Column(db.String(50), index=True)
    field_value = db.Column(db.String(200), index=True)
    lang = db.Column(db.String(5))
    translation = db.Column(db.String(200))

# ==========================================
# 3. HELPER: TRACING & MAPPING
# ==========================================
def get_infrabel_id(sncb_stop_id, stop_name=None):
    """Maps sncb_id -> infrabel_id more robustly."""
    if not sncb_stop_id: return None
    
    # HARDCODED MISSING HUBS (Data cleanup)
    if stop_name:
        lname = stop_name.lower()
        if 'lokeren' in lname: return 'FLK'
        if 'zwijndrecht' in lname: return 'FZW'
        if 'bevers' in lname: return 'FBV' # Beveren
        if 'antwerpen-centraal' in lname: return 'FN'
        if 'berchem' in lname: return 'FCV' # Antwerp-Berchem
        if 'sint-niklaas' in lname: return 'FSN'
        if 'gent-sint-pieters' in lname: return 'FGSP'
        if 'gent-dampoort' in lname: return 'FGDM'
        if 'gentbrugge' in lname: return 'FUGE'
        if 'gentbrugge' in lname: return 'FUGE'
        # HSL FIX: Map Leuven to FLV (Leuven Vorming/Main HSL Node) instead of FL (Broken)
        if 'leuven' in lname and 'heverlee' not in lname: return 'FLV'
        
        # PEPINSTER FIX: Ensure Pepinster maps to FPS, not FPSC (Cite)
        if 'pepinster' in lname and 'cit' not in lname: return 'FPS'

    # 1. Direct try
    cand = StationMapping.query.filter_by(sncb_id=sncb_stop_id).first()
    if cand: 
        return cand.infrabel_id
    
    # 2. Try with/without 'S' prefix
    alt_id = sncb_stop_id[1:] if sncb_stop_id.startswith('S') else f"S{sncb_stop_id}"
    cand = StationMapping.query.filter_by(sncb_id=alt_id).first()
    if cand: return cand.infrabel_id
    
    # 3. Try splitting old SNCB:123:0 format
    clean_numeric = ''.join(filter(str.isdigit, sncb_stop_id))
    # We expect at least a decent length for an ID
    if len(clean_numeric) > 4:
        cand = StationMapping.query.filter(StationMapping.sncb_id.like(f"%{clean_numeric}%")).first()
        if cand: return cand.infrabel_id

    # 4. Name Fallback (Last Resort)
    if stop_name:
        clean_name = stop_name.lower().replace('-', ' ').replace('/', ' ')
        trans = StopTranslation.query.filter(
            or_(
                StopTranslation.translation.ilike(f"%{clean_name}%"),
                StopTranslation.field_value.ilike(f"%{clean_name}%")
            )
        ).first()
        if trans and trans.stop_id != sncb_stop_id:
            return get_infrabel_id(trans.stop_id, None) # Recursion without name to avoid infinite loop
            
    return None

def build_railway_graph():
    """Builds an adjacency list from InfrabelStationToStation segments using Haversine weights."""
    global RAILWAY_GRAPH
    try:
        with app.app_context():
            print("🛠️  Building railway graph...")
            
            # Load Coordinates
            all_ops = InfrabelOperationalPoint.query.all()
            coords = {op.id: (op.latitude, op.longitude) for op in all_ops if op.latitude and op.longitude}
            
            segments = InfrabelStationToStation.query.all()
            new_graph = {}
            
            def haversine(lat1, lon1, lat2, lon2):
                R = 6371.0 # Radius of Earth in km
                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)
                a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                return R * c

            for seg in segments:
                u, v = seg.stationfrom_id, seg.stationto_id
                
                # Calculate Weight
                w = seg.length
                
                # If length is missing or suspiciously 1.0 (default), try Haversine
                if w is None:
                    if u in coords and v in coords:
                        w = haversine(coords[u][0], coords[u][1], coords[v][0], coords[v][1])
                    else:
                        w = 1.0 # Last resort fallback
                
                if w is None: w = 1.0 # Double check
                
                # --- MANUAL WEIGHT ADJUSTMENT ---

                # BLOCK BAD DATA: Segment 875/1319 claims to be FL->ANS with 5km len. 
                # Seg 321/1249 claims FL->LGR (Liège) with 5km len.
                # Seg 1305/1310 claims FL->GVX with 16km (Voroux teleport).
                # This breaks graph logic. Block it.
                bad_segs = [321, 1249, 1305, 1310]
                if seg.id in bad_segs:
                    continue # Skip adding this edge entirely

                # HSL FIX: Penalize Classic Line Landen-Ans - REMOVED
                # avoid_nodes = ['FWR', 'FLD', 'FRM'] 
                # if u in avoid_nodes or v in avoid_nodes:
                #      w *= 2000.0
                
                # FAVOR HSL: ANS <-> FLV (Leuven Vorming/HSL Start)
                # This segment is ~66km. We slightly favor it to ensure selection over penalized classic.
                hsl_nodes = ['FLV', 'ANS']
                if u in hsl_nodes and v in hsl_nodes:
                    w *= 0.8 # Slight preference
                
                # AIRPORT FIX: Force Direct trains via Diabolo (Top) not Classic (Bottom)
                # The "bottom" route goes via Diegem (FDG) and Zaventem (FZA).
                # We penalize these edges so DIRECT trains choose the "top" path.
                # Stopping trains (FN->FDG) will still use them as it's the only way.
                classic_airport_nodes = ['FDG', 'FZA']
                if u in classic_airport_nodes or v in classic_airport_nodes:
                   w *= 5.0

                if (u=='FTNN' and v=='FLD') or (v=='FTNN' and u=='FLD'):
                    w *= 1000.0

                # SPA BRANCH FIX: Penalize entering the Spa branch unless necessary
                # This prevents mainline trains (Liege-Verviers) from taking the "dip" via Pepinster-Cite (FPSC).
                spa_nodes = ['FPSC', 'FSS', 'FSSG', 'FJL', 'FTX', 'FRO']
                if u in spa_nodes or v in spa_nodes:
                    w *= 10.0

                if u not in new_graph: new_graph[u] = []
                if v not in new_graph: new_graph[v] = []
                new_graph[u].append((v, w, seg.id))
                new_graph[v].append((u, w, seg.id))

            # --- VIRTUAL EXTREME FIX FOR AIRPORT (FBNL) ---
            # Ensure connectivity to avoiding reversals
            virtual_edges = [
                ('FBNL', 'FM', 0.1, 'V_FBNL_FM'),
                ('FBNL', 'FN', 0.1, 'V_FBNL_FN'),
                ('FBNL', 'FLV', 0.1, 'V_FBNL_FLV')
            ]
            for u, v, w, vid in virtual_edges:
                if u not in new_graph: new_graph[u] = []
                if v not in new_graph: new_graph[v] = []
                new_graph[u].append((v, w, vid))
                new_graph[v].append((u, w, vid))

            RAILWAY_GRAPH = new_graph
            print(f"✅ Graph built with {len(RAILWAY_GRAPH)} nodes.")
    except Exception as e:
        print(f"⚠️ Error building graph: {e}")

def find_path(start_node, end_node):
    """Dijkstra to find the shortest path between two Infrabel IDs."""
    if not RAILWAY_GRAPH: return None
    if start_node == end_node: return []
    if start_node not in RAILWAY_GRAPH or end_node not in RAILWAY_GRAPH: return None
    
    # queue: (cost, current_node, [segment_ids])
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
    """Fallback to get lat/lon for an operational point."""
    op = InfrabelOperationalPoint.query.filter_by(id=pt_id).first()
    if op and op.latitude and op.longitude:
        return [float(op.longitude), float(op.latitude)]
    return None

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000  # Radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def get_journey_distance(train_id, start_stop_id, end_stop_id):
    """Calculates total distance in KM for a journey segment."""
    global RAILWAY_GRAPH
    if not RAILWAY_GRAPH:
        build_railway_graph()
    
    print(f"📏 Distance Calc: Train {train_id} | {start_stop_id} to {end_stop_id}")
        
    stops = TrainStop.query.filter_by(train_id=train_id).order_by(TrainStop.stop_sequence).all()
    if not stops: return 0.0
    
    filtered_stops = []
    in_range = False if not start_stop_id else False
    for s in stops:
        if start_stop_id and s.stop_id == start_stop_id: in_range = True
        elif not start_stop_id: in_range = True
        
        if in_range: filtered_stops.append(s)
        if end_stop_id and s.stop_id == end_stop_id: break
    
    if len(filtered_stops) < 2: return 0.0

    total_dist = 0.0
    resolved_pts = []
    for s in filtered_stops:
        inf_id = get_infrabel_id(s.stop_id, s.stop_name)
        if inf_id and inf_id in RAILWAY_GRAPH:
            resolved_pts.append(inf_id)

    for i in range(len(resolved_pts) - 1):
        path_seg_ids = find_path(resolved_pts[i], resolved_pts[i+1])
        if path_seg_ids:
            for seg_id in path_seg_ids:
                seg = db.session.get(InfrabelStationToStation, seg_id)
                if seg and seg.length:
                    total_dist += float(seg.length)
                else:
                    total_dist += 0.1 # nominal 100m fallback
        else:
            # Fallback to straight-line haversine distance if no path found
            p1 = get_pt_coords(resolved_pts[i])
            p2 = get_pt_coords(resolved_pts[i+1])
            if p1 and p2:
                dist_m = haversine_distance(p1[1], p1[0], p2[1], p2[0])
                total_dist += dist_m / 1000.0
            else:
                total_dist += 1.0 # Last resort fallback
                
    return round(total_dist, 2)

def get_trace_geometry(train_id, start_stop_id=None, end_stop_id=None):
    """Fetches Infrabel segments for a train's stops, filling gaps with Dijkstra."""
    global RAILWAY_GRAPH
    if not RAILWAY_GRAPH:
        build_railway_graph()
        
    stops = TrainStop.query.filter_by(train_id=train_id).order_by(TrainStop.stop_sequence).all()
    if not stops: return None
    
    # Filter stops for partial route if requested
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
    seen_segment_ids = set()
    
    # Pre-resolve all available Infrabel IDs
    resolved_stops = []
    for s in stops:
        inf_id = get_infrabel_id(s.stop_id, s.stop_name)
        if inf_id and inf_id in RAILWAY_GRAPH: # ONLY ADD IF IN GRAPH
            resolved_stops.append({"inf_id": inf_id, "stop": s})

    # Connect resolved stops sequentially, skipping gaps
    for i in range(len(resolved_stops) - 1):
        id1 = resolved_stops[i]["inf_id"]
        id2 = resolved_stops[i+1]["inf_id"]
        
        path_seg_ids = find_path(id1, id2)
        if path_seg_ids:
            for seg_id in path_seg_ids:
                # Handle Virtual Edges
                if isinstance(seg_id, str) and seg_id.startswith('V_'):
                    # Parse V_FROM_TO
                    try:
                        _, u, v = seg_id.split('_')
                        p1 = get_pt_coords(u)
                        p2 = get_pt_coords(v)
                        if p1 and p2:
                            features.append({
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": [p1, p2]
                                },
                                "properties": { "from_id": u, "to_id": v, "virtual": True }
                            })
                    except:
                        continue
                    continue
                seen_segment_ids.add(seg_id)
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
            # STRAIGHT LINE FALLBACK for disconnected components
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
# 3. HELPER: GTFS TIME
# ==========================================
def gtfs_time_to_dt(time_str, date_str):
    """Converteert GTFS tijd (bv. 25:30:00) naar datetime object"""
    try:
        h, m, s = map(int, time_str.split(':'))
        day_offset = 0
        if h >= 24:
            h -= 24
            day_offset = 1
        
        base_date = datetime.strptime(date_str, "%Y%m%d") + timedelta(days=day_offset)
        return base_date.replace(hour=h, minute=m, second=s)
    except:
        return None

# ==========================================
# 4. STATIC DATA & PLANNING
# ==========================================
def load_static_data():
    """Laadt ruwe GTFS data in geheugen voor snelle verwerking"""
    print("📂 Static data inladen (Pandas)...")
    
    # Check of bestand bestaat, anders downloaden
    if not os.path.exists(os.path.join(DATA_FOLDER, 'trips.txt')): 
        download_static_data()
        
    try:
        dtype_cfg = str
        # Basis bestanden laden
        static_data['trips'] = pd.read_csv(os.path.join(DATA_FOLDER, 'trips.txt'), dtype=dtype_cfg)
        static_data['routes'] = pd.read_csv(os.path.join(DATA_FOLDER, 'routes.txt'), dtype=dtype_cfg)
        static_data['calendar'] = pd.read_csv(os.path.join(DATA_FOLDER, 'calendar.txt'), dtype=dtype_cfg)
        static_data['calendar_dates'] = pd.read_csv(os.path.join(DATA_FOLDER, 'calendar_dates.txt'), dtype=dtype_cfg)
        
        # Stop Times laden (hier halen we de tijden én de counts vandaan)
        df_st = pd.read_csv(os.path.join(DATA_FOLDER, 'stop_times.txt'), dtype=dtype_cfg)
        
        # Zorg dat stop_sequence een nummer is voor correcte sortering
        df_st['stop_sequence'] = pd.to_numeric(df_st['stop_sequence'])
        
        # Groepeer per rit
        grouped = df_st.sort_values('stop_sequence').groupby('trip_id')
        
        # 1. Eerste halte (Vertrektijd)
        static_data['trip_starts'] = grouped.first().reset_index()[['trip_id', 'departure_time']]
        
        # 2. Laatste halte (Bestemming + Aankomsttijd)
        last_stops = grouped.last().reset_index()
        stops = pd.read_csv(os.path.join(DATA_FOLDER, 'stops.txt'), dtype=dtype_cfg)
        ends = last_stops[['trip_id', 'stop_id', 'arrival_time']]
        ends = ends.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')
        static_data['trip_ends'] = ends[['trip_id', 'stop_name', 'arrival_time']]
        
        # =========================================================
        # HIER GING HET FOUT: DEZE REGEL ONTBRAK
        # We tellen hoeveel stops elke rit heeft
        # =========================================================
        static_data['trip_counts'] = grouped.size().reset_index(name='stop_count')

        # Voeg dit toe aan load_static_data()
        static_data['stops'] = pd.read_csv(os.path.join(DATA_FOLDER, 'stops.txt'), dtype=str)
        static_data['stop_times'] = pd.read_csv(os.path.join(DATA_FOLDER, 'stop_times.txt'), dtype={'stop_sequence': int, 'trip_id': str, 'stop_id': str})

        print("✅ Static data geladen in geheugen!")
    except Exception as e: 
        print(f"❌ Fout static: {e}")

def download_static_data():
    print("⬇️ Downloaden static data met Backup & Rollback protectie...")
    
    backup_created = False
    
    # STAP 1: Backup Maken
    if os.path.exists(DATA_FOLDER):
        try:
            if os.path.exists(BACKUP_FOLDER):
                shutil.rmtree(BACKUP_FOLDER) # Oude backup weg
            # Gebruik copytree ipv rename omdat DATA_FOLDER een mountpoint kan zijn
            shutil.copytree(DATA_FOLDER, BACKUP_FOLDER)
            backup_created = True
            print("   📦 Backup gemaakt.")
        except Exception as e:
            print(f"   ❌ Kon geen backup maken (overslaan): {e}")
            # We gaan door zonder backup als het echt niet lukt

    success = False
    try:
        # STAP 2: Downloaden
        r = requests.get(STATIC_URL, timeout=30)
        if r.status_code == 200:
            os.makedirs(DATA_FOLDER, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(DATA_FOLDER)
            
            # STAP 3: Validatie (Bestaat het EN is het niet leeg?)
            required_files = ['trips.txt', 'routes.txt', 'stop_times.txt', 'calendar.txt']
            
            for f in required_files:
                f_path = os.path.join(DATA_FOLDER, f)
                
                # Check 1: Bestaat het bestand?
                if not os.path.exists(f_path):
                    raise Exception(f"Bestand ontbreekt: {f}")
                
                # Check 2: Is het bestand leeg?
                if os.path.getsize(f_path) == 0:
                    raise Exception(f"Bestand is leeg (0 bytes): {f}")
            
            print("   ✅ Nieuwe data succesvol gedownload en gevalideerd.")
            success = True
        else:
            raise Exception(f"HTTP Fout {r.status_code}")

    except Exception as e:
        print(f"   ⚠️ FOUT tijdens update: {e}")
    
    # STAP 4: Rollback indien nodig
    if not success:
        print("   🔙 ROLLBACK: Backup terugzetten...")
        if backup_created and os.path.exists(BACKUP_FOLDER):
            # Verwijder corrupte bestanden in DATA_FOLDER maar behou de map zelf (ivm mount)
            for item in os.listdir(DATA_FOLDER):
                item_path = os.path.join(DATA_FOLDER, item)
                if os.path.isfile(item_path): os.remove(item_path)
                elif os.path.isdir(item_path): shutil.rmtree(item_path)
            
            # Bestanden terugzetten uit backup
            for item in os.listdir(BACKUP_FOLDER):
                shutil.copy2(os.path.join(BACKUP_FOLDER, item), os.path.join(DATA_FOLDER, item))
            print("   ✅ Rollback voltooid. Oude data is actief.")
        else:
            print("   ❌ Rollback gefaald: geen backup beschikbaar.")
    else:
        # Als alles gelukt is, kan je hier eventueel de backup map verwijderen
        # shutil.rmtree(BACKUP_FOLDER)
        pass

def get_active_services(target_date):
    date_str = target_date.strftime("%Y%m%d")
    # Robust weekday mapping (GTFS requires English column names)
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_name = days[target_date.weekday()]
    
    services = set()
    cal = static_data.get('calendar')
    if cal is not None and not cal.empty:
        # Check if column exists (safeguard)
        if day_name in cal.columns:
            mask = (cal['start_date'] <= date_str) & (cal['end_date'] >= date_str) & (cal[day_name] == '1')
            services.update(cal[mask]['service_id'].tolist())
            
    cd = static_data.get('calendar_dates')
    if cd is not None and not cd.empty:
        exc = cd[cd['date'] == date_str]
        to_add = exc[exc['exception_type'] == '1']['service_id'].tolist()
        to_remove = exc[exc['exception_type'] == '2']['service_id'].tolist()
        services.update(to_add)
        services.difference_update(to_remove)
    return services

def sync_day(target_date, time_limit=None):
    """
    Syncs schedules for a specific date object. 
    Handles rank calculation, deduplication and DB storage.
    """
    global static_data
    with app.app_context():
        target_str = target_date.strftime("%Y%m%d")
        print(f"🗓️  Syncing {target_str} (Limit: {time_limit if time_limit else 'None'})...")

        # A. Check welke treinen we al hebben voor deze datum (om dubbels te voorkomen)
        existing_train_nums = set(
            t.train_number for t in Train.query.filter_by(date=target_str).all()
        )

        # B. Data ophalen uit Pandas (MOET VOOR get_active_services)
        required_keys = ['trips', 'calendar', 'calendar_dates', 'trip_starts']
        is_missing = any(k not in static_data for k in required_keys)
        
        if is_missing:
            print("   Static data incomplete or not loaded. Loading now...")
            load_static_data()

        # C. Haal actieve services op voor DEZE specifieke dag
        active_services = get_active_services(target_date)
        if not active_services:
            print(f"   Geen dienstregeling gevonden voor {target_str}.")
            return

        trips_df = static_data['trips']
        trips_active = trips_df[trips_df['service_id'].isin(active_services)].copy()

        # D. TIJDFILTER (Specifiek voor de nachtritten van morgen)
        trips_active = trips_active.merge(static_data['trip_starts'], on='trip_id')

        if time_limit:
            count_before = len(trips_active)
            trips_active = trips_active[trips_active['departure_time'] < time_limit]
            print(f"   🕒 Nachtfilter: {count_before} -> {len(trips_active)} ritten over.")
        
        if trips_active.empty:
            return

        # ---------------------------------------------------------
        # E. VERBETERDE RANKING LOGICA (Gecorrigeerd)
        # ---------------------------------------------------------

        def calculate_rank(row, target_date_str):
            tid = str(row['trip_id'])
            date_at_end_match = re.search(r':(\d{8})$', tid)
            if date_at_end_match:
                found_date = date_at_end_match.group(1)
                if found_date == target_date_str:
                    return 1
                else:
                    return 2
            if re.search(r':\d{8}:', tid):
                return 3
            return 4

        trips_active['rank'] = trips_active.apply(calculate_rank, axis=1, args=(target_str,))

        # Nu pas mergen met de rest
        merged = trips_active \
            .merge(static_data['routes'], on='route_id') \
            .merge(static_data['trip_ends'], on='trip_id', suffixes=('_start', '_end')) \
            .merge(static_data['trip_counts'], on='trip_id')

        merged.sort_values(
            by=['trip_short_name', 'rank', 'stop_count'], 
            ascending=[True, True, False], 
            inplace=True
        )

        final_df = merged.drop_duplicates(subset=['trip_short_name'], keep='first')
        
        # ---------------------------------------------------------
        # F. OPSLAAN
        # ---------------------------------------------------------
        df_st = pd.read_csv(os.path.join(DATA_FOLDER, 'stop_times.txt'), dtype=str)
        df_stops_static = pd.read_csv(os.path.join(DATA_FOLDER, 'stops.txt'), dtype=str)
        
        new_trains_count = 0
        for _, r in final_df.iterrows():
            train_num = r.get('trip_short_name', '?')
            if not train_num or train_num == '?' or train_num in existing_train_nums:
                continue

            new_train = Train(
                trip_id=r['trip_id'], 
                train_number=train_num,
                route_name=r.get('route_long_name', ''), 
                destination=r.get('trip_headsign') or r.get('stop_name', ''),
                date=target_str,
                departure_time=r['departure_time'],
                arrival_time=r['arrival_time'] 
            )
            db.session.add(new_train)
            db.session.flush() 

            this_trip_stops = df_st[df_st['trip_id'] == r['trip_id']].copy()
            this_trip_stops['stop_type'] = this_trip_stops.apply(
                lambda row: 'STOP' if not (row['pickup_type'] == '1' and row['drop_off_type'] == '1') else 'DOORRIT',
                axis=1
            )

            this_trip_stops = this_trip_stops.merge(df_stops_static[['stop_id', 'stop_name']], on='stop_id')
            this_trip_stops['stop_sequence'] = pd.to_numeric(this_trip_stops['stop_sequence'])
            this_trip_stops.sort_values('stop_sequence', inplace=True)

            stop_objects = []
            for _, s_row in this_trip_stops.iterrows():
                stop_objects.append(TrainStop(
                    train_id=new_train.id,
                    stop_id=s_row['stop_id'],
                    stop_name=s_row['stop_name'],
                    stop_type=s_row['stop_type'],
                    arrival_time=s_row['arrival_time'],
                    departure_time=s_row['departure_time'],
                    stop_sequence=int(s_row['stop_sequence'])
                ))
            
            db.session.bulk_save_objects(stop_objects)
            new_trains_count += 1
        
        db.session.commit()
        if new_trains_count > 0:
            print(f"   ✅ {new_trains_count} ritten toegevoegd voor {target_str}.")



def import_data(target_date_str, skip_if_exists=False):
    """Imports train data for a specific date (YYYY-MM-DD)."""
    global static_data
    
    # Needs pandas, db, models
    # Expects static_data to be loaded
    if not static_data:
        download_static_data() # Ensure data is loaded

    # OPTIMIZATION: On boot, if data exists, skip heavy processing
    if skip_if_exists:
        count = Train.query.filter_by(date=target_date_str).count()
        if count > 0:
            print(f"   ⏩ Skipping {target_date_str} (Data exists: {count} trains).")
            return
        
    print(f"   📥 Importing data for {target_date_str}...")
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"   ❌ Invalid date format: {target_date_str}")
        return

    # 1. Active Services
    active_services = get_active_services(target_date)
    if not active_services:
        print(f"   ⚠️ No active services found for {target_date_str}.")
        return

    # 2. Filter Trips
    trips_df = static_data['trips']
    if trips_df is None or trips_df.empty:
        print("   ❌ Trips data empty.")
        return

    # Filter by service_id
    trips_active = trips_df[trips_df['service_id'].isin(active_services)].copy()
    
    if trips_active.empty:
        return

    # 3. Add Rank
    def calculate_rank(row, target_date_s):
         tid = str(row['trip_id'])
         # Regex for date in trip_id
         date_at_end_match = re.search(r':(\d{8})$', tid)
         target_compact = target_date_s.replace('-', '')
         if date_at_end_match:
             if date_at_end_match.group(1) == target_compact: return 1
             return 2
         if re.search(r':\d{8}:', tid): return 3
         return 4

    trips_active['rank'] = trips_active.apply(calculate_rank, axis=1, args=(target_date_str,))
    
    # Merge details
    # We must merge trip_starts to get 'departure_time'
    if 'trip_starts' in static_data:
        trips_active = trips_active.merge(static_data['trip_starts'], on='trip_id', suffixes=('', '_start'))

    merged = trips_active \
        .merge(static_data['routes'], on='route_id') \
        .merge(static_data['trip_ends'], on='trip_id', suffixes=('_start', '_end')) \
        .merge(static_data['trip_counts'], on='trip_id')

    # Sort and Dedup
    merged.sort_values(by=['trip_short_name', 'rank', 'stop_count'], ascending=[True, True, False], inplace=True)
    final_df = merged.drop_duplicates(subset=['trip_short_name'], keep='first')
    
    # 4. Save to DB
    df_st = pd.read_csv(os.path.join(DATA_FOLDER, 'stop_times.txt'), dtype=str)
    df_stops_static = pd.read_csv(os.path.join(DATA_FOLDER, 'stops.txt'), dtype=str)
    
    count_synced = 0
    
    for _, r in final_df.iterrows():
        train_num = r.get('trip_short_name', '?')
        if not train_num or train_num == '?': continue
        
        # Check existence
        existing_train = Train.query.filter_by(train_number=train_num, date=target_date_str).first()
        
        # Logic: If exists, update. If not, create.
        if existing_train:
            # Update
            existing_train.trip_id = r['trip_id']
            existing_train.route_name = r.get('route_long_name', '')
            existing_train.destination = r.get('trip_headsign') or r.get('stop_name', '')
            # departure_time might be in r as 'departure_time' or 'departure_time_start' depending on merges
            # trip_starts has 'departure_time'
            existing_train.departure_time = r.get('departure_time')
            existing_train.arrival_time = r.get('arrival_time')
            TrainStop.query.filter_by(train_id=existing_train.id).delete()
            train_obj = existing_train
        else:
            train_obj = Train(
                trip_id=r['trip_id'],
                train_number=train_num,
                route_name=r.get('route_long_name', ''),
                destination=r.get('trip_headsign') or r.get('stop_name', ''),
                date=target_date_str,
                departure_time=r.get('departure_time'),
                arrival_time=r.get('arrival_time')
            )
            db.session.add(train_obj)
        
        db.session.flush() # Get ID
        
        # Stops
        this_trip_stops = df_st[df_st['trip_id'] == r['trip_id']].copy()
        this_trip_stops['stop_type'] = this_trip_stops.apply(
            lambda row: 'STOP' if not (row['pickup_type'] == '1' and row['drop_off_type'] == '1') else 'DOORRIT', axis=1
        )
        this_trip_stops = this_trip_stops.merge(df_stops_static[['stop_id', 'stop_name']], on='stop_id')
        this_trip_stops['stop_sequence'] = pd.to_numeric(this_trip_stops['stop_sequence'])
        this_trip_stops.sort_values('stop_sequence', inplace=True)
        
        stop_objects = []
        for _, s_row in this_trip_stops.iterrows():
             stop_objects.append(TrainStop(
                 train_id=train_obj.id,
                 stop_id=s_row['stop_id'],
                 stop_name=s_row['stop_name'],
                 stop_type=s_row['stop_type'],
                 arrival_time=s_row['arrival_time'],
                 departure_time=s_row['departure_time'],
                 stop_sequence=int(s_row['stop_sequence'])
             ))
        db.session.bulk_save_objects(stop_objects)
        count_synced += 1
        
    db.session.commit()
    print(f"   ✅ Imported {count_synced} trains for {target_date_str}.")

def populate_todays_schedule(fast_boot=False):
    """
    Populates schedule for [today-7, today+7] and cleans up old data.
    Runs at 4 AM daily.
    """
    with app.app_context():
        today = datetime.now()
        
        # 1. Cleanup old data (older than 7 days ago, or future > 7 days?)
        # User said "database may also be -7 to +7".
        # Let's keep data strictly within window.
        min_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        max_date = (today + timedelta(days=7)).strftime("%Y-%m-%d")
        
        print(f"🧹 [CLEANUP] Removing trains outside {min_date} to {max_date}...")
        try:
            # First delete stops for the trains we are about to delete
            # This fixes ForeignKeyViolation
            trains_to_delete = db.session.query(Train.id).filter(or_(Train.date < min_date, Train.date > max_date)).subquery()
            db.session.query(TrainStop).filter(TrainStop.train_id.in_(trains_to_delete)).delete(synchronize_session=False)
            
            # Now delete trains
            db.session.query(Train).filter(or_(Train.date < min_date, Train.date > max_date)).delete(synchronize_session=False)
            db.session.commit()
            print("✅ [CLEANUP] Old data removed.")
        except Exception as e:
            db.session.rollback()
            print(f"❌ [CLEANUP] Failed: {e}")

        # 2. Populate/Update window
        print(f"📅 [SYNC] Updating schedule for range [-7, +7] (FastBoot={fast_boot})...")
        for i in range(-7, 8):
            target_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
            print(f"   🔄 Syncing {target_date}...")
            try:
                # Use fast_boot to skip if data exists
                import_data(target_date, skip_if_exists=fast_boot)
            except Exception as e:
                print(f"   ⚠️ Failed to sync {target_date}: {e}")
        
        print("✅ [SYNC] Window update completed.")

# ... (rest of main.py until end)

# End of main.py logic update:
# Search for the startup loop to pass fast_boot=True



def sync_translations_to_db():
    """Leest translations.txt en zet het in de database zonder duplicaten."""
    print("🔄 Synchroniseren vertalingen naar DB...")
    trans_path = os.path.join(DATA_FOLDER, 'translations.txt')
    stops_path = os.path.join(DATA_FOLDER, 'stops.txt')
    if not os.path.exists(trans_path) or not os.path.exists(stops_path): 
        return

    try:
        df_trans = pd.read_csv(trans_path, dtype=str)
        df_stops = pd.read_csv(stops_path, dtype=str)
        
        # 1. Filter alleen de stops uit de vertalingstabel
        stop_trans = df_trans[df_trans['table_name'] == 'stops'].copy()
        
        # 2. NMBS gebruikt 'field_value' als de officiële naam die in stops.txt staat.
        # We mergen dit om de stop_id te bemachtigen.
        merged = stop_trans.merge(df_stops[['stop_id', 'stop_name']], left_on='field_value', right_on='stop_name')
        
        # 3. CRUCIAAL: Verwijder duplicaten op basis van de unieke constraint (naam + taal)
        # We houden de eerste stop_id die we tegenkomen voor dat station.
        unique_translations = merged.drop_duplicates(subset=['field_value', 'language'])
        
        # 4. Tabel opschonen en opnieuw vullen
        db.session.query(StopTranslation).delete()
        
        translations_to_add = []
        for _, row in unique_translations.iterrows():
            translations_to_add.append(StopTranslation(
                stop_id=row['stop_id'],
                field_value=row['field_value'],
                lang=row['language'],
                translation=row['translation']
            ))
        
        db.session.bulk_save_objects(translations_to_add)
        db.session.commit()
        print(f"✅ Vertalingen geladen voor {len(translations_to_add)} unieke stationsnamen.")
        
    except Exception as e:
        db.session.rollback()
        print(f"❌ Fout bij synchroniseren vertalingen: {e}")

def migrate_db():
    """Manually adds missing columns to the database (for when not using Alembic)."""
    print("🔄 Checking for database migrations...")
    try:
        # Check TrainUnit columns
        res = db.session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='train_units'"))
        existing_cols = [r[0] for r in res]
        if 'traction_type' not in existing_cols:
            print("   ➕ Adding traction_type to train_units")
            db.session.execute(text("ALTER TABLE train_units ADD COLUMN traction_type VARCHAR(10)"))
            db.session.commit()

        # Check Journey columns
        res = db.session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='journeys'"))
        existing_cols = [r[0] for r in res]
        
        for i in [3, 4]:
            if f'unit_id_{i}' not in existing_cols:
                print(f"   ➕ Adding unit selection {i} to journeys")
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN unit_id_{i} INTEGER REFERENCES train_units(id) ON DELETE SET NULL"))
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN material_type_{i} VARCHAR(50)"))
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN material_number_{i} VARCHAR(50)"))
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN parent_type_{i} VARCHAR(50)"))
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN sub_type_{i} VARCHAR(10)"))
                db.session.execute(text(f"ALTER TABLE journeys ADD COLUMN orientation_{i} VARCHAR(20)"))
                db.session.commit()
                
        print("✅ Database migrations checked.")
    except Exception as e:
        print(f"❌ Migration error: {e}")
        db.session.rollback()

# ==========================================
# 5. LOGICA & FETCHING (Aangepaste iRail Parser)
# ==========================================
# --- COMPOSITION LOGIC REMOVED ---
def parse_irail(data):
    return []

def fetch_irail_composition(train, timeout=10):
    return False

# --- END CORE LOGIC ---

# ==========================================
# 6. API ENDPOINTS
# ==========================================
@app.route('/api/update_journey', methods=['POST'])
def update_journey():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"success": False, "error": "Not logged in"}), 401
        
    data = request.get_json()
    journey_id = data.get('journey_id')
    start_stop_id = data.get('start_stop_id')
    end_stop_id = data.get('end_stop_id')
    
    # Selected Units (up to 4)
    unit_ids = data.get('unit_ids', []) # Expecting a list from frontend
    # Fallback to legacy single/double unit_id if list not provided
    if not unit_ids:
        u1 = data.get('unit_id')
        u2 = data.get('unit_id_2')
        if u1: unit_ids.append(u1)
        if u2: unit_ids.append(u2)
    
    if not journey_id or not start_stop_id or not end_stop_id:
        return jsonify({"success": False, "error": "Missing journey ID or stops"}), 400
        
    journey = Journey.query.filter_by(id=journey_id, user_id=user_id).first()
    if not journey:
        return jsonify({"success": False, "error": "Journey not found"}), 404
        
    try:
        # Update Stops
        journey.start_stop_id = start_stop_id
        journey.end_stop_id = end_stop_id
        
        # Recalculate Distance
        dist = get_journey_distance(journey.train_id, start_stop_id, end_stop_id)
        journey.distance_km = dist
        
        # Clear all previous units first
        for i in range(1, 5):
            suffix = f"_{i}" if i > 1 else ""
            setattr(journey, f"unit_id{suffix}", None)
            setattr(journey, f"material_type{suffix}", None)
            setattr(journey, f"material_number{suffix}", None)
            setattr(journey, f"parent_type{suffix}", None)
            setattr(journey, f"sub_type{suffix}", None)
            setattr(journey, f"orientation{suffix}", None)

        # Update with new units (max 4)
        for i, uid in enumerate(unit_ids[:4]):
            u = TrainUnit.query.get(uid)
            if u:
                suffix = f"_{i+1}" if i > 0 else ""
                setattr(journey, f"unit_id{suffix}", u.id)
                setattr(journey, f"material_type{suffix}", u.material_type)
                setattr(journey, f"material_number{suffix}", u.material_number)
                setattr(journey, f"parent_type{suffix}", u.parent_type)
                setattr(journey, f"sub_type{suffix}", u.sub_type)
                setattr(journey, f"orientation{suffix}", u.orientation)

        db.session.commit()
        return jsonify({"success": True, "message": "Journey updated"}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/delete_journey/<int:journey_id>', methods=['DELETE'])
def api_delete_journey(journey_id):
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    journey = Journey.query.filter_by(id=journey_id, user_id=session['user_id']).first()
    if not journey:
        return jsonify({"error": "Journey not found"}), 404
        
    db.session.delete(journey)
    db.session.commit()
    return jsonify({"success": True})

@app.route('/api/trace/<int:train_id>')
def api_trace(train_id):
    start_stop = request.args.get('start')
    end_stop = request.args.get('end')
    geom = get_trace_geometry(train_id, start_stop, end_stop)
    if geom:
        return jsonify(geom)
    return jsonify({"error": "No trace data"}), 404

@app.route('/api/composition/<int:train_id>')
def api_composition(train_id):
    train = db.session.get(Train, train_id)
    if not train:
        return jsonify({"error": "Train not found"}), 404
    
    # Return stops for the timeline, but empty units (Composition disabled)
    stops = [{"id": s.stop_id, "name": get_translated_stop(s.stop_id), "time": s.departure_time or s.arrival_time, "type": s.stop_type} for s in train.stops]
    
    return jsonify({
        "train_number": train.train_number,
        "units": [],
        "stops": stops,
        "has_data": False,
        "fetched": True
    })

@app.route('/api/fetch_composition/<int:train_id>', methods=['POST'])
def api_fetch_composition(train_id):
    return jsonify({"error": "Composition fetching disabled"}), 404

# Auth Routes Removed - delegated to Treinfo Account Service

@app.route('/api/whoami')
def api_whoami():
    return jsonify({
        "logged_in": 'user_id' in session,
        "user_id": session.get('user_id'),
        "username": session.get('username'),
        "session_cookie_name": app.config.get('SESSION_COOKIE_NAME')
    })

@app.route('/api/log_journey', methods=['POST'])
def api_log_journey():
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    data = request.json
    try:
        train_id = data.get('train_id')
        start_id = data.get('start_stop_id')
        end_id = data.get('end_stop_id')
        
        # Calculate distance
        dist = get_journey_distance(train_id, start_id, end_id)
        
        # Selected Units (up to 4)
        unit_ids = data.get('unit_ids', [])
        if not unit_ids:
            u1 = data.get('unit_id')
            u2 = data.get('unit_id_2')
            if u1: unit_ids.append(u1)
            if u2: unit_ids.append(u2)

        journey_kwargs = {
            "user_id": session['user_id'],
            "train_id": train_id,
            "start_stop_id": start_id,
            "end_stop_id": end_id,
            "train_number": data.get('train_number'),
            "destination": data.get('destination'),
            "distance_km": dist
        }

        for i, uid in enumerate(unit_ids[:4]):
            u = db.session.get(TrainUnit, uid)
            if u:
                suffix = f"_{i+1}" if i > 0 else ""
                journey_kwargs[f"unit_id{suffix}"] = u.id
                journey_kwargs[f"material_type{suffix}"] = u.material_type
                journey_kwargs[f"material_number{suffix}"] = u.material_number
                journey_kwargs[f"parent_type{suffix}"] = u.parent_type
                journey_kwargs[f"sub_type{suffix}"] = u.sub_type
                journey_kwargs[f"orientation{suffix}"] = u.orientation

        journey = Journey(**journey_kwargs)
        db.session.add(journey)
        db.session.commit()
        return jsonify({"success": True})
        db.session.add(journey)
        db.session.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/my_history')
def api_my_history():
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    journeys = Journey.query.filter_by(user_id=session['user_id']).order_by(Journey.created_at.desc()).all()
    
    result = []
    for j in journeys:
        train = db.session.get(Train, j.train_id)
        
        # Resolve names and times
        start_name = get_translated_stop(j.start_stop_id)
        end_name = get_translated_stop(j.end_stop_id)
        
        dep_time = "?"
        arr_time = "?"
        train_full_path = "?"
        full_route_list = []
        your_route_list = []
        
        if train:
            all_stops = db.session.query(TrainStop).filter_by(train_id=train.id).order_by(TrainStop.stop_sequence).all()
            
            # Full Route
            for s in all_stops:
                full_route_list.append({
                    "id": s.stop_id,
                    "name": get_translated_stop(s.stop_id),
                    "time": s.departure_time[:5] if s.departure_time else (s.arrival_time[:5] if s.arrival_time else "?"),
                    "type": s.stop_type
                })
            
            if all_stops:
                train_full_path = f"{get_translated_stop(all_stops[0].stop_id)} → {get_translated_stop(all_stops[-1].stop_id)}"

            # Your Route (Slice)
            in_range = False
            for s in all_stops:
                if s.stop_id == j.start_stop_id: in_range = True
                if in_range:
                    your_route_list.append({
                        "id": s.stop_id,
                        "name": get_translated_stop(s.stop_id),
                        "time": s.departure_time[:5] if s.departure_time else (s.arrival_time[:5] if s.arrival_time else "?"),
                        "type": s.stop_type
                    })
                if s.stop_id == j.end_stop_id: in_range = False
            
            s_stop = next((s for s in all_stops if s.stop_id == j.start_stop_id), None)
            e_stop = next((s for s in all_stops if s.stop_id == j.end_stop_id), None)
            
            if s_stop: dep_time = s_stop.departure_time[:5] if s_stop.departure_time else "?"
            if e_stop: arr_time = e_stop.arrival_time[:5] if e_stop.arrival_time else "?"
        else:
            your_route_list = [
                {"id": j.start_stop_id, "name": start_name, "time": dep_time},
                {"id": j.end_stop_id, "name": end_name, "time": arr_time}
            ]

        # Formatting Unit string (All 4 units)
        units_display = []
        for i in range(1, 5):
            suffix = f"_{i}" if i > 1 else ""
            m_type = getattr(j, f"material_type{suffix}")
            if m_type:
                p_type = getattr(j, f"parent_type{suffix}") or m_type
                s_type = getattr(j, f"sub_type{suffix}")
                m_num = getattr(j, f"material_number{suffix}")
                
                part = f"{p_type}"
                if s_type: part += f" - {s_type}"
                if m_num: part += f" - {m_num}"
                units_display.append(part)
        
        units_display_str = " | ".join(units_display)

        result.append({
            "id": j.id,
            "train_id": train.id if train else j.train_id,
            "train_number": j.train_number if j.train_number and j.train_number != 'History' else (train.train_number if train else j.train_number or '?'),
            "train_full_path": train_full_path,
            "start_station": start_name,
            "end_station": end_name,
            "destination": j.destination if j.destination and j.destination != 'Saved Journey' else (get_translated_stop(train.destination) if train else j.destination or '?'),
            "start_stop": j.start_stop_id,
            "end_stop": j.end_stop_id,
            "dep_time": dep_time,
            "arr_time": arr_time,
            "your_route": your_route_list,
            "full_route": full_route_list,
            "distance_km": j.distance_km,
            "units_display": units_display_str,
            "created_at": j.created_at.isoformat(),
            "date": train.date if train else j.created_at.strftime('%Y%m%d')
        })
        
    return jsonify({"journeys": result})

@app.route('/api/coverage')
def api_coverage():
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    journeys = Journey.query.filter_by(user_id=session['user_id']).all()
    all_segments = set()
    
    for j in journeys:
        # Trace path for each journey if possible
        if j.start_stop_id and j.end_stop_id:
            id1 = get_infrabel_id(j.start_stop_id)
            id2 = get_infrabel_id(j.end_stop_id)
            if id1 and id2:
                path_seg_ids = find_path(id1, id2)
                if path_seg_ids:
                    all_segments.update(path_seg_ids)
        elif j.train_id:
            # Fallback: full train path if no specific stops recorded
            stops = TrainStop.query.filter_by(train_id=j.train_id).order_by(TrainStop.stop_sequence).all()
            if len(stops) >= 2:
                id1 = get_infrabel_id(stops[0].stop_id)
                id2 = get_infrabel_id(stops[-1].stop_id)
                if id1 and id2:
                    path_seg_ids = find_path(id1, id2)
                    if path_seg_ids: all_segments.update(path_seg_ids)
            
    features = []
    for seg_id in all_segments:
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
            
    return jsonify({
        "type": "FeatureCollection",
        "features": features
    })

@app.route('/api/monthly_coverage')
def api_monthly_coverage():
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    user_id = session['user_id']
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    
    now = datetime.utcnow()
    if not month: month = now.month
    if not year: year = now.year
    
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    journeys = Journey.query.filter(
        Journey.user_id == user_id,
        Journey.created_at >= start_date,
        Journey.created_at < end_date
    ).all()
    
    features = []
    for j in journeys:
        trace = get_trace_geometry(j.train_id, j.start_stop_id, j.end_stop_id)
        if trace and trace['type'] == 'FeatureCollection':
            features.extend(trace['features'])
            
    return jsonify({
        "type": "FeatureCollection",
        "features": features
    })

@app.route('/api/range_coverage')
def api_range_coverage():
    if 'user_id' not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    user_id = session['user_id']
    start_month = request.args.get('start_month', type=int)
    start_year = request.args.get('start_year', type=int)
    end_month = request.args.get('end_month', type=int)
    end_year = request.args.get('end_year', type=int)
    
    if not all([start_month, start_year, end_month, end_year]):
        return jsonify({"error": "Missing parameters"}), 400
    
    start_date = datetime(start_year, start_month, 1)
    if end_month == 12:
        end_date = datetime(end_year + 1, 1, 1)
    else:
        end_date = datetime(end_year, end_month + 1, 1)
    
    journeys = Journey.query.filter(
        Journey.user_id == user_id,
        Journey.created_at >= start_date,
        Journey.created_at < end_date
    ).all()
    
    features = []
    for j in journeys:
        trace = get_trace_geometry(j.train_id, j.start_stop_id, j.end_stop_id)
        if trace and trace['type'] == 'FeatureCollection':
            features.extend(trace['features'])
            
    return jsonify({
        "type": "FeatureCollection",
        "features": features
    })

@app.route('/api/wrapped')
def api_wrapped():
    token = request.args.get('token')
    user_id = None
    username = None
    
    if token:
        try:
            # Token is base64 encoded JSON
            missing_padding = len(token) % 4
            if missing_padding:
                token += '=' * (4 - missing_padding)
            
            decoded_data = json.loads(base64.b64decode(token).decode('utf-8'))
            user_id = decoded_data.get('u')
            user = db.session.get(User, user_id)
            if user:
                username = user.username
        except:
            return jsonify({"error": "Invalid share token"}), 400
    else:
        if 'user_id' not in session:
            return jsonify({"error": "Not logged in"}), 401
        user_id = session['user_id']
        username = session.get('username')

    if not user_id:
        return jsonify({"error": "User not found"}), 404

    year = request.args.get('year')
    month = request.args.get('month')

    query = Journey.query.filter_by(user_id=user_id)
    if year:
        query = query.filter(db.extract('year', Journey.created_at) == int(year))
    if month and month != 'all':
        query = query.filter(db.extract('month', Journey.created_at) == int(month))

    journeys = query.all()
    if not journeys:
        return jsonify({
            "total_trips": 0,
            "total_km": 0,
            "username": username,
            "rankings": {
                "stations": [], "materials": [], "units": [], "routes": [], "segments": [], "time_blocks": []
            }
        }), 200

    # Data aggregation
    station_stats = {}
    material_stats = {}
    unit_stats = {}
    route_stats = {}
    segment_stats = {}
    time_block_stats = {
        "Night": 0,
        "Early Morning": 0,
        "Morning": 0,
        "Noon": 0,
        "Afternoon": 0,
        "Evening": 0,
        "Late Evening": 0
    }
    day_stats = {}
    unique_units_set = set()
    total_km = 0.0

    for j in journeys:
        total_km += (j.distance_km or 0.0)
        
        # Day of week
        day_name = j.created_at.strftime('%A')
        day_stats[day_name] = day_stats.get(day_name, 0) + 1
        
        # 1. Stations
        for sid in [j.start_stop_id, j.end_stop_id]:
            if sid:
                name = get_translated_stop(sid)
                station_stats[name] = station_stats.get(name, 0) + 1
        
        # 2. Materials (Distinct per journey)
        journey_materials = set()
        for p_t, m_t in [
            (j.parent_type, j.material_type),
            (j.parent_type_2, j.material_type_2),
            (j.parent_type_3, j.material_type_3),
            (j.parent_type_4, j.material_type_4)
        ]:
            m = p_t or m_t
            if m:
                journey_materials.add(m)
        for m in journey_materials:
            material_stats[m] = material_stats.get(m, 0) + 1
        
        # 3. Units (Weighted)
        for unit_attr, num_attr, p_type_attr in [
            (j.unit_id, j.material_number, j.parent_type),
            (j.unit_id_2, j.material_number_2, j.parent_type_2),
            (j.unit_id_3, j.material_number_3, j.parent_type_3),
            (j.unit_id_4, j.material_number_4, j.parent_type_4)
        ]:
            if num_attr or unit_attr:
                u_label = num_attr or f"ID {unit_attr}"
                score = unit_stats.get(u_label, 0) + 1
                
                # Check for special units
                is_special = False
                lnum = str(u_label).upper()
                # Locomotives
                if re.match(r'^(18|19|13|21|27)\d{2}', lnum): is_special = True
                # Control cars: BDx, BVx, Bx, ADx, Bmx
                if any(x in lnum for x in ['BDX', 'BVX', 'BX', 'ADX', 'BMX']): is_special = True
                # EMU/DMU
                if lnum.startswith('AM') or lnum.startswith('AR'): is_special = True
                
                if p_type_attr:
                    lp = p_type_attr.upper()
                    if any(x in lp for x in ['HLE18', 'HLE19', 'HLE13', 'HLE21', 'HLE27']): is_special = True
                    if 'M7' in lp and 'BMX' in lnum: is_special = True
                    if lp.startswith('AM') or lp.startswith('AR'): is_special = True

                if is_special:
                    score += 10000 
                
                unit_stats[u_label] = score
                unique_units_set.add(u_label)

        # 4. Routes & Segments
        if j.start_stop_id and j.end_stop_id:
            s_name = get_translated_stop(j.start_stop_id)
            e_name = get_translated_stop(j.end_stop_id)
            if s_name != "Unknown" and e_name != "Unknown":
                # Unified Route: Sort names to ensure Gent-Lokeren == Lokeren-Gent
                names = sorted([s_name, e_name])
                r_key = f"{names[0]} - {names[1]}"
                route_stats[r_key] = route_stats.get(r_key, 0) + 1
                
                # Segments
                id1 = get_infrabel_id(j.start_stop_id)
                id2 = get_infrabel_id(j.end_stop_id)
                if id1 and id2:
                    path = find_path(id1, id2)
                    if path:
                        for seg_id in path:
                            seg = db.session.get(InfrabelStationToStation, seg_id)
                            if seg:
                                # Unified Segment: Sort IDs to ensure A->B == B->A
                                ids = sorted([seg.stationfrom_id, seg.stationto_id])
                                seg_key = f"{ids[0]} - {ids[1]}"
                                segment_stats[seg_key] = segment_stats.get(seg_key, 0) + 1

        # 5. Time Blocks
        h = j.created_at.hour
        if 0 <= h < 4: time_block_stats["Night"] += 1
        elif 4 <= h < 7: time_block_stats["Early Morning"] += 1
        elif 7 <= h < 11: time_block_stats["Morning"] += 1
        elif 11 <= h < 13: time_block_stats["Noon"] += 1
        elif 13 <= h < 17: time_block_stats["Afternoon"] += 1
        elif 17 <= h < 22: time_block_stats["Evening"] += 1
        else: time_block_stats["Late Evening"] += 1

    # Sorting
    def sort_top(d, bonus=0, limit=10):
        sorted_keys = sorted(d, key=d.get, reverse=True)[:limit]
        return [{"label": k, "count": d[k] % bonus if bonus > 0 else d[k]} for k in sorted_keys]

    # Resolve unified segment names
    top_segments_raw = sorted(segment_stats, key=segment_stats.get, reverse=True)[:10] # Increased limit for details
    top_segments = []
    for seg_key in top_segments_raw:
        # seg_key is "ID1 - ID2"
        id1, id2 = seg_key.split(" - ")
        n1 = get_op_name(id1)
        n2 = get_op_name(id2)
        top_segments.append({"label": f"{n1} - {n2}", "count": segment_stats[seg_key]})

    # Sharing Token Generation
    share_payload = {"u": user_id, "y": datetime.now().year, "m": datetime.now().month}
    share_token = base64.b64encode(json.dumps(share_payload).encode('utf-8')).decode('utf-8')

    top_day = max(day_stats, key=day_stats.get) if day_stats else "None"

    return jsonify({
        "username": username,
        "total_trips": len(journeys),
        "total_km": round(total_km, 1),
        "unique_units": len(unique_units_set),
        "top_day": top_day,
        "share_token": share_token,
        "rankings": {
            "stations": sort_top(station_stats),
            "materials": sort_top(material_stats),
            "units": sort_top(unit_stats, 10000),
            "routes": sort_top(route_stats),
            "segments": top_segments,
            "days": sort_top(day_stats),
            "time_blocks": [{"label": k, "count": v} for k, v in time_block_stats.items() if v > 0]
        }
    })

def get_translated_stop(stop_name_or_id):
    """Resolves name or ID to a translated name."""
    if not stop_name_or_id: return "Unknown"
    lang = get_locale()
    
    # 1. Check if it looks like an ID (S88... or 88...)
    clean_id = str(stop_name_or_id).strip()
    if clean_id.startswith('88') or (clean_id.startswith('S') and clean_id[1:].isdigit()):
        stop_id = clean_id
        # Try exact stop_id match
        trans = StopTranslation.query.filter_by(stop_id=stop_id, lang=lang).first()
        if trans: return trans.translation
        
        # Try without S prefix or with S prefix
        alt_id = stop_id[1:] if stop_id.startswith('S') else f"S{stop_id}"
        trans = StopTranslation.query.filter_by(stop_id=alt_id, lang=lang).first()
        if trans: return trans.translation
        
        # Fallback: any language for this ID
        trans = StopTranslation.query.filter_by(stop_id=stop_id).first()
        if trans: return trans.translation
        
        # Try finding the name in TrainStop table
        stop_entry = TrainStop.query.filter_by(stop_id=stop_id).first()
        if stop_entry: return stop_entry.stop_name

        return stop_id # Last resort
    
    # 2. Traditional name-based lookup
    stop_name_raw = stop_name_or_id
    trans = StopTranslation.query.filter_by(field_value=stop_name_raw, lang=lang).first()
    if trans: return trans.translation

    # fallback: for French, if no translation exists, we often WANT the original name (which is usually French)
    # instead of falling back to a Dutch translation.
    if lang == 'fr':
        return stop_name_raw

    # fallback: try ANY language for this field_value (e.g. return NL if EN is missing)
    trans = StopTranslation.query.filter_by(field_value=stop_name_raw).first()
    if trans: return trans.translation

    return stop_name_raw

def get_op_name(op_id):
    """Resolves Infrabel ID or PTCAR ID to a name."""
    op = InfrabelOperationalPoint.query.filter_by(id=op_id).first()
    if not op:
        op = InfrabelOperationalPoint.query.filter_by(ptcar_id=op_id).first()
    if op:
        lang = get_locale()
        return op.name_nl if lang == 'nl' else (op.name_fr or op.name_nl)
    return op_id

# ==========================================
# 6. PWA & ROUTES
# ==========================================
@app.context_processor
def inject():
    last_update_time = None
    try:
        # OUD: entry = SystemStatus.query.get('last_realtime_update')
        # NIEUW:
        entry = db.session.get(SystemStatus, 'last_realtime_update')
        if entry:
            last_update_time = entry.updated_at
    except:
        pass

    return dict(t=t, current_lang=get_locale(), now=datetime.now(), last_update=last_update_time)

@app.template_filter('format_date')
def format_date(value):
    try: return datetime.strptime(str(value), "%Y%m%d").strftime("%d/%m/%Y")
    except: return value

@app.template_filter('more_time_to_str')
def more_time_to_str(time_str):
    """Converteert GTFS tijd (bv. 25:30:00) naar normale tijd (01:30)"""
    try:
        h, m = map(int, time_str.split(':'))
        if h >= 24:
            h -= 24
        return f"{str(h).zfill(2)}:{str(m).zfill(2)}"
    except:
        return time_str

@app.template_filter('display_service_date')
def display_service_date(date_str, time_str):
    """
    Bepaalt de weergave van de datum.
    Als de trein voor 04:00 rijdt, tonen we de 'Ritdag' (gisteren).
    """
    try:
        dt = datetime.strptime(str(date_str), "%Y%m%d")
        hour = int(time_str.split(':')[0])
        
        # Formaat voor de kalenderdag (bv. Za 24/12)
        day_names = {
            'nl': ['Ma', 'Di', 'Wo', 'Do', 'Vr', 'Za', 'Zo'],
            'fr': ['Lu', 'Ma', 'Me', 'Je', 'Ve', 'Sa', 'Di'],
            'en': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'de': ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
        }
        lang = get_locale()
        wd = dt.weekday()
        day_name = day_names.get(lang, day_names['nl'])[wd]
        formatted_cal_date = f"{day_name} {dt.strftime('%d/%m')}"

        # Als het NACHT is (00:00 - 03:59), bereken dan de Ritdag (Gisteren)
        if 0 <= hour < 4:
            prev_day = dt - timedelta(days=1)
            prev_wd = prev_day.weekday()
            prev_day_name = day_names.get(lang, day_names['nl'])[prev_wd]
            formatted_service_date = f"{prev_day_name} {prev_day.strftime('%d/%m')}"
            
            # Retourneer een dictionary of object voor de template
            return {
                "is_night": True,
                "calendar": formatted_cal_date,     # Bv. Za 24/12
                "service": formatted_service_date   # Bv. Vr 23/12
            }
        
        # Normale trein
        return {
            "is_night": False,
            "calendar": formatted_cal_date,
            "service": formatted_cal_date
        }
    except:
        return {"is_night": False, "calendar": str(date_str), "service": str(date_str)}
    
@app.template_filter('translate_stop')
def translate_stop_filter(value):
    return get_translated_stop(str(value))

# --- PWA ENDPOINTS ---
@app.route('/manifest.json')
def manifest():
    return jsonify({
        "name": "Treinfo Tracker",
        "short_name": "B Tracker",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#212529",
        "icons": [
            {"src": "/app/icon/192", "sizes": "192x192", "type": "image/png"},
            {"src": "/app/icon/512", "sizes": "512x512", "type": "image/png"},
            {"src": "/app/icon/2048", "sizes": "2048x2048", "type": "image/png"}
        ]
    })

@app.route('/service-worker.js')
def service_worker():
    sw_code = """
    self.addEventListener('install', function(event) { event.waitUntil(self.skipWaiting()); });
    self.addEventListener('activate', function(event) { event.waitUntil(self.clients.claim()); });
    self.addEventListener('fetch', function(event) { });
    """
    response = make_response(sw_code)
    response.headers['Content-Type'] = 'application/javascript'
    return response

@app.route('/app/icon/<size>')
def app_icon(size):
    size_map = {
        '192': 'icons/icon-192.png',
        '512': 'icons/icon-512.png',
        '2048': 'icons/icon-2048.png'
    }
    icon_path = size_map.get(size, 'icons/icon-192.png')
    return send_from_directory('static', icon_path)

@app.route('/api/set_lang')
def set_lang():
    lang = request.args.get('lang')
    if lang in TRANSLATIONS: session['lang'] = lang
    return jsonify({"success": True})

@app.route('/api/search_suggestions')
def search_suggestions():
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify([])
    trains = db.session.query(Train.train_number).filter(Train.train_number.like(f"%{q}%")).distinct().limit(5).all()
    units = db.session.query(TrainUnit.material_number).filter(TrainUnit.material_number.like(f"%{q}%")).distinct().limit(5).all()
    db.session.remove()
    suggestions = [t[0] for t in trains] + [u[0] for u in units]
    return jsonify(suggestions)

def apply_filters(query, args, enforce_today=True, filter_status=True):
    # Check of we in de 'Search' route zitten (gebaseerd op URL of parameters)
    is_search = 'q' in args and args.get('q').strip() != ""

    # 1. Datum filter: Alleen op dashboard (vandaag)
    if enforce_today and not is_search:
        today_str = datetime.now().strftime("%Y%m%d")
        query = query.filter(Train.date == today_str)

    # 2. Status & Samenstelling filters
    # Op het dashboard passen we de filters toe die in de args (URL) staan
    # Maar bij ZOEKEN (is_search) negeren we deze filters standaard zodat de 
    # gebruiker alle historische info ziet.
    if not is_search:
        show_no_comp = args.get('show_no_comp') == 'on'
        show_inactive = args.get('show_inactive') == 'on'

        if filter_status and not show_inactive:
            query = query.filter(Train.status.in_(['ACTIVE', 'SCHEDULED']))
        
        if not show_no_comp:
            query = query.filter(or_(Train.has_composition_data == True, Train.status == 'CANCELED'))
        
    return query

if not os.environ.get('FLASK_TESTING'):
    with app.app_context():
        db.create_all()
        migrate_db()
        # Forceer import van vertalingen als de tabel leeg is
        if db.session.query(StopTranslation).count() == 0:
            print("Tabel StopTranslation is leeg, synchroniseren...")
            sync_translations_to_db()

@app.route('/')
def index():
    user_id = session.get('user_id')
    user_name = session.get('username')
    session.modified = True # Force update
    print(f"🌐 Accessing Index | Session User: {user_name} (ID: {user_id})")
    
    # 1. Limiet: Haal op, forceer integer, default op 100 als het mislukt
    try:
        limit = int(request.args.get('limit', 100))
    except:
        limit = 100

    query = db.session.query(Train)
    
    # Als er geen zoekopdracht is, tonen we niks op het dashboard (Map-First)
    q = request.args.get('q', '').strip()
    if not q:
        trains = []
    else:
        # Zoek op treinnummer als er wel een q is op de index
        # [MODIFIED] Extract digits if present to handle "IC 2831"
        clean_q = ''.join(filter(str.isdigit, q))
        if clean_q:
            trains = query.filter(Train.train_number == clean_q).order_by(Train.date.desc()).limit(10).all()
        else:
            # Fallback if no digits (e.g. station name entered in wrong box? unlikely on index which assumes train nr)
            trains = []

    today = datetime.now().strftime("%Y-%m-%d")
    return render_template('index.html', 
                           trains=trains, 
                           mode="dashboard", 
                           args=request.args, 
                           today=today,
                           user_id=user_id,
                           username=user_name,
                           current_lang=get_locale(),
                           translations_json=json.dumps(TRANSLATIONS.get(get_locale(), TRANSLATIONS['nl'])))

@app.route('/api/plan_journey')
def api_plan_journey():
    from_name = request.args.get('from', '').strip()
    to_name = request.args.get('to', '').strip()
    if not from_name or not to_name: return jsonify([])
    
    raw_date = request.args.get('date', '')
    # [MODIFIED] Keep dashes to match DB format YYYY-MM-DD
    target_date = raw_date if raw_date else datetime.now().strftime("%Y-%m-%d")
    hour_param = request.args.get('hour', '')
    
    # 1. Resolve 'from' and 'to' stations
    def resolve_stations(name):
        search_query = name.lower()
        matched = StopTranslation.query.filter(
            or_(
                StopTranslation.translation.ilike(f"%{search_query}%"),
                StopTranslation.field_value.ilike(f"%{search_query}%")
            )
        ).all()
        ids = [m.stop_id for m in matched]
        names = [m.field_value for m in matched]
        return ids, names

    from_ids, from_names = resolve_stations(from_name)
    to_ids, to_names = resolve_stations(to_name)
    
    if not from_ids and not from_names: return jsonify([])
    if not to_ids and not to_names: return jsonify([])

    # 2. Query trains that stop at BOTH (A -> B order)
    t1 = aliased(TrainStop)
    t2 = aliased(TrainStop)
    
    f_ids = [t1.stop_id.like(f"{sid}%") for sid in from_ids] + [t1.stop_name == name for name in from_names]
    t_ids = [t2.stop_id.like(f"{sid}%") for sid in to_ids] + [t2.stop_name == name for name in to_names]
    
    query = db.session.query(Train, t1.stop_id.label('start_id'), t2.stop_id.label('end_id'))\
        .join(t1, Train.id == t1.train_id).join(t2, Train.id == t2.train_id)
    
    query = query.filter(Train.date == target_date)
    query = query.filter(or_(*f_ids))
    query = query.filter(or_(*t_ids))
    query = query.filter(t1.stop_sequence < t2.stop_sequence)
    
    if hour_param:
        query = query.filter(t1.departure_time >= f"{hour_param}:00:00")
        query = query.filter(t1.departure_time < f"{int(hour_param)+1:02d}:00:00")
    
    trains_data = query.order_by(t1.departure_time).limit(20).all()
    
    result = []
    for t, start_id, end_id in trains_data:
        # Get departure time at Station A
        stop_at_a = db.session.query(TrainStop).filter_by(train_id=t.id, stop_id=start_id).first()
        result.append({
            "id": t.id,
            "train_number": t.train_number,
            "destination": get_translated_stop(t.destination),
            "departure_time": stop_at_a.departure_time if stop_at_a else t.departure_time,
            "date": t.date,
            "has_comp": t.has_composition_data,
            "planned_start_stop": start_id,
            "planned_end_stop": end_id
        })
    return jsonify(result)
@app.route('/api/search_trains')
def api_search_trains():
    q = request.args.get('q', '').strip()
    # [MODIFIED] Do not strip dashes, match YYYY-MM-DD
    date_param = request.args.get('date', '')
    if not q: return jsonify([])
    
    # Check if 'q' looks like a number or a station name
    # [MODIFIED] Check if it contains digits (e.g. "IC 2831")
    import re
    digit_match = re.search(r'\d+', q)
    
    # If purely digits or looks like "Type Number"
    if q.isdigit() or (digit_match and len(digit_match.group()) >= 2 and len(q) < 10):
        clean_q = digit_match.group() if digit_match else q
        query = Train.query.filter(Train.train_number.like(f"%{clean_q}%"))
        if date_param: query = query.filter(Train.date == date_param)
        trains = query.order_by(Train.date.desc()).limit(20).all()
    else:
        # Station search: join with TrainStop and StopTranslation
        search_query = q.lower()
        matched_translations = StopTranslation.query.filter(
            or_(
                StopTranslation.translation.ilike(f"%{search_query}%"),
                StopTranslation.field_value.ilike(f"%{search_query}%")
            )
        ).all()
        
        target_stop_ids = [m.stop_id for m in matched_translations]
        station_names = [m.field_value for m in matched_translations]
        
        query = Train.query.join(TrainStop)
        if target_stop_ids:
            id_filters = [TrainStop.stop_id.like(f"{sid}%") for sid in target_stop_ids]
            name_filters = [TrainStop.stop_name == name for name in station_names]
            query = query.filter(or_(*id_filters, *name_filters))
        else:
            query = query.filter(TrainStop.stop_name.ilike(f"%{search_query}%"))
            
        trains = query.distinct().order_by(Train.date.desc(), Train.departure_time.desc()).limit(20).all()

    result = []
    for t in trains:
        result.append({
            "id": t.id,
            "train_number": t.train_number,
            "destination": get_translated_stop(t.destination),
            "departure_time": t.departure_time,
            "date": t.date,
            "has_comp": t.has_composition_data,
            "route_name": t.route_name
        })
    return jsonify(result)

@app.route('/api/station_board')
def api_station_board():
    station_name = request.args.get('station', '').strip()
    if not station_name: return jsonify([])
    
    raw_date = request.args.get('date', '')
    # [MODIFIED] Keep dashes to match DB format YYYY-MM-DD
    target_date = raw_date if raw_date else datetime.now().strftime("%Y-%m-%d")
    hour_param = request.args.get('hour', '')
    
    search_query = station_name.lower()
    matched = StopTranslation.query.filter(
        or_(
            StopTranslation.translation.ilike(f"%{search_query}%"),
            StopTranslation.field_value.ilike(f"%{search_query}%")
        )
    ).all()
    
    ids = [m.stop_id for m in matched]
    names = [m.field_value for m in matched]
    
    if not ids and not names:
        query = Train.query.join(TrainStop).filter(TrainStop.stop_name.ilike(f"%{search_query}%"))
    else:
        id_filters = [TrainStop.stop_id.like(f"{sid}%") for sid in ids]
        name_filters = [TrainStop.stop_name == name for name in names]
        query = Train.query.join(TrainStop).filter(or_(*id_filters, *name_filters))
        
    query = query.filter(Train.date == target_date)
    
    if hour_param:
        query = query.filter(TrainStop.departure_time >= f"{hour_param}:00:00")
        query = query.filter(TrainStop.departure_time < f"{int(hour_param)+1:02d}:00:00")
    else:
        # Default now + 1 hour as requested
        now_h = datetime.now().hour
        query = query.filter(TrainStop.departure_time >= f"{now_h:02d}:00:00")
        query = query.filter(TrainStop.departure_time < f"{(now_h+2)%24:02d}:00:00")

    trains_data = query.with_entities(Train, TrainStop.departure_time.label('stop_departure')).order_by(TrainStop.departure_time).limit(50).all()
    
    result = []
    for t, stop_time in trains_data:
        result.append({
            "id": t.id,
            "train_number": t.train_number,
            "destination": get_translated_stop(t.destination),
            "departure_time": stop_time, # Now using station-specific time
            "date": t.date,
            "has_comp": t.has_composition_data
        })
    return jsonify(result)

@app.route('/search')
def search():
    search_type = request.args.get('type', 'train_number')
    q = request.args.get('q', '').strip()
    # Haal datum op en converteer naar YYYYMMDD formaat
    raw_date = request.args.get('date', '') 
    # [MODIFIED] Use YYYY-MM-DD
    search_date = raw_date if raw_date else ''
    
    try: limit = int(request.args.get('limit', 100))
    except: limit = 100

    if not q: return redirect(url_for('index'))
    
    query = db.session.query(Train)
    target_stop_ids = []
    station_names_to_match = []

    if search_type == 'train_number':
        # [MODIFIED] Relaxed search
        clean_q = ''.join(filter(str.isdigit, q))
        if clean_q:
             query = query.filter(Train.train_number == clean_q)
        else:
             query = query.filter(Train.train_number == q)
    elif search_type == 'material_number':
        query = query.join(TrainUnit).filter(TrainUnit.material_number == q)
    elif search_type == 'material_type':
        query = query.join(TrainUnit).filter(TrainUnit.material_type.ilike(f"{q}%"))
    elif search_type == 'station':
        search_query = q.lower()
        # 1. Zoek matchende stations in de vertalingstabel
        matched = db.session.query(StopTranslation).filter(
            or_(
                StopTranslation.translation.ilike(f"%{search_query}%"),
                StopTranslation.field_value.ilike(f"%{search_query}%")
            )
        ).all()
        
        target_stop_ids = [m.stop_id for m in matched]
        station_names_to_match = [m.field_value for m in matched]

        # 2. Query: Zoek ritten die OF het ID OF de naam bevatten (robuuster)
        if target_stop_ids:
            # We gebruiken 'like' op het stop_id om platform-extensies te vangen (bv. 8833001%)
            # OF we zoeken op de officiële namen die we hebben gevonden
            id_filters = [TrainStop.stop_id.like(f"{sid}%") for sid in target_stop_ids]
            name_filters = [TrainStop.stop_name == name for name in station_names_to_match]
            
            query = query.join(TrainStop).filter(or_(*id_filters, *name_filters))
        else:
            # Volledige fallback op de tekstuele stationsnaam
            query = query.join(TrainStop).filter(TrainStop.stop_name.ilike(f"%{search_query}%"))
        
        # 3. Datum filtering
        if search_date:
            query = query.filter(Train.date == search_date)

    # Pas filters toe (LocalStorage parameters)
    # enforce_today=False zodat we in de geschiedenis kunnen kijken
    query = apply_filters(query, request.args, enforce_today=False if search_type == 'station' else True)
    
    if search_type == 'station' and search_date:
        results = query.distinct().order_by(Train.date.desc(), Train.departure_time.desc()).all()
    else:
        results = query.order_by(Train.date.desc(), Train.departure_time.desc()).limit(limit).all()

    # Gebruiker-specifieke tijden bepalen voor de tabel weergave
    for train in results:
        train.display_time = train.departure_time
        train.search_station_name = None
        
        if search_type == 'station':
            # Zoek de halte in deze rit die het beste matcht met wat de gebruiker zocht
            # We kijken eerst naar ID (prefix match voor platformen) en dan naar naam
            rel_stop = next((s for s in train.stops if 
                            any(s.stop_id.startswith(sid) for sid in target_stop_ids) or 
                            s.stop_name in station_names_to_match or 
                            search_query in s.stop_name.lower()), None)
            
            if rel_stop:
                # Toon vertrek, of aankomst als het de eindhalte is
                train.display_time = rel_stop.departure_time if rel_stop.departure_time else rel_stop.arrival_time
                train.search_station_name = rel_stop.stop_name

    # Sorteer op display_time als we op station zoeken voor een logische volgorde per dag
    if search_type == 'station':
        results.sort(key=lambda tr: (tr.date, tr.display_time), reverse=False)

    return render_template('index.html', 
                           trains=results, 
                           mode="search", 
                           args=request.args, 
                           q=q,
                           user_id=session.get('user_id'),
                           username=session.get('username'))


def ensure_infrabel_data():
    """Ensures Infrabel infrastructure tables are populated."""
    print("🔍 DEBUG: Checking infrastructure data state...")
    try:
        s2s_count = db.session.query(InfrabelStationToStation).count()
        op_count = db.session.query(InfrabelOperationalPoint).count()
        print(f"🔍 DEBUG: S2S Count={s2s_count}, OP Count={op_count}")
        
        if s2s_count == 0 or op_count == 0:
            print("🚀 Infrastructure tables are incomplete. Starting auto-ingestion...")
            
            # 1. Operational Points
            print("   📥 Downloading Infrabel Operational Points...")
            url_op = "https://opendata.infrabel.be/explore/dataset/operationele-punten-van-het-netwerk/download/?format=csv&timezone=Europe/Brussels&use_labels_for_header=true&csv_separator=%3B"
            r = requests.get(url_op, timeout=60)
            df_op = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';', dtype=str)
            
            # Clear if necessary to avoid merge conflicts on empty/partial datasets
            db.session.query(InfrabelOperationalPoint).delete()
            
            ptcar_map = {}
            for _, row in df_op.iterrows():
                geo_point = str(row.get('Geo Point', ''))
                lat, lon = None, None
                if ',' in geo_point:
                    parts = geo_point.split(',')
                    lat = float(parts[0].strip())
                    lon = float(parts[1].strip())
                
                sid = str(row.get('Symbolische naam', '')).strip()
                pid = str(row.get('PTCAR ID', '')).strip()
                if pid.endswith('.0'): pid = pid[:-2]
                
                if pid:
                    ptcar_map[pid] = sid

                op = InfrabelOperationalPoint(
                    id=sid,
                    ptcar_id=pid,
                    name_nl=row.get('Naam NL middelgroot'),
                    name_fr=row.get('Naam FR middelgroot'),
                    bts_code=row.get('Afkorting BVT NL kort'),
                    taf_tap_code=row.get('TAF/TAP code'),
                    latitude=lat,
                    longitude=lon
                )
                db.session.add(op)
            db.session.commit()
            print(f"   ✅ {len(ptcar_map)} Operational Points ingested.")

            # 2. Station to Station
            print("   📥 Downloading Infrabel segments (S2S)...")
            url_s2s = "https://opendata.infrabel.be/explore/dataset/station_to_station/download/?format=csv&timezone=Europe/Brussels&use_labels_for_header=true&csv_separator=%3B"
            r = requests.get(url_s2s, timeout=60)
            df_s2s = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';', dtype=str)
            
            db.session.query(InfrabelStationToStation).delete()
            
            mapped_count = 0
            for _, row in df_s2s.iterrows():
                from_ptcar = str(row.get('Station van vertrek (id)', '')).strip()
                to_ptcar = str(row.get('Aankomstation (id)', '')).strip()
                
                if from_ptcar.endswith('.0'): from_ptcar = from_ptcar[:-2]
                if to_ptcar.endswith('.0'): to_ptcar = to_ptcar[:-2]
                
                # Resolve to symbolic name
                from_id = ptcar_map.get(from_ptcar, from_ptcar)
                to_id = ptcar_map.get(to_ptcar, to_ptcar)
                
                if from_id != from_ptcar or to_id != to_ptcar:
                    mapped_count += 1

                length_val = row.get('Lengte')
                try:
                    length_val = float(length_val) if length_val and str(length_val).lower() != 'nan' else None
                except:
                    length_val = None

                s2s = InfrabelStationToStation(
                    stationfrom_id=from_id,
                    stationto_id=to_id,
                    length=length_val,
                    geom_wkt=row.get('Geo Shape')
                )
                db.session.add(s2s)
            db.session.commit()
            print(f"   ✅ segments (S2S) ingested. Mapped {mapped_count} symbolic names.")

            # 3. Station Mapping (using coords)
            print("   🗺️  Generating Station Mapping...")
            db.session.query(StationMapping).delete()
            all_ops = db.session.query(InfrabelOperationalPoint).all()
            
            stops_path = os.path.join(DATA_FOLDER, "stops.txt")
            if os.path.exists(stops_path):
                df_stops = pd.read_csv(stops_path, dtype=str)
                df_stops['stop_lat'] = pd.to_numeric(df_stops['stop_lat'])
                df_stops['stop_lon'] = pd.to_numeric(df_stops['stop_lon'])
                
                def haversine(lat1, lon1, lat2, lon2):
                    if lat1 is None or lon1 is None or lat2 is None or lon2 is None: return 999
                    R = 6371 
                    dlat = math.radians(lat2 - lat1)
                    dlon = math.radians(lon2 - lon1)
                    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
                    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                    return R * c

                mapping_count = 0
                for _, s in df_stops.iterrows():
                    slat, slon = s['stop_lat'], s['stop_lon']
                    if pd.isna(slat): continue
                    
                    best_op = None
                    min_dist = 999
                    for op in all_ops:
                        dist = haversine(slat, slon, op.latitude, op.longitude)
                        if dist < min_dist:
                            min_dist = dist
                            best_op = op
                    
                    # Threshold: 0.2 km (200 meter)
                    if best_op and min_dist < 0.2:
                        db.session.add(StationMapping(
                            sncb_id=s['stop_id'],
                            infrabel_id=best_op.id, # Symbolic name
                            name=s['stop_name']
                        ))
                        mapping_count += 1
                db.session.commit()
                print(f"   ✅ Mapping complete ({mapping_count} stations mapped).")
            
            print("🚀 Infrastructure auto-ingestion completed successfully.")
            
            print("🚀 Infrastructure auto-ingestion completed successfully.")
    except Exception as e:
        db.session.rollback()
        print(f"   ❌ Infrastructure ingestion failed: {e}")

def run_startup_tasks():
    with app.app_context():
        print("🚀 [STARTUP] Background tasks started...")
        db.create_all()
        migrate_db()
        
        # Ensure data is ready for manual lookup
        if not os.path.exists(os.path.join(DATA_FOLDER, 'trips.txt')):
             download_static_data()
        load_static_data()
        ensure_infrabel_data() # Ensure segments are present
        build_railway_graph()

        force_first_run = True
        last_sync_date = ""
        while True:
            now = datetime.now()
            current_date = now.strftime("%Y%m%d")
            current_hour = now.hour
            
            # Run sync if it's the first time OR if it's a new day and it's 4 AM or later
            if force_first_run or (current_date != last_sync_date and current_hour >= 4):
                print(f"🌅 [BACKEND] Running sync for {current_date} (Force: {force_first_run})...")
                # On startup (Force=True), skip existing days to unblock server quickly.
                # On daily schedule (Force=False), do full sync.
                populate_todays_schedule(fast_boot=force_first_run)
                last_sync_date = current_date
                force_first_run = False
                print("✅ [BACKEND] Sync completed.")
            
            # Check every hour
            time.sleep(3600)

if __name__ == '__main__':
    if not os.environ.get('FLASK_TESTING'):
        threading.Thread(target=run_startup_tasks, daemon=True).start()
    debug_mode = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=5000, debug=debug_mode, use_reloader=debug_mode)
else:
    if not os.environ.get('FLASK_TESTING'):
        threading.Thread(target=run_startup_tasks, daemon=True).start()
