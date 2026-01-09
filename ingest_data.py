print("Starting ingest_data.py...")
import os
import requests
import zipfile
import io
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, DateTime, ForeignKey, UniqueConstraint, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# Configuration
DB_URL = os.environ.get('DATABASE_URL', 'postgresql://infrabel_user:infrabel_password@localhost:5432/infrabel_kaart')
GTFS_STATIC_URL = "https://sncb-opendata.hafas.de/gtfs/static/d22ad6759ee25bg84ddb6c818g4dc4de_TC"
INFRABEL_API_BASE = "https://opendata.infrabel.be/explore/dataset/"

Base = declarative_base()

# Models
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)

class Train(Base):
    __tablename__ = 'trains'
    id = Column(Integer, primary_key=True)
    train_number = Column(String(20), index=True)
    date = Column(String(20), index=True) # YYYYMMDD
    trip_id = Column(String(100), index=True)
    route_name = Column(String(200))
    destination = Column(String(100))
    departure_time = Column(String(20))
    arrival_time = Column(String(20))
    status = Column(String(20), default="SCHEDULED")
    delay = Column(Integer, default=0)
    realtime_trip_id = Column(String(100), index=True, nullable=True)
    composition_fetched = Column(Boolean, default=False)
    has_composition_data = Column(Boolean, default=False)
    
    units = relationship('TrainUnit', backref='train')
    stops = relationship('TrainStop', backref='train')
    
    __table_args__ = (UniqueConstraint('train_number', 'date', name='_train_number_date_uc'),)

class TrainUnit(Base):
    __tablename__ = 'train_units'
    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('trains.id'))
    position = Column(String(10))
    material_number = Column(String(50))
    material_type = Column(String(50))
    has_bike = Column(Boolean, default=False)
    has_airco = Column(Boolean, default=False)

class TrainStop(Base):
    __tablename__ = 'train_stops'
    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('trains.id'))
    stop_id = Column(String(50), index=True)
    stop_name = Column(String(200), index=True)
    stop_type = Column(String(20)) # STOP, VERTREK, AANKOMST, DOORRIT
    arrival_time = Column(String(20))
    departure_time = Column(String(20))
    stop_sequence = Column(Integer)
    
class Journey(Base):
    __tablename__ = 'journeys'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    train_id = Column(Integer, ForeignKey('trains.id'))
    unit_id = Column(Integer, ForeignKey('train_units.id'), nullable=True)
    start_stop_id = Column(String(50))
    end_stop_id = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

class InfrabelOperationalPoint(Base):
    __tablename__ = 'infrabel_operational_points'
    id = Column(String(50), primary_key=True)
    ptcar_id = Column(String(20), index=True)
    name_nl = Column(String(200))
    name_fr = Column(String(200))
    bts_code = Column(String(20))
    taf_tap_code = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)

class InfrabelStationToStation(Base):
    __tablename__ = 'infrabel_station_to_station'
    id = Column(Integer, primary_key=True)
    stationfrom_id = Column(String(50))
    stationto_id = Column(String(50))
    length = Column(Float)
    # Storing GeoJSON or WKT as text for simplicity, can be cast to geometry
    geom_wkt = Column(String) 

class StationMapping(Base):
    __tablename__ = 'station_mapping'
    sncb_id = Column(String(100), primary_key=True) # SNCB:8814001
    infrabel_id = Column(String(50), index=True) # FBMZ
    name = Column(String(200))

# Helper functions
def get_session():
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def download_gtfs():
    print("Downloading NMBS GTFS...")
    r = requests.get(GTFS_STATIC_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("gtfs_data")
    print("GTFS downloaded and extracted.")

def download_infrabel_csv(dataset_name):
    print(f"Downloading Infrabel dataset: {dataset_name}...")
    url = f"{INFRABEL_API_BASE}{dataset_name}/download/?format=csv&timezone=Europe/Brussels&use_labels_for_header=true&csv_separator=%3B"
    r = requests.get(url)
    filename = f"{dataset_name}.csv"
    with open(filename, 'wb') as f:
        f.write(r.content)
    print(f"Dataset {dataset_name} downloaded.")
    return filename

def ingest_gtfs_today(session):
    print("Ingesting GTFS for today...")
    if not os.path.exists("gtfs_data"):
        download_gtfs()
    
    today_str = datetime.now().strftime("%Y%m%d")
    dtype_cfg = str
    
    try:
        trips = pd.read_csv("gtfs_data/trips.txt", dtype=dtype_cfg)
        routes = pd.read_csv("gtfs_data/routes.txt", dtype=dtype_cfg)
        stop_times = pd.read_csv("gtfs_data/stop_times.txt", dtype=dtype_cfg)
        stops = pd.read_csv("gtfs_data/stops.txt", dtype=dtype_cfg)
        cal_dates = pd.read_csv("gtfs_data/calendar_dates.txt", dtype=dtype_cfg)
        
        # Ensure numeric types for classification
        stop_times['pickup_type'] = pd.to_numeric(stop_times['pickup_type'], errors='coerce').fillna(0).astype(int)
        stop_times['drop_off_type'] = pd.to_numeric(stop_times['drop_off_type'], errors='coerce').fillna(0).astype(int)
        
        # Filter active services for today (exception_type = 1)
        active_services = cal_dates[(cal_dates['date'] == today_str) & (cal_dates['exception_type'] == '1')]['service_id'].tolist()
        
        # Filter trips for today
        trips_today = trips[trips['service_id'].isin(active_services)].copy()
        
        if trips_today.empty:
            print(f"No active services found for {today_str}.")
            return

        # Ranking logic based on user's SQL
        def calculate_rank(row, target_date_str):
            tid = str(row['trip_id'])
            date_at_end_match = re.search(r':(\d{8})$', tid)
            if date_at_end_match:
                found_date = date_at_end_match.group(1)
                if found_date == target_date_str: return 1
                return 2
            if re.search(r':\d{8}:', tid): return 3
            return 4

        import re
        trips_today['rank'] = trips_today.apply(calculate_rank, axis=1, args=(today_str,))
        
        # Merge with routes to get names
        merged = trips_today.merge(routes, on='route_id')
        
        # Get stop_times with sequence 1 (departure) and last (arrival)
        st_first = stop_times[stop_times['stop_sequence'] == '1'].copy()
        
        # For last stop, we need to find the max sequence per trip
        stop_times['stop_sequence_int'] = pd.to_numeric(stop_times['stop_sequence'])
        st_last_idx = stop_times.groupby('trip_id')['stop_sequence_int'].idxmax()
        st_last = stop_times.loc[st_last_idx].copy()
        
        # Sorteren op trip_short_name, rank, en dan aantal stops (meeste stops eerst als backup)
        # Maar de user query gebruikt ROW_NUMBER() over trip_short_name, date partition.
        merged.sort_values(by=['trip_short_name', 'rank'], ascending=[True, True], inplace=True)
        final_trips = merged.drop_duplicates(subset=['trip_short_name'], keep='first')
        
        print(f"Processing {len(final_trips)} unique trains...")
        
        for _, r in final_trips.iterrows():
            train_num = r.get('trip_short_name')
            if not train_num: continue
            
            # Start and End times
            start_stop = st_first[st_first['trip_id'] == r['trip_id']]
            end_stop = st_last[st_last['trip_id'] == r['trip_id']]
            
            if start_stop.empty or end_stop.empty: continue
            
            dep_time = start_stop.iloc[0]['departure_time']
            arr_time = end_stop.iloc[0]['arrival_time']
            
            t = Train(
                train_number=train_num,
                date=today_str,
                trip_id=r['trip_id'],
                route_name=r.get('route_long_name'),
                destination=r.get('trip_headsign'),
                departure_time=dep_time,
                arrival_time=arr_time
            )
            t = session.merge(t)
            session.flush() # Ensure we have t.id
            
            # Ingest all stops for this trip
            this_trip_stops = stop_times[stop_times['trip_id'] == r['trip_id']].sort_values('stop_sequence_int')
            for _, s_row in this_trip_stops.iterrows():
                stop_name = stops[stops['stop_id'] == s_row['stop_id']].iloc[0]['stop_name']
                
                # Classify stop type
                dt = s_row['drop_off_type']
                pt = s_row['pickup_type']
                
                if dt == 1 and pt == 0: stop_type = 'VERTREK'
                elif dt == 0 and pt == 1: stop_type = 'AANKOMST'
                elif dt == 1 and pt == 1: stop_type = 'DOORRIT'
                else: stop_type = 'STOP'
                
                ts = TrainStop(
                    train_id=t.id,
                    stop_id=s_row['stop_id'],
                    stop_name=stop_name,
                    stop_type=stop_type,
                    arrival_time=s_row['arrival_time'],
                    departure_time=s_row['departure_time'],
                    stop_sequence=int(s_row['stop_sequence'])
                )
                session.merge(ts)
        
        session.commit()
        print("GTFS ingestion complete.")
        
    except Exception as e:
        session.rollback()
        print(f"Error ingesting GTFS: {e}")

def ingest_infrabel_operational_points(session):
    filename = download_infrabel_csv("operationele-punten-van-het-netwerk")
    df = pd.read_csv(filename, sep=";", dtype=str)
    print(f"Ingesting {len(df)} operational points...")
    for _, row in df.iterrows():
        geo_point = str(row.get('Geo Point', ''))
        lat, lon = None, None
        if ',' in geo_point:
            parts = geo_point.split(',')
            lat = float(parts[0].strip())
            lon = float(parts[1].strip())
            
        op = InfrabelOperationalPoint(
            id=str(row.get('Symbolische naam')),
            ptcar_id=str(row.get('PTCAR ID', '')).replace('.0', '') if pd.notna(row.get('PTCAR ID')) else None,
            name_nl=row.get('Naam NL middelgroot'),
            name_fr=row.get('Naam FR middelgroot'),
            bts_code=row.get('Afkorting BVT NL kort'),
            taf_tap_code=row.get('TAF/TAP code'),
            latitude=lat,
            longitude=lon
        )
        session.merge(op)
    session.commit()
    print("Operational points ingested.")

def ingest_infrabel_station_to_station(session):
    # Clear existing segments
    session.query(InfrabelStationToStation).delete()
    session.commit()
    
    filename = download_infrabel_csv("station_to_station")
    df = pd.read_csv(filename, sep=";", dtype=str)
    
    # Pre-load operational point mapping (PTCAR ID -> Symbolic Name)
    ops = session.query(InfrabelOperationalPoint).all()
    ptcar_map = {str(op.ptcar_id).strip(): op.id for op in ops if op.ptcar_id}
    
    print(f"Ingesting {len(df)} station to station segments...")
    mapped_count = 0
    for _, row in df.iterrows():
        from_ptcar = str(row.get('Station van vertrek (id)', '')).strip()
        to_ptcar = str(row.get('Aankomstation (id)', '')).strip()
        
        # Remove trailing .0 if present (Pandas legacy)
        if from_ptcar.endswith('.0'): from_ptcar = from_ptcar[:-2]
        if to_ptcar.endswith('.0'): to_ptcar = to_ptcar[:-2]
        
        # Resolve to symbolic name if possible, else keep original
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
        session.add(s2s)
        
    session.commit()
    print(f"Station to station ingested. Successfully mapped {mapped_count} symbolic names from PTCAR IDs.")

import math

def haversine(lat1, lon1, lat2, lon2):
    """Calculates distance between two points in km."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None: return 999
    R = 6371 # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def ingest_station_mapping(session):
    print("Ingesting station mapping using coordinate matching...")
    
    # 1. Load Infrabel Operational Points from DB
    ops = session.query(InfrabelOperationalPoint).all()
    if not ops:
        print("Error: No Infrabel operational points found in DB.")
        return
        
    # 2. Load GTFS Stops from file
    stops_path = os.path.join("gtfs_data", "stops.txt")
    if not os.path.exists(stops_path):
        print("Error: stops.txt not found.")
        return
        
    df_stops = pd.read_csv(stops_path, dtype=str)
    df_stops['stop_lat'] = pd.to_numeric(df_stops['stop_lat'])
    df_stops['stop_lon'] = pd.to_numeric(df_stops['stop_lon'])
    
    print(f"Comparing {len(df_stops)} GTFS stops with {len(ops)} Infrabel points...")
    
    mapping_count = 0
    for _, s in df_stops.iterrows():
        slat, slon = s['stop_lat'], s['stop_lon']
        if pd.isna(slat): continue
        
        # Find closest Infrabel point
        best_op = None
        min_dist = 999
        
        for op in ops:
            dist = haversine(slat, slon, op.latitude, op.longitude)
            if dist < min_dist:
                min_dist = dist
                best_op = op
        
        # Threshold: 0.75 km (750 meter)
        if best_op and min_dist < 0.75:
            m = StationMapping(
                sncb_id=s['stop_id'],
                infrabel_id=best_op.id, # Symbolic Name (e.g. FGSP)
                name=s['stop_name']
            )
            session.merge(m)
            mapping_count += 1
            if mapping_count % 500 == 0:
                print(f"   Mapped {mapping_count} stations...")
                session.flush()

    session.commit()
    print(f"Station mapping complete. {mapping_count} stations mapped.")

if __name__ == "__main__":
    print("Main block started...")
    session = get_session()
    print("Database session established.")
    
    # download_gtfs()
    # ingest_gtfs_today(session)
    ingest_gtfs_today(session)
    ingest_infrabel_operational_points(session)
    ingest_infrabel_station_to_station(session)
    ingest_station_mapping(session)
    
    session.close()
