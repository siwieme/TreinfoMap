
from main import app, db, InfrabelStationToStation

try:
    with app.app_context():
        print('Checking direct connections LEUVEN (FL) <-> ANS (FANS)')
        # Check FL -> FANS
        segs = InfrabelStationToStation.query.filter_by(stationfrom_id='FL', stationto_id='FANS').all()
        for s in segs:
            print(f'DIRECT FL->FANS: ID={s.id} Len={s.length} GeomLen={len(s.geom_wkt) if s.geom_wkt else 0}')
            
        segs = InfrabelStationToStation.query.filter_by(stationfrom_id='FANS', stationto_id='FL').all()
        for s in segs:
            print(f'DIRECT FANS->FL: ID={s.id} Len={s.length} GeomLen={len(s.geom_wkt) if s.geom_wkt else 0}')
        
        if not segs:
            print('No direct connections found.')

        print('
Checking segments with length > 10 (Long segments might be HSL)')
        # Heuristic: HSL segments are often long continuous blocks without stops
        # or we check connectivity around FL and FANS
        
        print('
Checking connectivity from FL (Leuven):')
        fl_segs = InfrabelStationToStation.query.filter_by(stationfrom_id='FL').all()
        for s in fl_segs:
            print(f'FL -> {s.stationto_id} (Len: {s.length})')

except Exception as e:
    print(f'Error: {e}')

