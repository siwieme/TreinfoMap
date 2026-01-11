
from main import app, db, InfrabelStationToStation

with app.app_context():
    print('Checking direct connections LEUVEN (FL) <-> ANS (FANS)')
    # Check FL -> FANS
    segs = InfrabelStationToStation.query.filter_by(stationfrom_id='FL', stationto_id='FANS').all()
    for s in segs:
        print(f'DIRECT FL->FANS: ID={s.id} Len={s.length}')
        
    segs = InfrabelStationToStation.query.filter_by(stationfrom_id='FANS', stationto_id='FL').all()
    for s in segs:
        print(f'DIRECT FANS->FL: ID={s.id} Len={s.length}')

    # Check for anything else connecting to FANS that looks like HSL (long segments?)
    print('
Checking all connections to ANS (FANS)')
    segs = InfrabelStationToStation.query.filter((InfrabelStationToStation.stationfrom_id == 'FANS') | (InfrabelStationToStation.stationto_id == 'FANS')).all()
    for s in segs:
        print(f'{s.stationfrom_id} <-> {s.stationto_id} : {s.length} (ID: {s.id})')

