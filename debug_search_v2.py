from main import app, db, Train
import sys

# Set context
ctx = app.app_context()
ctx.push()

q = "2831"
date_str = "2026-01-10"
date_param = date_str.replace('-', '')

print(f"DEBUG: Searching for q='{q}', date='{date_param}'")

# 1. Check if train exists at all
all_trains = db.session.query(Train).filter(Train.train_number == q).all()
print(f"Total entries for {q}: {len(all_trains)}")
if all_trains:
    print(f"Sample dates: {[t.date for t in all_trains[:5]]}")

# 2. Check precise query
clean_q = q # simplistic for this test
query = db.session.query(Train).filter(Train.train_number.like(f"%{clean_q}%"))
query = query.filter(Train.date == date_param)
results = query.all()
print(f"Results with date filter: {len(results)}")

# 3. Check what IS in the DB for this date
count_today = db.session.query(Train).filter(Train.date == date_param).count()
print(f"Total trains on {date_param}: {count_today}")

# 4. Station Board Check
print("Checking Station Board for 'Lokeren'")
from main import StopTranslation, TrainStop
# Just check raw stops for Lokeren
stops = db.session.query(TrainStop).filter(TrainStop.stop_name.ilike("%Lokeren%")).join(Train).filter(Train.date == date_param).count()
print(f"Stops in Lokeren on {date_param}: {stops}")
