import os
import sys
import pandas as pd
from datetime import datetime
from main import app, db, Train, TrainStop, sync_day

def delete_day(date_str):
    """Deletes all trains and stops for a specific date (YYYYMMDD)."""
    with app.app_context():
        trains = Train.query.filter_by(date=date_str).all()
        if not trains:
            print(f"❌ Geen ritten gevonden voor {date_str}.")
            return
        
        count = len(trains)
        for t in trains:
            db.session.delete(t)
        db.session.commit()
        print(f"✅ {count} ritten (en bijbehorende haltes) verwijderd voor {date_str}.")

def load_day(date_str):
    """Loads schedules for a specific date (YYYYMMDD)."""
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        print("❌ Ongeldig datumformaat. Gebruik YYYYMMDD.")
        return

    with app.app_context():
        sync_day(dt)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Gebruik: python manage_data.py [load|delete] [YYYYMMDD]")
        sys.exit(1)

    action = sys.argv[1].lower()
    date_val = sys.argv[2]

    if action == "load":
        load_day(date_val)
    elif action == "delete":
        delete_day(date_val)
    else:
        print("❌ Onbekende actie. Gebruik 'load' of 'delete'.")
