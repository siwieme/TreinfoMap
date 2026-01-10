print("DEBUG: Script started")
import sys
try:
    print("DEBUG: Importing main...")
    from main import app, db, Train, TrainStop, StopTranslation
    print("DEBUG: Import successful")

    with app.app_context():
        print("DEBUG: Context active")
        train_count = db.session.query(Train).count()
        print(f"Trains: {train_count}")
        
except Exception as e:
    print(f"FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
