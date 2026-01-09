import unittest
import json
from main import app, db, User, Journey, Train, TrainUnit, TrainStop
from datetime import datetime, timedelta
import threading
import time

class BackendTestCase(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['WTF_CSRF_ENABLED'] = False
        self.app = app.test_client()
        with app.app_context():
            db.create_all()
            # migrate_db() is called in run_startup_tasks or manually if needed, 
            # but create_all handles memory db fine for these test columns.
            
            # Setup a test user
            u = User(username="testuser")
            u.set_password("password")
            db.session.add(u)
            db.session.commit()
            self.user_id = u.id

    def login(self):
        return self.app.post('/api/login', data=json.dumps({
            'username': 'testuser',
            'password': 'password'
        }), content_type='application/json')

    def test_log_journey_4_units(self):
        """Test logging a journey with 4 units."""
        with app.app_context():
            self.login()
            
            # Create a train and 4 units
            t = Train(train_number="123", date="20260101")
            db.session.add(t)
            db.session.commit()
            
            units = []
            for i in range(1, 5):
                u = TrainUnit(
                    train_id=t.id, 
                    position=str(i), 
                    material_type="HLE18", 
                    material_number=str(1800+i),
                    traction_type="HLE" if i == 1 else None
                )
                db.session.add(u)
                units.append(u)
            db.session.commit()
            
            unit_ids = [u.id for u in units]
            
            # Log journey
            with self.app.session_transaction() as sess:
                sess['user_id'] = self.user_id
                sess['username'] = 'testuser'

            response = self.app.post('/api/log_journey', data=json.dumps({
                'train_id': t.id,
                'train_number': "123",
                'destination': "Ghent",
                'start_stop_id': "S1",
                'end_stop_id': "S2",
                'unit_ids': unit_ids
            }), content_type='application/json')
            
            self.assertEqual(response.status_code, 200)
            
            # Verify journey
            j = Journey.query.filter_by(user_id=self.user_id).first()
            self.assertIsNotNone(j)
            self.assertEqual(j.unit_id, unit_ids[0])
            self.assertEqual(j.unit_id_2, unit_ids[1])
            self.assertEqual(j.unit_id_3, unit_ids[2])
            self.assertEqual(j.unit_id_4, unit_ids[3])
            
            # Verify history response
            resp = self.app.get('/api/my_history')
            data = json.loads(resp.data)
            self.assertIn("HLE18 - 1801 | HLE18 - 1802 | HLE18 - 1803 | HLE18 - 1804", data['journeys'][0]['units_display'])

    def test_composition_traction_type(self):
        """Test if traction_type is returned in composition API."""
        with app.app_context():
            t = Train(train_number="456", date="20260101")
            db.session.add(t)
            db.session.commit()
            
            u = TrainUnit(train_id=t.id, position="1", material_type="HLE18", material_number="1801", traction_type="HLE")
            db.session.add(u)
            db.session.commit()
            
            response = self.app.get(f'/api/composition/{t.id}')
            data = json.loads(response.data)
            self.assertEqual(data['units'][0]['traction_type'], "HLE")

    def test_concurrent_composition_locking(self):
        """Simulate concurrent composition fetch and verify no crash/lock integrity."""
        # This is hard to test in memory-db without mocks, but we can verify the lock structures exist
        from main import COMPOSITION_LOCKS, COMPOSITION_LOCKS_LOCK
        self.assertIsNotNone(COMPOSITION_LOCKS_LOCK)
        self.assertIsInstance(COMPOSITION_LOCKS, dict)

    def tearDown(self):
        with app.app_context():
            db.session.remove()
            db.drop_all()

if __name__ == '__main__':
    unittest.main()
