import os
import psycopg2

db_url = os.environ.get('DATABASE_URL', 'postgresql://user:password@localhost:5432/treinfomap')
print(f"Connecting to: {db_url}")

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM train WHERE train_number LIKE '%2831%' AND date = '20260110';")
    print(f"Count for 2831 on 20260110: {cur.fetchone()[0]}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
