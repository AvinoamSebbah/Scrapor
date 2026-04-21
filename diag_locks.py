import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("""
  SELECT pid, state, wait_event_type, wait_event,
         LEFT(query, 100) as query
  FROM pg_stat_activity
  WHERE state <> 'idle'
  AND pid <> pg_backend_pid()
  ORDER BY query_start
""")
rows = cur.fetchall()
print("Active queries:")
for r in rows:
    print(r)

cur.execute("""
  SELECT blocking.pid AS blocking_pid,
         blocked.pid AS blocked_pid,
         LEFT(blocked.query, 80) AS blocked_query,
         LEFT(blocking.query, 80) AS blocking_query
  FROM pg_stat_activity blocked
  JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
""")
rows = cur.fetchall()
print("\nBlocking relationships:")
for r in rows:
    print(r)

conn.close()
print("Done.")
