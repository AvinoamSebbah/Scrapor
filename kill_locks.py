import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Terminate the idle-in-transaction session (images-only script stuck)
# and the orphaned ALTER TABLE from the killed terminal
pids_to_kill = [76370, 76381]

for pid in pids_to_kill:
    cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    result = cur.fetchone()
    print(f"Terminate PID {pid}: {result[0]}")

conn.commit()
conn.close()
print("Done.")
