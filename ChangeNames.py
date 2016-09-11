import psycopg2 as pg
import os

db = pg.connect(dsn="postgresql://lustcom:qwerty@localhost:5432/lustration_dev")
cur = db.cursor()
query = "SELECT id, declaration_report_id FROM attachments WHERE is_declaration;"

cur.execute(query)

f = open('script.sh', 'w')

for row in cur.fetchall():
    old = str(row[1]) + "_0_0_" + str(row[0])
    new = '0_0_' + str(row[1]) + '_' + str(row[0])
    f.write("mv %s %s\n" % (old, new))

f.close()
db.close()
