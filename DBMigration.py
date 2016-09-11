import psycopg2 as pg

dbOld = pg.connect(dsn="postgresql://postgres@localhost:5432/lustold")

dbNew = pg.connect(dsn="postgresql://lustcom:qwerty@127.0.0.1:5432/lustration_dev")
curOld = dbOld.cursor()
curNew = dbNew.cursor()

try:
    curOld.execute('SELECT p.id, p.first_name, p.last_name, p.patronym, p.created_at, p.updated_at, '
                   'p.is_legacy, p.is_criminal, p.incoming_number, p.incoming_date '
                   'FROM people p;')
    officials = curOld.fetchall()
    for row in officials:

        if not all([i not in ['', None] for i in row[:2] + row[3:6]]):
            print(row)
            continue

        curOld.execute("SELECT position_info, c.id, position_id, c.district_id, pp.start_of_work, pp.end_of_work, pp.note, pp.person_id, pp.is_legacy, pp.is_criminal from person_positions pp "
                       "JOIN cities c ON c.district_id=pp.district_id "
                       "WHERE pp.person_id=%i AND ((c.region_center = TRUE AND c.id != 168 AND c.id !=30420) OR c.district_id = 701 OR c.district_id = 702) ORDER BY end_of_work DESC;" % row[0])
        rowsPos = curOld.fetchall()
        for i in range(len(rowsPos)):
            if rowsPos[i][3] == 701:
                rowsPos[i] = rowsPos[i][:1] + (30295, ) + rowsPos[i][2:]
            if rowsPos[i][3] == 702:
                rowsPos[i] = rowsPos[i][:1] + (30420, ) + rowsPos[i][2:]
            if not rowsPos[i][5]:
                rowsPos[i] = rowsPos[i][:5] + ("2014-02-01", ) + rowsPos[i][6:]

        rowPos = rowsPos[0]
        qstr = ",".join(
            ["'" + i.replace("'", "''") + "'" for i in row[1:4]] + ["timestamp '%s'" % str(i) for i in row[4:6]] +
            # [str(is_l)] + [str(is_c)] + [("'" + row[8] + "'" if (row[8]) else 'NULL')] +
            [("date '" + str(rowPos[5]) + "' " if (rowPos[5]) else 'NULL')])

        # print(qstr, row[0])
        query = ("INSERT INTO people (first_name, last_name, patronym, created_at, updated_at, "
                 "incoming_date, post, location_id, position_id, "
                 "is_verified, reason, is_public) "
                 "VALUES(%s, %s, 3, 'Перенесено з люстраційного реєстру', TRUE) RETURNING id;" %
                 (qstr, ("'%s'," % rowPos[0].replace("'", "''")) + str(rowPos[1]) + "," + str(rowPos[2])))

        # print(query)

        curNew.execute(query)
        new_off_id = curNew.fetchone()[0]

        query = ("INSERT INTO official_events (status, note, official_id, admin_id) "
                 "VALUES(5, 'Перенесено з люстраційного реєстру', %i, 1);" %
                 new_off_id)

        curNew.execute(query)


        for r in rowsPos:
            ct = 0 if r[8] else 1
            query = ("INSERT INTO public_positions (title, official_id, position_id, location_id, start_date, "
                    "end_date, crime_type, ban_type, note) "
                    "VALUES(%s, %i, %i, %i, %s, %s, %i, 0, %s);" %
                    ("'%s'" % r[0].replace("'", "''"), new_off_id, r[2], r[1], "timestamp '%s'" % str(r[4]), "timestamp '%s'" % str(r[5]) if (r[5]) else "NULL", ct, "'%s'" % r[6].replace("'", "''") if (r[6] != None) else "''"))
            curNew.execute(query)


    dbNew.commit()


except pg.DatabaseError as e:
    print(e)
    dbNew.rollback()


dbOld.close()
dbNew.close()
