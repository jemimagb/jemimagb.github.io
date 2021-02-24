import sqlite3 as lite

import pandas
import pandas.io.sql as pd_lite

''' Create the stations database '''
with lite.connect("./Databases/uk_stations.db") as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS Stations")
    stations = pandas.read_csv('./datasets/uk_stations.csv', encoding="utf-8")
    pd_lite.to_sql(stations, "Stations", con)

''' Create the emissions factors database '''
with lite.connect("./Databases/carbon_emissions.db") as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS Activities")

    activities = ["Bus", "Car_Type", "Car_Size",
                  "Flights", "Train", "Motorbike", "Taxi"]

    for activity in activities:
        cur.execute("DROP TABLE IF EXISTS %s" % activity)

    cur.execute("CREATE TABLE Activities(Id INTEGER PRIMARY KEY, Activity TEXT)")

    for activity in activities:
        cur.execute("INSERT INTO Activities(Activity) VALUES(?)", (activity,))
        activity_data = pandas.read_csv('./datasets/%s.csv' % activity)
        pd_lite.to_sql(activity_data, activity, con)

    cur.execute("SELECT Activity FROM Activities")
    rows = cur.fetchall()
    for row in rows:
        print(row[0])
