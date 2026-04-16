import datetime
import duckdb

con = duckdb.connect("../data/fpl-mentat.duckdb")

tables = [
        'raw_teams',
        'raw_players',
        'raw_events',
        'raw_gw_data',
        'raw_fixtures'
        ]

for table in tables:
    con.execute(f"DROP TABLE IF EXISTS {table}")
    
    con.execute(f"INSERT INTO raw_log(payload, response_code, table_name, timestamp, status) VALUES(?, ?, ?, ?, ?)", [
        None,
        None,
        str(table),
        datetime.datetime.now(),
        'dropped'
        ]
    )


print('All raw tables dropped')
