import requests
import pandas as pd
import duckdb
import json
import datetime

# Headers
headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'}

# DuckDB connection
con = duckdb.connect("data/raw_layer.db")

con.execute("CREATE SEQUENCE IF NOT EXISTS raw_log_id_seq START 1") # ID Sequence for log table

# raw ingestion log table
con.execute("""
            CREATE TABLE IF NOT EXISTS raw_log(
                id INTEGER PRIMARY KEY DEFAULT nextval('raw_log_id_seq'),
                payload JSON,
                response_code VARCHAR(10), 
                table_name VARCHAR(50), 
                timestamp TIMESTAMP,
                status VARCHAR(30)
            )
            """)

# Events
print(f"[1/3] Loading Events tables...")
events_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

r_events = requests.get(events_url, headers = headers)

# The three events tables we want to pull: teams, players, and events
events = {
    "teams":{
        'key_name':'teams',
        'table_name':'raw_teams'
    },
    "players":{
        'key_name':'elements',
        'table_name':'raw_players'
    },
    "events":{
        'key_name':'events',
        'table_name':'raw_events'
    }
}

if r_events.status_code == 200:
    full_events_data = r_events.json()

    for element in events:
        key_name = events[element]['key_name']
        table_name = events[element]['table_name']

        payload_df = pd.DataFrame(full_events_data[key_name])
        payload_df['ingested_at']=datetime.datetime.now()

        # Log
        con.execute("""
                    INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                    VALUES(?, ?, ?, ?, ?)
                    """, [
                        json.dumps(full_events_data[key_name]),
                        str(r_events.status_code),
                        f'events.{table_name}',
                        datetime.datetime.now(),
                        'success'
                    ]
                )
        
        print(f"...{element} loaded: {len(payload_df)}")
        
        # Create Events DuckDB tables
        con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM payload_df WHERE 1=0
                    """)
        
        # Insert into Events tables
        con.execute(f"INSERT INTO {table_name} SELECT * FROM payload_df")
        print(f"...{element} loaded into [DuckDB]")
        print("✓ Events loaded successfully!")
else:
    print(f"x Events not loaded successfully, error code {r_events.status_code}")

    # Log
    con.execute("""
                INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                VALUES(?, ?, ?, ?, ?)
                """, [
                    None,
                    str(r_events.status_code),
                    'events',
                    datetime.datetime.now(),
                    f'failure: {r_events.status_code}'
                ])



# Elements / GW Data
print(f"[2/3] Loading GW data tables...")

all_gw_df = []

for gw in range(1,39):
    elements_url = f'https://fantasy.premierleague.com/api/event/{str(gw)}/live/'
    r_elements = requests.get(elements_url, headers = headers)

    if r_elements.status_code == 200:
        full_events_data = r_elements.json()
        df = pd.json_normalize(full_events_data['elements'])

        df['gameweek'] = gw
        all_gw_df.append(df)

        # Log
        con.execute("""
                    INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                    VALUES(?, ?, ?, ?, ?)
                    """, [
                        json.dumps(full_events_data),
                        str(r_elements.status_code),
                        f'gw_data_gw{gw}',
                        datetime.datetime.now(),
                        'success'
                    ]
                )

    else:
        print(f"x Player GW data for gw{gw} not loaded successfully, error code {r_elements.status_code}")
        
        # Log
        con.execute("""
                    INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                    VALUES(?, ?, ?, ?, ?)
                    """, [
                        None,
                        str(r_elements.status_code),
                        f'gw_data_gw{gw}',
                        datetime.datetime.now(),
                        'failure'
                    ]
                )
    
player_gw_df = pd.concat(all_gw_df, ignore_index=True)

if len(player_gw_df)>1: # TODO: Update to actual value
    print(f"...player gw data loaded: {len(player_gw_df)} rows")

    # Create GW DuckDB table
    con.execute(f"""
                CREATE TABLE IF NOT EXISTS raw_gw_data AS SELECT * FROM player_gw_df WHERE 1=0
                """)

    # Insert into GW table
    con.execute(f"INSERT INTO raw_gw_data SELECT * FROM player_gw_df")
    print(f"...raw_gw_data loaded into [DuckDB]")
    print("✓ Player GW data loaded successfully")

else:
    print("x Player GW data not loaded successfully")



# Fixtures
print(f"[3/3] Loading Fixtures tables...")

all_fixtures_df = []

for gw in range(1,39):
    fixtures_url = f'https://fantasy.premierleague.com/api/fixtures/?event={str(gw)}'
    r_fixtures = requests.get(fixtures_url, headers = headers)

    if r_fixtures.status_code == 200:
        full_fixtures_data = r_fixtures.json()
        df = pd.json_normalize(full_fixtures_data)
        df['gameweek'] = gw
        all_fixtures_df.append(df)

        # Log
        con.execute("""
                    INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                    VALUES(?, ?, ?, ?, ?)
                    """, [
                        json.dumps(full_fixtures_data),
                        str(r_fixtures.status_code),
                        f'fixtures_gw{gw}',
                        datetime.datetime.now(),
                        'success'
                    ]
                )


    else:
        # Log
        con.execute("""
                    INSERT INTO raw_log(payload, response_code, table_name, timestamp, status)
                    VALUES(?, ?, ?, ?, ?)
                    """, [
                        None,
                        str(r_fixtures.status_code),
                        f'fixtures_gw{gw}',
                        datetime.datetime.now(),
                        'failure'
                    ]
                )
        print(f"x Player GW data not loaded successfully, error code {r_fixtures.status_code}")
    
    
fixtures_gw_df = pd.concat(all_fixtures_df, ignore_index=True)

if len(fixtures_gw_df)>1: # TODO: Update to actual value

    # Create GW DuckDB table
    con.execute(f"""
                CREATE TABLE IF NOT EXISTS raw_fixtures AS SELECT * FROM fixtures_gw_df WHERE 1=0
                """)

    # Insert into GW table
    con.execute(f"INSERT INTO raw_fixtures SELECT * FROM fixtures_gw_df")

    print(f"...fixtures gw data loaded: {len(fixtures_gw_df)} rows")
    print("✓ Fixtures data loaded successfully")
else:
    print("x Fixtures data not loaded successfully")

print("*Raw Layer Ingestion Complete")