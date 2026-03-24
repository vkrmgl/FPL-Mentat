import requests
import pandas as pd
import duckdb

headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'}

con = duckdb.connect("raw_layer.db")

con.execute("CREATE TABLE IF NOT EXISTS raw_log")

# Events
print(f"[1/3] Loading Events tables...")
events_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

r_events = requests.get(events_url, headers = headers)

if r_events.status_code == 200:
    full_events_data = r_events.json()

    

    teams_df = pd.DataFrame(full_events_data['teams'])
    players_df = pd.DataFrame(full_events_data['elements'])
    fixtures_df = pd.DataFrame(full_events_data['events'])

    print(f"✓ teams loaded: {len(teams_df)}")
    print(f"✓ players loaded: {len(players_df)}")
    print(f"✓ fixtures loaded: {len(fixtures_df)}")
    print("✅ Events loaded successfully")
else: 
    print(f"❌ Events not loaded successfully, error code {r_events.status_code}")



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
    else:
        print(f"❌ Player GW data not loaded successfully, error code {r_elements.status_code}")
    
player_gw_df = pd.concat(all_gw_df, ignore_index=True)

if len(player_gw_df)>1:
    print(f"✓ player gw data loaded: {len(player_gw_df)} rows")
    print("✅ Player GW data loaded successfully")
else:
    print("❌ Player GW data not loaded successfully")



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
    else:
        print(f"❌ Player GW data not loaded successfully, error code {r_fixtures.status_code}")
    
    
fixtures_gw_df = pd.concat(all_fixtures_df, ignore_index=True)

if len(fixtures_gw_df)>1:
    print(f"✓ fixtures gw data loaded: {len(fixtures_gw_df)} rows")
    print("✅ Fixtures data loaded successfully")
else:
    print("❌ Fixtures data not loaded successfully")


print("Loading dataframes into Bronze layer(DuckDB)")


