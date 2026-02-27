import sqlite3
import json
import pandas as pd
import os
from datetime import datetime

db_path = "databases/MLB_Betting.db"
data_dir = 'historic_odds'

conn = sqlite3.connect(db_path)

for year in range(2022, 2026):
    print(f"\n--- Analyzing {year} Season ---")
    
    # 1. Get OldGames data and determine Regular Season bounds
    query = f"SELECT game_id, date_time, home_team, away_team FROM OldGames WHERE season = '{year}'"
    old_games_df = pd.read_sql(query, conn)
    
    if old_games_df.empty:
        print(f"No data in OldGames for {year}.")
        continue
        
    # Convert UTC to US/Eastern to match our actual calendar dates
    old_games_df['date_time'] = pd.to_datetime(old_games_df['date_time'], utc=True)
    old_games_df['local_date'] = old_games_df['date_time'].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d')
    
    reg_season_start = old_games_df['local_date'].min()
    reg_season_end = old_games_df['local_date'].max()
    
    db_game_count = len(old_games_df)
    print(f"OldGames Table (Regular Season: {reg_season_start} to {reg_season_end}): {db_game_count} games")

    # 2. Parse JSON and count valid Regular Season games
    json_path = os.path.join(data_dir, f'oddsportal_mlb_{year}.json')
    if not os.path.exists(json_path):
        print(f"JSON file not found: {json_path}")
        continue
        
    with open(json_path, 'r') as f:
        json_data = json.load(f)
        
    json_reg_season_count = 0
    
    for row in json_data:
        home_team = row.get('home_team', '')
        away_team = row.get('away_team', '')
        
        # Skip All-Star Games
        if home_team in ['National League', 'American League'] or away_team in ['National League', 'American League']:
            continue
            
        raw_date = row['date']
        formatted_date = datetime.strptime(raw_date, "%d %b %Y").strftime("%Y-%m-%d")
        
        # Check if game falls inside the regular season window
        if reg_season_start <= formatted_date <= reg_season_end:
            json_reg_season_count += 1
            
    print(f"JSON File (Filtered to regular season window): {json_reg_season_count} games")
    
    diff = db_game_count - json_reg_season_count
    if diff > 0:
        print(f"RESULT: The JSON is inherently missing {diff} games compared to OldGames.")
    elif diff < 0:
        print(f"RESULT: The JSON has {abs(diff)} MORE games than OldGames (Likely shifted dates or duplicates).")
    else:
        print("RESULT: Perfect match in raw game counts!")
        
conn.close()