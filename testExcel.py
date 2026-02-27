import sqlite3
import pandas as pd
import os

db_path = "databases/MLB_Betting.db"
data_dir = 'historic_odds'

conn = sqlite3.connect(db_path)

for year in range(2015, 2022):
    print(f"\n--- Analyzing {year} Season (Excel) ---")
    
    # 1. Get OldGames data and determine Regular Season bounds
    query = f"SELECT game_id, date_time, home_team, away_team FROM OldGames WHERE season = '{year}'"
    old_games_df = pd.read_sql(query, conn)
    
    if old_games_df.empty:
        print(f"No data in OldGames for {year}.")
        continue
        
    # Convert UTC to US/Eastern
    old_games_df['date_time'] = pd.to_datetime(old_games_df['date_time'], utc=True)
    old_games_df['local_date'] = old_games_df['date_time'].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d')
    
    reg_season_start = old_games_df['local_date'].min()
    reg_season_end = old_games_df['local_date'].max()
    
    db_game_count = len(old_games_df)
    print(f"OldGames Table (Regular Season: {reg_season_start} to {reg_season_end}): {db_game_count} games")

    # 2. Parse Excel and count pairs
    excel_path = os.path.join(data_dir, f'mlb-odds-{year}.xlsx')
    if not os.path.exists(excel_path):
        print(f"Excel file not found: {excel_path}")
        continue
        
    df = pd.read_excel(excel_path)
    # Every 2 rows is 1 game
    total_excel_games = len(df) // 2
    
    excel_reg_season_count = 0
    
    # Iterate through pairs
    for i in range(0, len(df), 2):
        # The Excel date is usually 3-4 digits (e.g., 407 for April 7th)
        raw_date = str(df.iloc[i]['Date']).zfill(4)
        # Format it as YYYY-MM-DD
        formatted_date = f"{year}-{raw_date[:2]}-{raw_date[2:]}"
        
        if reg_season_start <= formatted_date <= reg_season_end:
            excel_reg_season_count += 1
            
    print(f"Excel File (Filtered to regular season window): {excel_reg_season_count} games")
    
    diff = db_game_count - excel_reg_season_count
    if diff > 0:
        print(f"RESULT: The Excel is missing {diff} games.")
    elif diff < 0:
        print(f"RESULT: The Excel has {abs(diff)} MORE games than OldGames (Likely shifted dates).")
    else:
        print("RESULT: Perfect match in raw game counts!")

conn.close()