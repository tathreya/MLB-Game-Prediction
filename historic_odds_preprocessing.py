import pandas as pd
import sqlite3
import os

def get_team_mapping(season_year):
    """Dynamically handles MLB team name changes and inconsistent Vegas abbreviations"""
    year = int(season_year)
    
    mapping = {
        'ARI': 'Arizona Diamondbacks', 'ATL': 'Atlanta Braves', 'BAL': 'Baltimore Orioles',
        'BOS': 'Boston Red Sox', 'CIN': 'Cincinnati Reds',
        
        # 1. The Cleveland Switch (2022)
        'CLE': 'Cleveland Indians' if year <= 2021 else 'Cleveland Guardians',
        
        'COL': 'Colorado Rockies', 'CUB': 'Chicago Cubs', 'CWS': 'Chicago White Sox',
        'DET': 'Detroit Tigers', 'HOU': 'Houston Astros', 'KAN': 'Kansas City Royals',
        'LAA': 'Los Angeles Angels', 'LAD': 'Los Angeles Dodgers', 
        
        # 2. The 2015 Dodgers "LOS" Fix
        'LOS': 'Los Angeles Dodgers', 
        
        'MIA': 'Miami Marlins', 'MIL': 'Milwaukee Brewers', 'MIN': 'Minnesota Twins', 
        'NYM': 'New York Mets', 'NYY': 'New York Yankees',
        
        # 3. The Athletics Switch (2025)
        'OAK': 'Oakland Athletics' if year <= 2024 else 'Athletics',
        
        'PHI': 'Philadelphia Phillies', 'PIT': 'Pittsburgh Pirates', 'SDG': 'San Diego Padres',
        'SEA': 'Seattle Mariners', 'SFO': 'San Francisco Giants', 'STL': 'St. Louis Cardinals',
        'TAM': 'Tampa Bay Rays', 'TEX': 'Texas Rangers', 'TOR': 'Toronto Blue Jays',
        'WAS': 'Washington Nationals'
    }
    return mapping

def parse_historical_odds(file_path, season_year):
    """Extracts raw game data from the Vegas betting spreadsheets"""
    df = pd.read_excel(file_path)
    games_data = []
    
    for i in range(0, len(df), 2):
        away_row = df.iloc[i]
        home_row = df.iloc[i+1]
        
        raw_date = str(away_row['Date']).zfill(4) 
        formatted_date = f"{season_year}-{raw_date[:2]}-{raw_date[2:]}"
        
        game_dict = {
            'date': formatted_date,
            'home_team_vegas': home_row['Team'],
            'away_open_ml': away_row['Open'],
            'away_close_ml': away_row['Close'],
            'home_open_ml': home_row['Open'],
            'home_close_ml': home_row['Close']
        }
        games_data.append(game_dict)
        
    return games_data

if __name__ == "__main__":
    start_year = 2015
    end_year = 2021
    db_path = "databases/MLB_Betting.db"
    excel_dir = 'historic_odds'
    
    conn = sqlite3.connect(db_path)
    total_inserted = 0

    for year in range(start_year, end_year + 1):
        excel_file = os.path.join(excel_dir, f'mlb-odds-{year}.xlsx')
        
        if not os.path.exists(excel_file):
            print(f"Skipping {year}: File not found.")
            continue

        print(f"\n--- Processing {year} Season ---")
        
        # 1. Parse Excel
        season_odds_list = parse_historical_odds(excel_file, year)
        odds_df = pd.DataFrame(season_odds_list)
        mapping_dict = get_team_mapping(year)
        odds_df['home_team'] = odds_df['home_team_vegas'].map(mapping_dict)
        
        # 2. Load OldGames for the specific year
        query = f"SELECT game_id, date_time, home_team, away_team FROM OldGames WHERE season = '{year}'"
        old_games_df = pd.read_sql(query, conn)
        
        # 3. Timezone Fix & Game Numbering
        old_games_df['date_time'] = pd.to_datetime(old_games_df['date_time'], utc=True)
        old_games_df['local_date'] = old_games_df['date_time'].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d')
        old_games_df = old_games_df.sort_values('date_time')
        old_games_df['game_num'] = old_games_df.groupby(['local_date', 'home_team']).cumcount() + 1
        odds_df['game_num'] = odds_df.groupby(['date', 'home_team']).cumcount() + 1
        
        # 4. Join datasets
        merged_df = pd.merge(
            old_games_df, 
            odds_df, 
            how='left', 
            left_on=['local_date', 'home_team', 'game_num'], 
            right_on=['date', 'home_team', 'game_num']
        )
        
        # 5. Diagnostic Log
        missing_odds = merged_df[merged_df['home_close_ml'].isna()]
        if not missing_odds.empty:
            print(f"MISSING ODDS ALERT: {len(missing_odds)} games found in {year}")
        
        # 6. Filter and Save
        final_odds_table = merged_df.dropna(subset=['home_close_ml']).copy()
        columns_to_keep = [
            'game_id', 'home_team', 'away_team',
            'home_open_ml', 'home_close_ml', 
            'away_open_ml', 'away_close_ml'
        ]
        final_odds_table = final_odds_table[columns_to_keep]
        
        # --- THE OVERWRITE LOGIC ---
        # If it's the first year (2015), use 'replace' to wipe the table.
        # Otherwise, use 'append' to add the other years to the clean table.
        write_mode = 'replace' if year == start_year else 'append'
        
        final_odds_table.to_sql('Odds_Temp', conn, if_exists=write_mode, index=False)
        
        total_inserted += len(final_odds_table)
        print(f"Successfully added {len(final_odds_table)} games for {year}.")

    print(f"\nDONE: Master Odds_Temp table built from scratch with {total_inserted} games.")
    conn.close()