import pandas as pd

def parse_historical_odds(file_path, season_year):
    """
    Extracts game data from betting spreadsheets.
    Pandas automatically handles the first row as headers.
    """
    # Load the excel sheet
    df = pd.read_excel(file_path)
    
    games_data = []
    
    # Iterate through the dataframe in steps of 2 (every pair = 1 game)
    for i in range(0, len(df), 2):
        away_row = df.iloc[i]
        home_row = df.iloc[i+1]
        
        # 1. Format the date (e.g., 401 -> '2021-04-01')
        raw_date = str(away_row['Date']).zfill(4) 
        month = raw_date[:2]
        day = raw_date[2:]
        formatted_date = f"{season_year}-{month}-{day}"
        
        # 2. Extract Data using the exact column names from your screenshot
        game_dict = {
            'date': formatted_date,
            'away_team': away_row['Team'],
            'home_team': home_row['Team'],
            'away_score': away_row['Final'],
            'home_score': home_row['Final'],
            'away_open_ml': away_row['Open'],
            'away_close_ml': away_row['Close'],
            'home_open_ml': home_row['Open'],
            'home_close_ml': home_row['Close']
        }
        
        games_data.append(game_dict)
        
    return games_data

# --- Example Usage ---
if __name__ == "__main__":
    # Point this to your actual Excel file
    excel_file = 'historic_odds/mlb-odds-2021.xlsx' 
    year = 2021
    
    # Run the extraction
    season_games = parse_historical_odds(excel_file, year)
    
    # Print the first game to verify it grabbed the PIT/CUB game correctly
    print(season_games[0])