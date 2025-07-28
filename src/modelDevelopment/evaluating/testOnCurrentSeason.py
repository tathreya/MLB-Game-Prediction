import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import pickle
import re
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

def extractAndPreprocessFeatures(games, scaler):

    # Turn raw SQL data into a DataFrame
    df = pd.DataFrame(games, columns=[
        "game_id", "date_time", "season", "status_code",
        "home_team", "away_team", "home_team_odds", "away_team_odds",
        "home_score", "away_score", "features_json"
    ])

    # Convert features_json from string to dict
    df["features_json"] = df["features_json"].apply(json.loads)

    # Flatten nested JSON features
    features_df = pd.json_normalize(df["features_json"])

    # Combine with base info (we need at least game_id and odds)
    df_final = pd.concat([df[[
        "game_id", "date_time", "season", "home_team", "away_team",
        "home_team_odds", "away_team_odds", "home_score", "away_score"
    ]], features_df], axis=1)

    # Build diff features
    diff_cols = []
    home_cols = [col for col in features_df.columns if re.match(r"(season|rolling)_home_avg_", col)]
    for home_col in home_cols:
        away_col = home_col.replace("home", "away")
        diff_col = home_col.replace("_home", "") + "_diff"

        df_final[diff_col] = df_final[home_col] - df_final[away_col]
        diff_cols.append(diff_col)

    # Select only diff columns for prediction
    X = df_final[diff_cols]

    # Scale the diff features
    games_to_predict_features_scaled = pd.DataFrame(scaler.transform(X), columns=diff_cols, index=df_final.index)

    return df_final, games_to_predict_features_scaled, diff_cols

def calculateTotalProfit():
    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    logreg = None
    with open('src/modelDevelopment/training/logistic_regression_model.pkl', 'rb') as f:
        logreg = pickle.load(f)
    
    scaler = None
    with open('src/modelDevelopment/training/logistic_regression_model.pkl', 'rb') as f:
        scaler = pickle.load(f)

    # STEP 1: get all features (games) that have occured before today by querying CurrentSchedule for game_ids of such games
    # # order by ascending date time so earliest games first, then finding the corresponding features from Feature Table and odds from 
    # Odds table
    all_completed_games_current_season = """
            SELECT F.game_id, C.date_time, C.season, C.status_code, 
                O.home_team, O.away_team, O.home_team_odds, O.away_team_odds, 
                C.home_score, C.away_score, F.features_json
            FROM Features AS F
            INNER JOIN Odds AS O ON F.game_id = O.game_id
            INNER JOIN CurrentSchedule AS C ON F.game_id = C.game_id
            ORDER BY C.date_time ASC;
    """
    
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute(all_completed_games_current_season)

    games = cursor.fetchall()
    
    print(len(games))

    for game in games:
        print(game)
        break

    # Step 4: make prediction on each game, get the prediction probabilities for home and away and then plug into the unit size function to get
    # unit size prediction, if no prediciton given, skip it, if yes, then depending on the actual outcome of game, either subtract the unit size
    # or add how much you won

def extractAndPreprocessFeatures():

def main():
   
    sys.stdout = open('testingOnCurrentSeason.log', 'w', encoding='utf-8')
    calculateTotalProfit()

if __name__ == "__main__":
    main()
