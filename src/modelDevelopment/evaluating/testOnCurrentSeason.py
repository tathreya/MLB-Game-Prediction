import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import os
import pickle
import re
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout

def extractAndPreprocessFeatures(games, scaler, model):

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

    games_to_predict_features = None
    if (model == 'logreg'):
        # Scale the features for logreg
        games_to_predict_features = pd.DataFrame(scaler.transform(X), columns=diff_cols, index=df_final.index)
    elif (model == 'xgboost'):
        # don't scale for xgboost
        games_to_predict_features = X


    return df_final, games_to_predict_features, diff_cols

def calculateTotalProfit(model = "logreg"):
    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    logreg = None
    scaler = None
    xgboost = None

    if model == "logreg":
        with open('src/modelDevelopment/training/logistic_regression_model.pkl', 'rb') as f:
            logreg = pickle.load(f)
    
        with open('src/modelDevelopment/training/scaler.pkl', 'rb') as f:
            scaler = pickle.load(f)
    
    elif model == 'xgboost':
        # TODO: LOAD AND TEST XGboost
        print('xgboost used')

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

    df_final, games_to_predict_features_scaled, diff_cols = extractAndPreprocessFeatures(games, scaler, model = "logreg")

    total_profit = 0

    for i, row in df_final.iterrows():
        game_id = row["game_id"]
        home_team = row["home_team"]
        away_team = row["away_team"]
        home_odds = row["home_team_odds"]
        away_odds = row["away_team_odds"]
        home_score = row["home_score"]
        away_score = row["away_score"]

        print("GAME INFO!, home team + odds + score comes first then away!")
        print((game_id, home_team, home_odds, home_score, away_team, away_odds, away_score))
        print()

        # Get the scaled features for this game
        current_game_features = games_to_predict_features_scaled.loc[[i]]  

        print("MODEL PREDICTION PROBABILITIES, first is away, 2nd is home")
        probabilities = logreg.predict_proba(current_game_features)[0]
        print(probabilities)
        home_win_proba = probabilities[1]
        away_win_proba = probabilities[0]
        print(f"home_probability = {home_win_proba}")
        print(f"away_probability = {away_win_proba}")

        print()

        teamToBetOn, unit_size, expected_roi = calculateUnitSize(home_win_proba, away_win_proba, home_odds, away_odds)

        print("UNIT SIZE RECOMMENDATION")

        # if there is no play for that game, skip it 
        if (teamToBetOn == None):
            print('no bet for that game!')
            continue

        print(f"teamToBetOn = {teamToBetOn}")
        print(f"unit_size = {unit_size}")
        print(f"expected_roi = {expected_roi}")

        print()

        print("OUTCOME")
        outcome = None
        if (home_score > away_score):
            outcome = 'home'
        else:
            outcome = 'away'
        
        if (teamToBetOn == outcome):
            print("Bet was correct!")
            if (teamToBetOn == 'home'):
                total_profit = total_profit + (unit_size * moneyLineToPayout(home_odds))
                print(f"Profitted {unit_size * moneyLineToPayout(home_odds)} units")
            else:
                total_profit = total_profit + (unit_size * moneyLineToPayout(away_odds))
                print(f"Profitted {unit_size * moneyLineToPayout(away_odds)} units")
        else:
            print(f"Bet was wrong, lost {unit_size} units")
            total_profit = total_profit - unit_size

        print(f"total running profit is {total_profit}")

        print()
        print()

def main():
   
    sys.stdout = open('testingOnCurrentSeason.log', 'w', encoding='utf-8')
    calculateTotalProfit(model="logreg")

if __name__ == "__main__":
    main()
