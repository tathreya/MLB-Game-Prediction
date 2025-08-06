import sqlite3
import pickle
import os
import sys
import json
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout
from modelDevelopment.utils.featureExtraction import buildFeatures

def computeDailyPredictions():

    fetch_games_today = """
    SELECT F.game_id, CS.date_time, CS.season, CS.status_code, CS.home_team, CS.away_team, F.features_json
    FROM CurrentSchedule AS CS
    INNER JOIN Features AS F 
    ON CS.game_id = F.game_id
    WHERE DATE(datetime(CS.date_time, '-4 hours')) = DATE(datetime('now', '-4 hours'))
    ORDER BY CS.date_time ASC; 
    """

    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    cursor.execute(fetch_games_today)
    games = cursor.fetchall()

    print(f"Games found today: {len(games)}")

    # load the feature set
    with open(f"src/modelDevelopment/training/model_files/feature_names_diff.pkl", "rb") as f:
        feature_names = pickle.load(f)
        
    print(len(feature_names))
    # load the xgboost model
    with open(f"src/modelDevelopment/training/model_files/xgboost_model_diff.pkl", "rb") as f:
            model = pickle.load(f)

    df = pd.DataFrame(games, columns=[
        "game_id", "date_time", "season", "status_code", "home_team", "away_team", "features_json"
    ])
    df["features_json"] = df["features_json"].apply(json.loads)

    X_all, _, _ = buildFeatures(df, method="diff")
    X_features = X_all[feature_names]
    # no need to scale for xgboost
    X_scaled = X_features.astype(np.float32)
    df_final = pd.concat([df, X_scaled], axis = 1)

    for _, row in df_final.iterrows():
        home_team = row["home_team"]
        away_team = row["away_team"]
        print(row["game_id"])
        print(f"\nGame: {away_team} @ {home_team}")
        home_odds = input(f"Enter home odds for {home_team}: ").strip()
        away_odds = input(f"Enter away odds for {away_team}: ").strip()

        features = row[feature_names].values.astype(np.float32).reshape(1, -1)
        probs = model.predict_proba(features)[0]
        print(probs)
        home_proba, away_proba = probs[1], probs[0]
   
        print(f"\nModel Predictions:")
        print(f"home_probability = {home_proba}")
        print(f"away_probability = {away_proba}")
        print()

        teamToBetOn, unit_size, expected_roi = calculateUnitSize(home_proba, away_proba, home_odds, away_odds)

        # if there is no play for that game, skip it 
        if teamToBetOn is None:
            print("skipped that game!")
            continue

        if (teamToBetOn == "home"):
            print(f"teamToBetOn = {home_team}")
        else:
            print(f"teamToBetOn = {away_team}")
       
        print(f"unit_size = {unit_size}")
        print(f"expected_roi = {expected_roi}")


def main():
    computeDailyPredictions()
if __name__ == "__main__":
    main()