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
from modelDevelopment.utils.featureExtraction import buildFeatures

def extractAndPreprocessFeatures(games, scaler, model, selected_features):

     # Convert SQL rows to DataFrame
    df = pd.DataFrame(games, columns=[
        "game_id", "date_time", "season", "status_code",
        "home_team", "away_team", "home_team_odds", "away_team_odds",
        "home_score", "away_score", "features_json"
    ])

    # Convert features_json string to dict
    df["features_json"] = df["features_json"].apply(json.loads)

    # Extract features using shared utility
    X_all, y_unused, all_features = buildFeatures(df, method="raw")

    X_features = X_all[selected_features]

    # Combine raw info + features
    df_final = pd.concat([df[[
        "game_id", "date_time", "season", "home_team", "away_team",
        "home_team_odds", "away_team_odds", "home_score", "away_score"
    ]], X_features], axis=1)

    # Scale if model requires it
    if model == "logreg":
        X_scaled = pd.DataFrame(scaler.transform(X_features), columns=selected_features, index=X_features.index)
    elif model == "xgboost":
        X_scaled = X_features
    else:
        raise ValueError("Unsupported model type")

    return df_final, X_scaled

def calculateTotalProfit(model = "logreg"):
    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    logreg = None
    scaler = None
    xgboost = None
    feature_names = None

    if model == "logreg":

        with open('src/modelDevelopment/training/logistic_regression_model.pkl', 'rb') as f:
            logreg = pickle.load(f)

        with open('src/modelDevelopment/training/scaler.pkl', 'rb') as f:
            scaler = pickle.load(f)
        
        with open('src/modelDevelopment/training/feature_names.pkl', 'rb') as f:
            feature_names = pickle.load(f)
            print(len(feature_names))
            print(feature_names)
    
    elif model == 'xgboost':
        # TODO: LOAD AND TEST XGboost
        print('xgboost used')

    # get all features (games) that have occured before today by querying CurrentSchedule for game_ids of such games
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

    df_final, games_to_predict_features_scaled = extractAndPreprocessFeatures(games, scaler, model = "logreg", selected_features=feature_names)

    total_profit = 0
    total_bets = 0
    correct_bets = 0
    incorrect_bets = 0
    skipped_games = 0
    unit_size_won = 0
    unit_size_lost = 0

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
            skipped_games += 1
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
        
        total_bets += 1
        if (teamToBetOn == outcome):
            correct_bets += 1
            print("Bet was correct!")
            unit_size_won += unit_size
            if (teamToBetOn == 'home'):
                total_profit = total_profit + (unit_size * moneyLineToPayout(home_odds))
                print(f"Profitted {unit_size * moneyLineToPayout(home_odds)} units")
            else:
                total_profit = total_profit + (unit_size * moneyLineToPayout(away_odds))
                print(f"Profitted {unit_size * moneyLineToPayout(away_odds)} units")
        else:
            incorrect_bets += 1
            print(f"Bet was wrong, lost {unit_size} units")
            unit_size_lost += unit_size
            total_profit = total_profit - unit_size

        print(f"total running profit is {total_profit}")

        print()
        print()
    
    print("FINAL STATS")
    print(f"Total Bets Placed: {total_bets}")
    print(f"Correct Bets: {correct_bets}")
    print(f"Incorrect Bets: {incorrect_bets}")
    print(f"Correct Bets Average Unit Size: {unit_size_won / correct_bets}")
    print(f"Wrong Bets Average Unit Size: {unit_size_lost / incorrect_bets}")
    print(f"Skipped Games (No Bet): {skipped_games}")
    print(f"Total Profit: {round(total_profit, 2)} units")
    if total_bets > 0:
        print(f"Hit Rate: {round(correct_bets / total_bets * 100, 2)}%")
        print(f"ROI: {round(total_profit / total_bets, 4)} units per bet")


def main():
   
    sys.stdout = open('testingOnCurrentSeason.log', 'w', encoding='utf-8')
    calculateTotalProfit(model="logreg")

if __name__ == "__main__":
    main()
