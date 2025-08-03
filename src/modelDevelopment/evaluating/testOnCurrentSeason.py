import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import os
import pickle
import re
import pickle
import torch
from torch import nn
from torch.utils.data import TensorDataset, DataLoader
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from pytorch_tabnet.tab_model import TabNetClassifier

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout
from modelDevelopment.utils.featureExtraction import buildFeatures

# MLP
class MLP(nn.Module):
    def __init__(self, input_size):
        super(MLP, self).__init__()
        self.fc1 = nn.Linear(input_size, 64)
        self.fc2 = nn.Linear(64, 2)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)


# Deep MLP with Dropout & BatchNorm
class DeepMLP(nn.Module):
    def __init__(self, input_size):
        super(DeepMLP, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_size, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 2)
        )

    def forward(self, x):
        return self.model(x)


    
    
def calculateTotalProfit(model_name, feature_method):
    db_path = "../../databases/MLB_Betting.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(f"training/model_files/feature_names_{feature_method}.pkl", "rb") as f:
        feature_names = pickle.load(f)

    needs_scaling = False
    scaler = None

    if model_name in ["logistic_regression", "gradient_boosting", "random_forest", "svm"]:
        with open(f"training/model_files/{model_name}_model_{feature_method}.pkl", "rb") as f:
            model = pickle.load(f)
        with open(f"training/model_files/scaler_{feature_method}.pkl", "rb") as f:
            scaler = pickle.load(f)
        needs_scaling = True

    elif model_name == "xgboost":
        with open(f"training/model_files/xgboost_model_{feature_method}.pkl", "rb") as f:
            model = pickle.load(f)

    elif model_name == "mlp":
        model = MLP(len(feature_names))
        model.load_state_dict(torch.load(f"training/model_files/mlp_model_{feature_method}.pt"))
        model.eval()
        with open(f"training/model_files/scaler_{feature_method}.pkl", "rb") as f:
            scaler = pickle.load(f)
        needs_scaling = True

    elif model_name == "deep_mlp":
        model = DeepMLP(len(feature_names))
        model.load_state_dict(torch.load(f"training/model_files/deep_mlp_model_{feature_method}.pt"))
        model.eval()
        with open(f"training/model_files/scaler_{feature_method}.pkl", "rb") as f:
            scaler = pickle.load(f)
        needs_scaling = True

    elif model_name == "tabnet":
        with open(f"training/model_files/tabnet_model_{feature_method}.pkl", "rb") as f:
            model = pickle.load(f)

    else:
        raise ValueError(f"Unsupported model: {model_name}")

    # Pull and preprocess games
    query = """
        SELECT F.game_id, C.date_time, C.season, C.status_code, 
               O.home_team, O.away_team, O.home_team_odds, O.away_team_odds, 
               C.home_score, C.away_score, F.features_json
        FROM Features AS F
        INNER JOIN Odds AS O ON F.game_id = O.game_id
        INNER JOIN CurrentSchedule AS C ON F.game_id = C.game_id
        ORDER BY C.date_time ASC;
    """
    cursor.execute(query)
    games = cursor.fetchall()

    df = pd.DataFrame(games, columns=[
        "game_id", "date_time", "season", "status_code", "home_team", "away_team",
        "home_team_odds", "away_team_odds", "home_score", "away_score", "features_json"
    ])
    df["features_json"] = df["features_json"].apply(json.loads)

    X_all, _, _ = buildFeatures(df, method=feature_method)
    X_features = X_all[feature_names]

    if needs_scaling:
        X_scaled = pd.DataFrame(scaler.transform(X_features), columns=feature_names)
    else:
        X_scaled = X_features.astype(np.float32)

    df_final = pd.concat([df, X_scaled], axis=1)

    from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout

    total_profit, total_bets, correct_bets, incorrect_bets = 0, 0, 0, 0
    total_wagered, unit_size_won, unit_size_lost, skipped_games = 0, 0, 0, 0

    for _, row in df_final.iterrows():
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
        print("MODEL PREDICTION PROBABILITIES, first is away, 2nd is home")
        
        features = row[feature_names].values.astype(np.float32).reshape(1, -1)

        if model_name in ["mlp", "deep_mlp"]:
            tensor_input = torch.tensor(features, dtype=torch.float32)
            with torch.no_grad():
                probs = torch.softmax(model(tensor_input), dim=1).numpy()[0]
        elif model_name == "tabnet":
            probs = model.predict_proba(features)[0]
        else:
            probs = model.predict_proba(features)[0]
            
        
        
        home_proba, away_proba = probs[1], probs[0]
        
        print(probs)
        print(f"home_probability = {home_proba}")
        print(f"away_probability = {away_proba}")
        print()
        
        teamToBetOn, unit_size, expected_roi = calculateUnitSize(home_proba, away_proba, row["home_team_odds"], row["away_team_odds"])
        
        print("UNIT SIZE RECOMMENDATION")
        
            
        ### ELIMINATING SOME BETS ###
        potential_payout = unit_size*(moneyLineToPayout(home_odds) if (teamToBetOn == 'home') else moneyLineToPayout(away_odds))

        if potential_payout < 0.2 or expected_roi < 10:
            skipped_games += 1
            continue
        #############################
        
        
        
        # if there is no play for that game, skip it 
        if teamToBetOn is None:
            skipped_games += 1
            continue

        
        print(f"teamToBetOn = {teamToBetOn}")
        print(f"unit_size = {unit_size}")
        print(f"expected_roi = {expected_roi}")
        print()
        print("OUTCOME")
        
        outcome = "home" if row["home_score"] > row["away_score"] else "away"
        total_bets += 1
        total_wagered += unit_size

        if teamToBetOn == outcome:
            correct_bets += 1
            unit_size_won += unit_size
            profit = unit_size * moneyLineToPayout(row[f"{teamToBetOn}_team_odds"])
            total_profit += profit
            
            print("Bet was correct!")
            if (teamToBetOn == 'home'):
                print(f"Profitted {unit_size * moneyLineToPayout(home_odds)} units")
            else:
                print(f"Profitted {unit_size * moneyLineToPayout(away_odds)} units")
        else:
            incorrect_bets += 1
            unit_size_lost += unit_size
            total_profit -= unit_size
            
            print(f"Bet was wrong, lost {unit_size} units")
            
        print(f"total running profit is {total_profit}\n\n")


    print("\nFINAL STATS")
    print(f"Model: {model_name}")
    print(f"Feature Method: {feature_method}")
    print(f"Total Bets Placed: {total_bets}")
    print(f"Amount Wagered: {total_wagered:.2f} units")
    #print(f"Correct Bets: {correct_bets}")
    #print(f"Incorrect Bets: {incorrect_bets}")
    #print(f"Correct Bets Avg Unit Size: {unit_size_won / correct_bets if correct_bets else 0:.2f}")
    #print(f"Wrong Bets Avg Unit Size: {unit_size_lost / incorrect_bets if incorrect_bets else 0:.2f}")
    print(f"Skipped Games: {skipped_games}")
    print(f"Total Profit: {total_profit:.2f} units")
    if total_bets > 0:
        print(f"Hit Rate: {round(correct_bets / total_bets * 100, 2)}%")
        print(f"ROI: {total_profit / total_wagered * 100:.2f}%")


        
def main_evaluate(model_name="logistic_regression", feature_method="raw"):
    with open(f'evaluation_logs/testingOnCurrentSeason_{model_name}_{feature_method}.log', 'w', encoding='utf-8') as f:
        old_stdout = sys.stdout
        sys.stdout = f
        try:
            calculateTotalProfit(model_name, feature_method)
        finally:
            sys.stdout = old_stdout
    

if __name__ == "__main__":
    main_evaluate()
