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
        hidden_size = max(32, input_size // 2)
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.fc2 = nn.Linear(hidden_size, 2)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)

class DeepMLP(nn.Module):
    def __init__(self, input_size):
        super(DeepMLP, self).__init__()
        hidden1 = max(64, input_size * 2)
        hidden2 = max(32, input_size)

        self.model = nn.Sequential(
            nn.Linear(input_size, hidden1),
            nn.BatchNorm1d(hidden1),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden1, hidden2),
            nn.BatchNorm1d(hidden2),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden2, 2)
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
    home_bets, away_bets, home_profit, away_profit, home_correct, away_correct = 0, 0, 0, 0, 0, 0

    # Confidence bin setup (0.10 to 1.00 in steps of 0.05)
    bin_edges = np.arange(0.10, 1.01, 0.05)

    bin_stats = {
        f"{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}": {
            "count": 0,
            "total_profit": 0,
            "correct": 0,
            "total_unit_size": 0,
            "sum_confidence": 0,
        } for i in range(len(bin_edges) - 1)
    }

    # Define unit size buckets (adjust as needed)
    unit_size_buckets = {
        "0-0.49": {"count": 0, "profit": 0, "correct": 0, "wagered": 0},
        "0.5-0.99": {"count": 0, "profit": 0, "correct": 0, "wagered": 0},
        "1-1.99": {"count": 0, "profit": 0, "correct": 0, "wagered": 0},
        "2-2.99": {"count": 0, "profit": 0, "correct": 0, "wagered": 0},
        "3+": {"count": 0, "profit": 0, "correct": 0, "wagered": 0},
    }

    expected_roi_bins = [(0,5), (5,10), (10,15), (15,20), (20,25), (25,30), (30,35), (35,40), (40,45), (45,50),
                     (50,55), (55,60), (60,65), (65,70), (70,75), (75,80)]

    # Add last bucket for 80+
    expected_roi_buckets = {
        f"{low}-{high}": {"count": 0, "profit": 0, "correct": 0, "wagered": 0}
        for low, high in expected_roi_bins
    }
    expected_roi_buckets["80+"] = {"count": 0, "profit": 0, "correct": 0, "wagered": 0}

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
        
        # if there is no play for that game, skip it 
        if teamToBetOn is None:
            skipped_games += 1
            continue
        
        print(f"teamToBetOn = {teamToBetOn}")
        print(f"unit_size = {unit_size}")
        print(f"expected_roi = {expected_roi}")
        print()
        print("OUTCOME")

        # TODO: mess with the filtering of plays, sweet spot was 35-65 expected ROI

        if teamToBetOn == "home":
            confidence = home_proba
        elif teamToBetOn == "away":
            confidence = away_proba
        
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
                print(f"Profitted {profit} units")
            else:
                print(f"Profitted {profit} units")
        else:
            incorrect_bets += 1
            unit_size_lost += unit_size
            profit = -1 * unit_size
            total_profit += profit
            print(f"Bet was wrong, lost {unit_size} units")

        if teamToBetOn == "home":
            home_bets += 1
            home_profit += profit
            if teamToBetOn == outcome:
                home_correct += 1
        elif teamToBetOn == "away":
            away_bets += 1
            away_profit += profit
            if teamToBetOn == outcome:
                away_correct += 1
        
        if unit_size < 0.5:
            bucket = "0-0.49"
        elif unit_size < 1:
            bucket = "0.5-0.99"
        elif unit_size < 2:
            bucket = "1-1.99"
        elif unit_size < 3:
            bucket = "2-2.99"
        else:
            bucket = "3+"

        unit_size_buckets[bucket]["count"] += 1
        unit_size_buckets[bucket]["profit"] += profit
        unit_size_buckets[bucket]["wagered"] += unit_size
        if teamToBetOn == outcome:
            unit_size_buckets[bucket]["correct"] += 1

        bucket_found = False
        for low, high in expected_roi_bins:
            if low <= expected_roi < high:
                bucket_key = f"{low}-{high}"
                bucket_found = True
                break
        if not bucket_found:
            bucket_key = "80+"

        # Update bucket stats
        expected_roi_buckets[bucket_key]["count"] += 1
        expected_roi_buckets[bucket_key]["profit"] += profit
        expected_roi_buckets[bucket_key]["wagered"] += unit_size
        if teamToBetOn == outcome:
            expected_roi_buckets[bucket_key]["correct"] += 1
        
        # Update bin stats
        for i in range(len(bin_edges) - 1):
            if bin_edges[i] <= confidence < bin_edges[i + 1]:
                bin_key = f"{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}"
                bin_stats[bin_key]["count"] += 1
                bin_stats[bin_key]["total_profit"] += profit
                if teamToBetOn == outcome:
                    bin_stats[bin_key]["correct"] += 1
                bin_stats[bin_key]["total_unit_size"] += unit_size
                bin_stats[bin_key]["sum_confidence"] += confidence
                break
            
        print(f"total running profit is {total_profit}\n\n")


    print("\nFINAL STATS")
    print(f"Model: {model_name}")
    print(f"Feature Method: {feature_method}")
    print(f"Total Profit: {total_profit:.2f} units")
    if total_bets > 0:
        print(f"Hit Rate: {round(correct_bets / total_bets * 100, 2)}%")
        print(f"ROI: {total_profit / total_wagered * 100:.2f}%")
    print(f"Total Bets Placed: {total_bets}")
    print(f"Amount Wagered: {total_wagered:.2f} units")
    print(f"Correct Bets: {correct_bets}")
    print(f"Incorrect Bets: {incorrect_bets}")
    print(f"Correct Bets Avg Unit Size: {unit_size_won / correct_bets if correct_bets else 0:.2f}")
    print(f"Wrong Bets Avg Unit Size: {unit_size_lost / incorrect_bets if incorrect_bets else 0:.2f}")
    print(f"Skipped Games: {skipped_games}")
    print(f"Home Bets: {home_bets}, Profit: {home_profit:.2f}, Correct: {home_correct}, Hit Rate: {home_correct/home_bets if home_bets else 0:.2%}")
    print(f"Away Bets: {away_bets}, Profit: {away_profit:.2f}, Correct: {away_correct}, Hit Rate: {away_correct/away_bets if away_bets else 0:.2%}")

    print("\nCONFIDENCE BINNING PROFIT ANALYSIS:")
    print(f"{'Bin':<11} {'Count':>6} {'Avg Conf':>9} {'Win Rate':>9} {'Total Profit':>13} {'Avg Profit/Bet':>15}")
    for bin_key, stats in bin_stats.items():
        count = stats["count"]
        if count == 0:
            continue
        avg_conf = stats["sum_confidence"] / count
        win_rate = stats["correct"] / count
        avg_profit_per_bet = stats["total_profit"] / count
        print(f"{bin_key:<11} {count:6} {avg_conf:9.3f} {win_rate:9.3f} {stats['total_profit']:13.2f} {avg_profit_per_bet:15.4f}")

    print("\nPROFIT BY UNIT SIZE BUCKETS:")
    print(f"{'Bucket':<10} {'Count':>6} {'Profit':>10} {'Wagered':>10} {'Hit Rate':>10}")
    for bucket, stats in unit_size_buckets.items():
        count = stats['count']
        if count == 0:
            continue
        profit = stats['profit']
        wagered = stats['wagered']
        correct = stats['correct']
        hit_rate = correct / count if count else 0
        print(f"{bucket:<10} {count:6} {profit:10.2f} {wagered:10.2f} {hit_rate:10.2%}")

    print("\nPROFIT BY EXPECTED ROI BUCKETS:")
    print(f"{'Bucket':<10} {'Count':>6} {'Profit':>10} {'Wagered':>10} {'Hit Rate':>10}")
    for bucket_key, stats in expected_roi_buckets.items():
        if stats["count"] == 0:
            continue
        hit_rate = stats["correct"] / stats["count"]
        print(f"{bucket_key:<10} {stats['count']:6} {stats['profit']:10.2f} {stats['wagered']:10.2f} {hit_rate:10.2%}")

def main_evaluate(model_name, feature_method):

    with open(f'evaluation_logs/testingOnCurrentSeason_{model_name}_{feature_method}.log', 'w', encoding='utf-8') as f:
        old_stdout = sys.stdout
        sys.stdout = f
        try:
            calculateTotalProfit(model_name, feature_method)
        finally:
            sys.stdout = old_stdout
    
if __name__ == "__main__":
    main_evaluate()
 