import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import os
import pickle
from xgboost import XGBClassifier

# ==========================================
# ROBUST SCRIPT PATH RESOLUTION
# ==========================================
# Using __file__ guarantees the paths resolve correctly 
# regardless of where you execute the script from in the terminal.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '../../../'))

src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout

DB_PATH = os.path.join(project_root, "databases", "MLB_Betting.db")

# --- NEW PROFIT-WEIGHTED MODEL PATHS ---
FEATURE_PATH = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "feature_names_profit.pkl")
SCALER_PATH = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "scaler_profit.pkl")
MODEL_XGB = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "xgboost_profit_weighted.pkl")

# --- GRID SEARCH ROI THRESHOLDS ---
# We will only bet if the Expected ROI is strictly greater than these values
ROI_THRESHOLDS = [0, 5, 10, 15, 20, 25, 30, 35]

def evaluate_season_profit_grid(season, model, feature_names, scaler):
    print(f"🚀 Processing {season} Data...")
    
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT F.game_id, OG.date_time, O.home_close_ml, O.away_close_ml, 
               OG.home_score, OG.away_score, F.features_json
        FROM Features AS F
        INNER JOIN Odds_Temp AS O ON F.game_id = O.game_id
        INNER JOIN OldGames AS OG ON F.game_id = OG.game_id
        WHERE OG.season = {season}
        ORDER BY OG.date_time ASC;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    results = {thresh: {"profit": 0, "bets": 0, "wagered": 0, "skipped": 0} for thresh in ROI_THRESHOLDS}
    
    if df.empty: 
        return results

    # Universal Feature Mapping
    raw_features = pd.json_normalize(df["features_json"].apply(json.loads))
    diff_df = pd.DataFrame()
    
    home_cols = [c for c in raw_features.columns if '_home_' in c]
    for h_col in home_cols:
        a_col = h_col.replace('_home_', '_away_')
        if a_col in raw_features.columns:
            new_style = h_col.replace('_home_', '_diff_')
            diff_df[new_style] = raw_features[h_col] - raw_features[a_col]

    df_final = pd.concat([df.drop(columns=['features_json']), diff_df], axis=1)
    
    for _, row in df_final.iterrows():
        try:
            X_raw = pd.DataFrame([row[feature_names].values], columns=feature_names)
            X_scaled = pd.DataFrame(scaler.transform(X_raw), columns=feature_names)
        except KeyError:
            continue

        # Profit-Weighted Model is a Classifier -> use predict_proba
        probs = model.predict_proba(X_scaled)[0]
        home_proba = probs[1]
        away_proba = probs[0]
        
        team, unit, expected_roi = calculateUnitSize(home_proba, away_proba, row["home_close_ml"], row["away_close_ml"])
        
        winner = "home" if row["home_score"] > row["away_score"] else "away"
        
        # Apply the prediction across ALL Expected ROI thresholds
        for thresh in ROI_THRESHOLDS:
            # Skip if there's no edge, or if the edge doesn't meet our minimum ROI threshold
            if team is None or expected_roi < thresh:
                results[thresh]["skipped"] += 1
                continue
                
            results[thresh]["bets"] += 1
            results[thresh]["wagered"] += unit
            
            if team == winner:
                results[thresh]["profit"] += unit * moneyLineToPayout(row[f"{team}_close_ml"])
            else:
                results[thresh]["profit"] -= unit

    return results

def main():
    # EXECUTE THE GRID SEARCH
    print("Loading Profit-Weighted Model and Data...")
    
    with open(FEATURE_PATH, "rb") as f: 
        feat = pickle.load(f)
    with open(SCALER_PATH, "rb") as f: 
        scaler = pickle.load(f)
    with open(MODEL_XGB, "rb") as f: 
        m_xgb = pickle.load(f)

    seasons = [2023, 2024, 2025]

    print("\n==========================================================")
    print("       PROFIT-WEIGHTED CLASSIFIER - ROI GRID SEARCH       ")
    print("==========================================================\n")

    # Fetch all results
    season_data = {}
    for s in seasons:
        season_data[s] = evaluate_season_profit_grid(s, m_xgb, feat, scaler)

    print(f"\n{'Min ROI %':<10} | {'Bets':<6} | {'2023 Prof':<10} | {'2024 Prof':<10} | {'2025 Prof':<10} | {'3-Yr Prof':<10} | {'Total ROI':<8}")
    print("-" * 85)

    # Aggregate and print row by row
    for thresh in ROI_THRESHOLDS:
        g_profit, g_bets, g_wagered = 0, 0, 0
        s_prof = {s: season_data[s][thresh]["profit"] for s in seasons}
        
        for s in seasons:
            g_profit += season_data[s][thresh]["profit"]
            g_bets += season_data[s][thresh]["bets"]
            g_wagered += season_data[s][thresh]["wagered"]
            
        total_roi = (g_profit / g_wagered * 100) if g_wagered > 0 else 0
        
        print(f"{thresh:>4}% ROI  | {g_bets:<6} | {s_prof[2023]:>10.2f} | {s_prof[2024]:>10.2f} | {s_prof[2025]:>10.2f} | {g_profit:>10.2f} | {total_roi:>7.2f}%")

    print("\n✅ Grid search complete.")

if __name__ == "__main__":
    main()