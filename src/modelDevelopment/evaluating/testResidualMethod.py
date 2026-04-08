import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import os
import pickle
from datetime import datetime
from xgboost import XGBRegressor
from sklearn.ensemble import HistGradientBoostingRegressor

# ==========================================
# PATH RESOLUTION
# ==========================================
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '../../../'))

src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout

DB_PATH = os.path.join(project_root, "databases", "MLB_Betting.db")
LOG_FILE = os.path.join(project_root, 'src', 'modelDevelopment', 'evaluating', 'evaluation_logs', 'residual_profit_results.txt')

# New Residual Model Paths
FEATURE_PATH = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "feature_names_residual.pkl")
SCALER_PATH = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "scaler_residual.pkl")
MODEL_XGB = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "xgboost_residual_new.pkl")
MODEL_GB = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "gradient_boosting_residual_new.pkl")

# --- THE GRID SEARCH THRESHOLDS ---
THRESHOLDS = [0.010, 0.015, 0.020, 0.025, 0.030, 0.035, 0.040, 0.045]

def evaluate_season_grid(season, model, feature_names, scaler, model_name):
    print(f"🚀 Processing {season} Data for {model_name}...")
    
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
    
    # Initialize results dictionary for all thresholds
    results = {thresh: {"profit": 0, "bets": 0, "wagered": 0, "skipped": 0} for thresh in THRESHOLDS}
    
    if df.empty: return results

    # Universal Feature Mapping
    raw_features = pd.json_normalize(df["features_json"].apply(json.loads))
    diff_df = pd.DataFrame()
    
    home_cols = [c for c in raw_features.columns if '_home_' in c]
    for h_col in home_cols:
        a_col = h_col.replace('_home_', '_away_')
        if a_col in raw_features.columns:
            new_style = h_col.replace('_home_', '_diff_')
            diff_df[new_style] = raw_features[h_col] - raw_features[a_col]

    # Calculate Vegas Probability
    df["h_implied"] = df["home_close_ml"].apply(lambda x: 100/(x+100) if x>0 else -x/(-x+100))
    df["a_implied"] = df["away_close_ml"].apply(lambda x: 100/(x+100) if x>0 else -x/(-x+100))
    df["vegas_fair_prob"] = df["h_implied"] / (df["h_implied"] + df["a_implied"])
    
    df_final = pd.concat([df.drop(columns=['features_json']), diff_df], axis=1)
    
    for _, row in df_final.iterrows():
        try:
            X_raw = pd.DataFrame([row[feature_names].values], columns=feature_names)
            X_scaled = pd.DataFrame(scaler.transform(X_raw), columns=feature_names)
        except KeyError:
            continue

        # Predict the RESIDUAL (The Error) ONCE per game
        pred_residual = model.predict(X_scaled)[0]
        
        # Reconstruct the Final Probability
        vegas_prob = row["vegas_fair_prob"]
        home_proba = vegas_prob + pred_residual
        home_proba = np.clip(home_proba, 0.01, 0.99)
        away_proba = 1.0 - home_proba
        
        winner = "home" if row["home_score"] > row["away_score"] else "away"
        
        # Now apply this prediction across ALL thresholds instantly
        for thresh in THRESHOLDS:
            if abs(pred_residual) < thresh:
                results[thresh]["skipped"] += 1
                continue
                
            team, unit, _ = calculateUnitSize(home_proba, away_proba, row["home_close_ml"], row["away_close_ml"])
            
            if team is None:
                results[thresh]["skipped"] += 1
                continue

            results[thresh]["bets"] += 1
            results[thresh]["wagered"] += unit
            
            if team == winner:
                results[thresh]["profit"] += unit * moneyLineToPayout(row[f"{team}_close_ml"])
            else:
                results[thresh]["profit"] -= unit

    return results

def run_grid_search():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    with open(FEATURE_PATH, "rb") as f: feat = pickle.load(f)
    with open(SCALER_PATH, "rb") as f: scaler = pickle.load(f)
    with open(MODEL_XGB, "rb") as f: m_xgb = pickle.load(f)
    with open(MODEL_GB, "rb") as f: m_gb = pickle.load(f)

    models = [
        {"name": "XGBoost_Residual_Sniper", "model": m_xgb},
        {"name": "Gradient_Boosting_Residual_Sniper", "model": m_gb}
    ]

    seasons = [2023, 2024, 2025]

    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        original_stdout = sys.stdout
        sys.stdout = f
        try:
            print("==========================================================")
            print("         VEGAS RESIDUAL - THRESHOLD GRID SEARCH           ")
            print("==========================================================\n")
            
            for m_cfg in models:
                print(f"{'#'*85}\n MODEL: {m_cfg['name']}\n{'#'*85}")
                
                # Fetch all results first
                season_data = {}
                for s in seasons:
                    season_data[s] = evaluate_season_grid(s, m_cfg["model"], feat, scaler, m_cfg["name"])
                
                print(f"\n{'Threshold':<10} | {'Bets':<6} | {'2023 Prof':<10} | {'2024 Prof':<10} | {'2025 Prof':<10} | {'3-Yr Prof':<10} | {'ROI %':<8}")
                print("-" * 85)
                
                # Aggregate and print row by row
                for thresh in THRESHOLDS:
                    g_profit, g_bets, g_wagered = 0, 0, 0
                    s_prof = {s: season_data[s][thresh]["profit"] for s in seasons}
                    
                    for s in seasons:
                        g_profit += season_data[s][thresh]["profit"]
                        g_bets += season_data[s][thresh]["bets"]
                        g_wagered += season_data[s][thresh]["wagered"]
                        
                    roi = (g_profit / g_wagered * 100) if g_wagered > 0 else 0
                    
                    print(f"{thresh*100:>5.1f}% Edge | {g_bets:<6} | {s_prof[2023]:>10.2f} | {s_prof[2024]:>10.2f} | {s_prof[2025]:>10.2f} | {g_profit:>10.2f} | {roi:>7.2f}%")
                
                print("\n")
                
        finally:
            sys.stdout = original_stdout
            
    print(f"✅ Grid search complete. Results overwritten in:\n{LOG_FILE}")

if __name__ == "__main__":
    run_grid_search()