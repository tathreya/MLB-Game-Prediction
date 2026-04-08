import sys
import sqlite3
import pandas as pd
import json
import numpy as np
import os
import pickle
from datetime import datetime
from xgboost import XGBClassifier

# ==========================================
# BULLETPROOF PATH RESOLUTION
# ==========================================
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '../../../'))

src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from odds.calculateUnitSize import calculateUnitSize, moneyLineToPayout

# Static Paths
DB_PATH = os.path.join(project_root, "databases", "MLB_Betting.db")
LOG_FILE = os.path.join(project_root, 'src', 'modelDevelopment', 'evaluating', 'evaluation_logs', 'multi_model_results.txt')

# Model File Paths
FEATURE_PATH_OLD = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "feature_names_diff.pkl")
MODEL_XGB_OLD = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "xgboost_base_96_profit.json")
FEATURE_PATH_NEW = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "feature_names_new.pkl")
MODEL_XGB_CAL = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "xgboost_calibrated_new.pkl")
MODEL_GB_CAL = os.path.join(project_root, "src", "modelDevelopment", "training", "model_files", "gradient_boosting_calibrated_new.pkl")

def evaluate_season(season, model, feature_names, model_name, use_roi_filter):
    print(f"\n🚀 Evaluating {season} | {model_name}")
    
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
    
    if df.empty: return 0, 0, 0

    # Universal Feature Mapping
    raw_features = pd.json_normalize(df["features_json"].apply(json.loads))
    diff_df = pd.DataFrame()
    
    home_cols = [c for c in raw_features.columns if '_home_' in c]
    for h_col in home_cols:
        a_col = h_col.replace('_home_', '_away_')
        if a_col in raw_features.columns:
            # Handle Old Style (e.g., season_avg_era_diff)
            old_style = h_col.replace('_home_', '_') + '_diff'
            # Handle New Style (e.g., season_diff_avg_era)
            new_style = h_col.replace('_home_', '_diff_')
            
            val = raw_features[h_col] - raw_features[a_col]
            diff_df[old_style] = val
            diff_df[new_style] = val

    # Calculate Vegas Probability (Vig-removed)
    df["h_implied"] = df["home_close_ml"].apply(lambda x: 100/(x+100) if x>0 else -x/(-x+100))
    df["a_implied"] = df["away_close_ml"].apply(lambda x: 100/(x+100) if x>0 else -x/(-x+100))
    df["vegas_fair_prob"] = df["h_implied"] / (df["h_implied"] + df["a_implied"])
    
    df_final = pd.concat([df.drop(columns=['features_json']), diff_df], axis=1)
    
    t_profit, t_bets, t_wagered = 0, 0, 0

    for _, row in df_final.iterrows():
        try:
            # Ensure features match model training exactly
            X = pd.DataFrame([row[feature_names].values.astype(np.float32)], columns=feature_names)
        except KeyError:
            continue

        probs = model.predict_proba(X)[0]
        h_prob, a_prob = probs[1], probs[0]
        
        team, unit, roi = calculateUnitSize(h_prob, a_prob, row["home_close_ml"], row["away_close_ml"])
        
        # If model is Calibrated (use_roi_filter=False), force 1-unit bet if no edge found
        if not use_roi_filter and team is None:
            team = "home" if h_prob > a_prob else "away"
            unit = 1.0
        elif use_roi_filter:
            if team is None or roi < 35 or roi > 65:
                continue

        t_bets += 1
        t_wagered += unit
        winner = "home" if row["home_score"] > row["away_score"] else "away"
        
        if team == winner:
            t_profit += unit * moneyLineToPayout(row[f"{team}_close_ml"])
        else:
            t_profit -= unit

    return t_profit, t_bets, t_wagered

def run_all_seasons():
    # Ensure log directory exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # Load All Models and Feature Lists
    with open(FEATURE_PATH_OLD, "rb") as f: feat_old = pickle.load(f)
    m_old = XGBClassifier(); m_old.load_model(MODEL_XGB_OLD)

    models = [
        {"name": "XGBoost_Base_96_Profit", "model": m_old, "feat": feat_old, "filter": True}
    ]

    seasons = [2015,2016,2017,2018,2019,2020,2021,2022,2023, 2024, 2025]

    # Open with 'w' to overwrite existing file
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        original_stdout = sys.stdout
        sys.stdout = f
        
        try:
            print("==========================================================")
            print("         OVERWRITING PREVIOUS BACKTEST RESULTS            ")
            print(f"         DATE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}         ")
            print("==========================================================\n")
            
            for m_cfg in models:
                print(f"{'#'*60}")
                print(f" MODEL: {m_cfg['name']}")
                print(f"{'#'*60}")
                
                grand_p, grand_b, grand_w = 0, 0, 0
                
                for s in seasons:
                    p, b, w = evaluate_season(s, m_cfg["model"], m_cfg["feat"], m_cfg["name"], m_cfg["filter"])
                    print(f"{s} Season -> Profit: {p:.2f} | Bets: {b} | Wagered: {w:.2f}")
                    grand_p += p; grand_b += b; grand_w += w
                
                roi = (grand_p / grand_w * 100) if grand_w > 0 else 0
                print(f"\n🏆 {m_cfg['name']} TOTALS:")
                print(f"   3-Year Profit: {grand_p:.2f} units")
                print(f"   Total Bets:    {grand_b}")
                print(f"   Overall ROI:   {roi:.2f}%")
                print("\n" + "-"*60 + "\n")
                
        finally:
            sys.stdout = original_stdout
            
    print(f"✅ Evaluation complete. Full report overwritten at:\n{LOG_FILE}")

if __name__ == "__main__":
    run_all_seasons()