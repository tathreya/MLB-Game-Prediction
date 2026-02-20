from flask import Flask, render_template, request, jsonify
import sqlite3
import sys
import os
import pandas as pd
import numpy as np
import pickle
import json
from xgboost import XGBClassifier
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from werkzeug.security import check_password_hash
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# Add src/ to Python path so you can import your modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from runFeaturePipeline import main as run_pipeline
from odds.calculateUnitSize import calculateUnitSize
from modelDevelopment.utils.featureExtraction import buildFeatures

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")
app.permanent_session_lifetime = timedelta(hours=2)

# ----------------- GLOBAL VARIABLES -----------------

# Get absolute paths to avoid directory issues - works from any directory
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "databases", "MLB_Betting.db")
FEATURE_FILE = os.path.join(BASE_DIR, "src", "modelDevelopment", "training", "model_files", "feature_names_diff.pkl")
MODEL_FILE = os.path.join(BASE_DIR, "src", "modelDevelopment", "training", "model_files", "xgboost_base_96_profit.json")

TEAM_LOGOS = {
    "Los Angeles Angels": "logos/angels.png",
    "Houston Astros": "logos/astros.png",
    "Athletics": "logos/athletics.png",
    "Toronto Blue Jays": "logos/blue_jays.png",
    "Atlanta Braves": "logos/braves.png",
    "Milwaukee Brewers": "logos/brewers.jpeg",
    "St. Louis Cardinals": "logos/cardinals.png",
    "Chicago Cubs": "logos/cubs.png",
    "Arizona Diamondbacks": "logos/diamondbacks.png",
    "Los Angeles Dodgers": "logos/dodgers.png",
    "San Francisco Giants": "logos/giants.png",
    "Cleveland Guardians": "logos/guardians.png",
    "Seattle Mariners": "logos/mariners.jpeg",
    "Miami Marlins": "logos/marlins.png",
    "New York Mets": "logos/mets.png",
    "Washington Nationals": "logos/nationals.png",
    "Baltimore Orioles": "logos/orioles.png",
    "San Diego Padres": "logos/padres.png",
    "Philadelphia Phillies": "logos/phillies.png",
    "Pittsburgh Pirates": "logos/pirates.png",
    "Texas Rangers": "logos/rangers.png",
    "Tampa Bay Rays": "logos/rays.png",
    "Boston Red Sox": "logos/red_sox.png",
    "Cincinnati Reds": "logos/reds.png",
    "Colorado Rockies": "logos/rockies.png",
    "Kansas City Royals": "logos/royals.png",
    "Detroit Tigers": "logos/tigers.png",
    "Minnesota Twins": "logos/twins.png",
    "Chicago White Sox": "logos/white_sox.png",
    "New York Yankees": "logos/yankees.png"
}

with open(FEATURE_FILE, "rb") as f:
    FEATURE_NAMES = pickle.load(f)

model = XGBClassifier()
model.load_model(MODEL_FILE)

# ----------------- HELPER FUNCTIONS -----------------

def validate_login(username, password):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return check_password_hash(row[0], password)
    return False

def login_required(func):
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)
    return wrapper
def fetch_todays_games():
    query = """
        SELECT CS.game_id, CS.date_time, CS.home_team, CS.away_team
        FROM CurrentSchedule AS CS
        WHERE DATE(datetime(CS.date_time, '-4 hours')) = DATE(datetime('now', '-4 hours'))
        ORDER BY CS.date_time ASC;
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Detect double headers (group by teams, keep only later game labeled)
    df["double_header"] = ""
    grouped = df.groupby(["home_team", "away_team"])
    for _, group in grouped:
        if len(group) > 1:
            latest_idx = group["date_time"].idxmax()
            df.loc[latest_idx, "double_header"] = "DH-2"

    df["home_logo"] = df["home_team"].map(
        lambda t: url_for("static", filename=TEAM_LOGOS.get(t, "logos/default.png"))
    )
    df["away_logo"] = df["away_team"].map(
        lambda t: url_for("static", filename=TEAM_LOGOS.get(t, "logos/default.png"))
    )

    return df.to_dict(orient="records")

def validate_odds(odds):
    """Return True if odds are valid (+/- followed by digits)."""
    return odds and (odds.startswith(("+", "-")) and odds[1:].isdigit() and len(odds) >= 4)

# ----------------- API ROUTES -----------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        if validate_login(username, password):
            session.permanent = True
            session["logged_in"] = True
            session["username"] = username
            return redirect(url_for("daily_prediction"))
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/dailyPrediction")
@login_required
def daily_prediction():
    return render_template("dailyPrediction.html")

@app.route("/fetch-games")
@login_required
def fetch_games_api():
    games = fetch_todays_games()
    return jsonify(games)

@app.route("/run-pipeline", methods=["POST"])
@login_required
def runFeaturePipeline():
    """
    Calls runFeaturePipeline.main() and waits for it to finish.
    Returns JSON with status when done.
    """
    try:
        run_pipeline()
        return jsonify({"status": "success", "message": "Feature engineering pipeline finished successfully!"})
    except Exception as e:
        return jsonify({"status": "error", "message": "Feature engineering pipeline failed!"})
    
@app.route("/predict", methods=["POST"])
@login_required
def predict():
    """
    Expects JSON: { game_id, home_odds, away_odds }
    Returns JSON: { teamToBetOn, unit_size, expected_roi }
    """
    data = request.get_json()
    game_id = data.get("game_id")
    home_odds = data.get("home_odds")
    away_odds = data.get("away_odds")

    # Validate odds
    if not validate_odds(home_odds) or not validate_odds(away_odds):
        return jsonify({"status": "error", "message": "Invalid odds format."})

    # Fetch features for this game
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT F.game_id, CS.date_time, CS.season, CS.status_code, CS.home_team, CS.away_team, F.features_json
        FROM Features AS F
        INNER JOIN CurrentSchedule AS CS ON CS.game_id = F.game_id
        WHERE F.game_id = ?
    """, (game_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return jsonify({"status": "error", "message": "Game not found."})

    df = pd.DataFrame([row], columns=[
        "game_id", "date_time", "season", "status_code", "home_team", "away_team", "features_json"
    ])
    df["features_json"] = df["features_json"].apply(json.loads)

    X_all, _, _ = buildFeatures(df, method="diff")
    X_features = X_all[FEATURE_NAMES]
    # no need to scale for xgboost
    X_scaled = X_features.astype(np.float32)
    df_final = pd.concat([df, X_scaled], axis = 1)
    X_row = df_final[FEATURE_NAMES].iloc[0].values.reshape(1, -1)    

    # Predict
    probs = model.predict_proba(X_row)[0]
    home_proba, away_proba = probs[1], probs[0]

    teamToBetOn, unit_size, expected_roi = calculateUnitSize(home_proba, away_proba, home_odds, away_odds)

    if teamToBetOn == "home":
        teamToBetOn = df_final["home_team"].iloc[0]
    else:
        teamToBetOn = df_final["away_team"].iloc[0]

    return jsonify({
        "status": "success",
        "teamToBetOn": teamToBetOn,
        "unit_size": float(unit_size),
        "expected_roi": float(expected_roi)
    })

if __name__ == '__main__':
    port = 8000
    print(f"🚀 Starting MLB Betting App on http://localhost:{port}")
    print(f"📊 Database: {DB_PATH}")
    print(f"🧠 Model: {MODEL_FILE}")
    app.run(host="0.0.0.0", port=port, debug=True)