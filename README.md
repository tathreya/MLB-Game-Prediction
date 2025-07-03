# MLB-AI-Betting

A machine learning project to predict outcomes of MLB games using historical and current season data, including advanced team-level statistics. Focuses on storing, engineering, and modeling structured team data for predictive accuracy

## 📂 Database Structure

### 1. `Teams`
Stores metadata about MLB teams.
- `id`: unique identifier (matches MLB API)
- `name`: full team name (e.g., "Los Angeles Dodgers")
- `abbreviation`: team abbreviation (e.g. LAD)

### 2. `CurrentSchedule`
Tracks the entire schedule and score outcomes of the **current MLB season**.
- Includes: game date, home/away teams, game status, final scores, venue
- Updates daily via script that pulls from the MLB Stats API.

### 3. `OldGames`
Stores historical data from **2015–2024** MLB seasons.
- Includes: game date, teams involved, final scores, game outcomes (W/L).

### 4. `Features`
Stores features for each historical game from **2015-2024** MLB seasons
- Includes: rolling average of advanced team stats + season averages too

## 🔄 Fetching and Storing Data

- MLB data is fetched from the official MLB API: [`https://statsapi.mlb.com/api/v1/`](https://statsapi.mlb.com/api/v1/)
- Information fetched:
  - Team list and metadata
  - Full season schedules (past and present)
  - Final scores
  - Advanced team statistics (e.g., OBP, ERA, FIP, wOBA)
- All data is stored in a **SQL database** for querying, feature generation, and model training.
- REGULAR SEASON API ENDPOINT —> https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R
- POSTSEASON API ENDPOINT —> https://statsapi.mlb.com/api/v1/schedule/postseason?season={season}&sportId=1
- ADVANCED STATS API ENDPOINT -> https://statsapi.mlb.com/api/v1/game/{gameID}/boxscore

## 🧠 Feature Engineering

For each game and for each team in that game, features are computed based on **seasonal** and **recent (last N games)** stats to capture both long-term performance and current momentum.

### 🔹 Offensive Metrics

- **Runs per Game**: Total runs scored divided by games played. Core indicator of offensive output.
- **Batting Average (AVG)**: Hits ÷ At-Bats. Measures how often a team gets a hit. Doesn't account for walks or power.
- **On-Base Percentage (OBP)**: (Hits + Walks + Hit By Pitch) / (At Bats + Walks + Hit By Pitch + Sacrifice Flies). Shows how often a team reaches base.
- **Slugging Percentage (SLG)**: Total bases ÷ At-Bats. Reflects power-hitting (extra-base hits).
- **OPS (On-base + Slugging)**: OBP + SLG. Combined measure of contact and power hitting.
- **Strikeout Rate (K%)**: Strikeouts ÷ Plate Appearances. High K% = unproductive outs.
- **Walk Rate (BB%)**: Walks ÷ Plate Appearances. More walks = more base runners.
- **BABIP (Batting Average on Balls In Play)**: (Hits - Home Runs) ÷ (At-Bats - K - HR + Sac Flies). Can indicate luck or fielding quality.
MAYBE:
- **wOBA (Weighted On-Base Average)**: Like OBP, but weights events by run value. More predictive of scoring than AVG or OPS.

### 🔹 Pitching & Defensive Metrics

- **ERA (Earned Run Average)**: (Earned Runs × 9) ÷ Innings Pitched. Lower = better run prevention.
- **WHIP**: (Walks + Hits) ÷ Innings Pitched. Tracks base runners allowed per inning.
- **FIP (Fielding Independent Pitching)**: Based only on HR, BB, K. Estimates a pitcher’s performance independent of defense.
- **K/9**: (Strikeouts × 9) ÷ Innings Pitched. Strikeout dominance.
- **K%**: (Strikeout Rate): Strikeouts ÷ Batters Faced. Measures the percentage of batters a pitcher strikes out — a direct indicator of pitching dominance independent of innings pitched.
- **BB/9**: (Walks × 9) ÷ Innings Pitched. Pitch control — lower is better.
- **HR/9**: (Home Runs × 9) ÷ Innings Pitched. Fewer home runs allowed = fewer big innings.
- **Opponent OBP/SLG/OPS**: How well opposing batters perform against the team’s pitchers. Lower values = stronger pitching.
- **DRS (Defensive Runs Saved)**: Measures how many runs a team’s defense saved above average.
- **OAA (Outs Above Average)**: Statcast-based measure of how many outs fielders made relative to average — includes range.

### 🔹 Game Context Features

- **Rolling Averages (Last N Games)**: Same stats above but computed over the last 5–10 games to capture form.
- **Home/Away Splits**: Separate stats when team is home vs away — some teams perform differently.
- **Win/Loss Streak**: Number of consecutive wins or losses before a game.
- **Days of Rest**: Days since team’s last game — fatigue or recovery.
- **Back-to-Back Flag**: Binary flag indicating if team is playing on consecutive days.
- **Head-to-Head Record**: Win rate vs specific opponent over recent seasons.

## 🛠️ Training Pipeline

The model is trained using historical MLB games from the 2015 to 2023 seasons. Each row in the training set represents a single game and contains:

- **Rolling averages** (last N games before the game) for key stats like:
  - OBP, SLG, ERA, WHIP, etc.
- **Seasonal averages** (season-to-date) for the same key stats
- **Contextual features**:
  - Home/away indicator
  - Rest days before the game
  - Game number in the season

These features are computed for both teams involved in the game (home and away). Labels for training can be:
- **Binary classification** (home win = 1, away win = 0)
- or **Regression** (e.g., predicted run differential)

All features are aligned with the data available **before** each game to avoid data leakage.

Example training row:

| Feature             | Value  |
|---------------------|--------|
| home_OBP_last5      | 0.345  |
| home_ERA_season     | 3.91   |
| away_OBP_last5      | 0.312  |
| away_ERA_season     | 4.23   |
| home_team_flag      | 1      |
| label_home_win      | 1      |

The training process includes:
1. **Feature engineering**: Building rolling and seasonal features for each team-game.
2. **Dataset split**:  
   - Train on 2015–2021  
   - Validate on 2022–2023  
   - Test on 2024  
3. **Model training**: Using models like XGBoost, LightGBM, or Logistic Regression
4. **Evaluation**: Accuracy, ROC AUC (for classification) or RMSE (for regression)


## 🎯 Prediction Pipeline (e.g. for Current Season Games)

After training the ML model, it is used to predict the outcomes of upcoming games in the current season

### How It Works

For each future game, the pipeline builds a feature vector using:

- **Rolling averages**: Stats from the last N games each team has played
- **Seasonal averages**: Team stats averaged over the current season up to the prediction date
- **Contextual features**: Whether the team is home or away, rest days, etc.

These features are passed into the trained model to generate a prediction.

