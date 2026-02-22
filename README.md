# MLB Game Prediction Engine

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
- **K/9**: (Strikeouts × 9) ÷ Innings Pitched. Strikeout dominance.
- **K%**: (Strikeout Rate): Strikeouts ÷ Batters Faced. Measures the percentage of batters a pitcher strikes out — a direct indicator of pitching dominance independent of innings pitched.
- **BB/9**: (Walks × 9) ÷ Innings Pitched. Pitch control — lower is better.
- **HR/9**: (Home Runs × 9) ÷ Innings Pitched. Fewer home runs allowed = fewer big innings.
- **Opponent OBP/SLG/OPS**: How well opposing batters perform against the team’s pitchers. Lower values = stronger pitching.

MAYBE:
- **FIP (Fielding Independent Pitching)**: Based only on HR, BB, K. Estimates a pitcher’s performance independent of defense.
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

## 🔄 Season Migration Guide

### Migrating to a New MLB Season (Streamlined)

**🎯 NEW: Single Point of Configuration!** 

All season management is centralized in the `.env` file.

#### 1. Update Environment Configuration (Only Step!)
**File**: `.env`
```bash
# Just change this one line:
CURRENT_SEASON=new_season_year
```

That's it! The system automatically handles everything else.

#### 2. Clean Current Season Table (Optional but Recommended)
**SQL Command**:
```sql
DELETE FROM CurrentSchedule WHERE season = 'last_year_season';
```

#### 3. Run Full Pipeline
```bash
python src/runFeaturePipeline.py
```

### What Happens Automatically

The system now dynamically generates season lists from your `.env` file:

- **Old Seasons**: Automatically generates `["2015", "2016", ..., "2025"]` from `CURRENT_SEASON=2026`
- **Feature Engineering**: Automatically includes `["2015", "2016", ..., "2026"]` 
- **Current Season**: Uses `CURRENT_SEASON` value for all operations

### Files That Auto-Update (No Manual Changes Needed)

✅ **`src/runFeaturePipeline.py`** - Uses `get_old_seasons()`  
✅ **`src/featureEngineering/createFeatures.py`** - Uses `get_all_seasons()`  
✅ **All other modules** - Use `get_current_season()`

### Migration Benefits

- ✅ **Single Source of Truth**: Only `.env` file needs changes
- ✅ **Error-Proof**: No more forgetting to update multiple files
- ✅ **Future-Proof**: Works for any season automatically
- ✅ **Consistent**: All modules use same season logic

### Example: Migrating to 2027 Season

```bash
# 1. Update .env
echo "CURRENT_SEASON=2027" > .env

# 2. Run pipeline (everything else is automatic)
python src/runFeaturePipeline.py
```

The system will automatically:
- Process seasons 2015-2026 as old seasons
- Build features for seasons 2015-2027
- Handle 2027 as current season

**Migration simplified from 3 steps to 1 step!** 🎉

## 🐳 Docker Deployment

### Quick Start with Docker

#### Option 1: Docker Compose (Recommended)
```bash
# Build and run (uses modern "docker compose" command)
docker compose up --build
```

#### Option 2: Docker Compose (Legacy)
```bash
# Build and run (uses legacy "docker-compose" command)
docker-compose up --build
```

#### Option 3: Manual Docker
```bash
# Build the image
docker build -t mlb-ai-betting .

# Run the container
docker run -p 8001:8000 -v $(pwd)/databases:/app/databases --env-file .env mlb-ai-betting
```

#### Stop Containers
```bash
# Modern command
docker compose down

# Legacy command
docker-compose down

# Or stop specific container
docker stop mlb-app
```

### Docker Configuration Details

**Dockerfile**:
- Uses `continuumio/miniconda3` base image
- Creates conda environment from `environment.yml`
- Exposes port 8000 (mapped to 8001 on host)
- Mounts database directory for persistence
- Runs Flask app from `/app/predictionsApp/app.py`

**docker-compose.yml**:
- Maps host port 8001 to container port 8000
- Persists database with volume mount
- Uses `.env` file for environment variables

### Container Access

- **Web App**: `http://localhost:8001`
- **Database**: Stored in `./databases/` (persisted outside container)
- **Logs**: View with `docker compose logs -f` (modern) or `docker-compose logs -f` (legacy)

### Which Command to Use?

```bash
# Check your Docker Compose version
docker-compose --version

# Or try the modern command first
docker compose up --build
```

Use whichever command works on your system - both are functionally identical.

#### 3. Stop Containers
```bash
docker-compose down
# Or stop specific container
docker stop mlb-app
```

### Docker Configuration Details

**Dockerfile**:
- Uses `continuumio/miniconda3` base image
- Creates conda environment from `environment.yml`
- Exposes port 8000 (mapped to 8001 on host)
- Mounts database directory for persistence
- Runs Flask app from `/app/predictionsApp/app.py`

**docker-compose.yml**:
- Maps host port 8001 to container port 8000
- Persists database with volume mount
- Uses `.env` file for environment variables

### Container Access

- **Web App**: `http://localhost:8001`
- **Database**: Stored in `./databases/` (persisted outside container)
- **Logs**: View with `docker compose logs -f` (Docker Compose V2) or `docker-compose logs -f` (V1)

### Development vs Production

For development, the volume mount allows you to:
- Edit source code locally
- Database changes persist across container restarts
- Use `docker compose up` for iterative development

For production, consider:
- Using Docker volumes instead of bind mounts
- Environment-specific configurations
- Process monitoring and restart policies
