import requests
import sqlite3
import logging
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)

"""""

1. Loop over each season (2015–2024):
    - For each game in the season (skipping the first N games, where N is the rolling window size):

2. For each team in the game (home and away):
    - Fetch the previous N games played before the current game date to compute rolling stats.
    - Fetch all games played earlier in the same season to compute season-to-date stats.
    - Compute relevant team-level stats:
        • Rolling stats (last N games):
            - OBP (On-base Percentage)
            - SLG (Slugging Percentage)
            - ERA (Earned Run Average)
            - WHIP, etc.
        • Season stats (from start of season to current game date):
            - OBP, SLG, ERA, etc.

3. Store results in a new GameFeatures table:
    - game_id
    - home_team_id, away_team_id
    - All computed feature columns (rolling + season stats for both teams)
    - Final label (e.g., home team win = 1 / 0, or run differential for regression)
"""

# ----------------------------- #
#        SQL STATEMENTS         #
# ----------------------------- #

CREATE_FEATURES_TABLE = """
    CREATE TABLE IF NOT EXISTS Features 
    (
        game_id INTEGER PRIMARY KEY,
        features_json TEXT
    )
"""

INSERT_INTO_FEATURES = """
    INSERT OR IGNORE INTO Features (
        game_id,
        features_json
        ) VALUES (
        ?, ?
    );
"""

SELECT_SEASON_GAMES_IN_ORDER = """
    SELECT *
    FROM OldGames
    WHERE season = ?
    ORDER BY date_time ASC
"""

# ----------------------------- #
#     FUNCTIONS START HERE      #
# ----------------------------- #

def engineerFeatures(rolling_window_size, base_url):

    try:
        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        # Check if Features table already exists
        cursor.execute("""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' AND name='Features';
        """)
        table_exists = cursor.fetchone()

        if table_exists:
            
            cursor.execute("DROP TABLE IF EXISTS Features;")

            # TODO: uncomment this out once feature engineering is finalized, maybe look into a way to make this process faster
            # by storing box scores locally...
            # logger.debug("Features table already exists — skipping historical feature engineering.")
            # return  # Exit early if already built
        
        createFeaturesTable(cursor)

        logger.debug("Creating Features table if it doesn't exist")

        cursor.execute("BEGIN TRANSACTION;")

        logger.debug("Attempting to engineer features for past seasons")

        old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
        for old_season in old_seasons:
            
            if (old_season != '2015'):
                break

            games = selectSeasonGames(cursor, old_season)
        
            print(len(games))

            # Outer dict maps team_id → that team's season stats
            team_season_stats = defaultdict(lambda: {

                # GENERAL STATS
                "gamesPlayed": 0,

                # OFFENSIVE/BATTING STATS
                "runsScored": 0,

                # DEFENSIVE/PITCHING STATS
                "runsGiven": 0,
            })

            team_rolling_stats = defaultdict(lambda: {
                # keep a deque of the last N game stats for eeach team

                # OFFENSIVE/BATTING STATS
                "runsScored": deque(maxlen=rolling_window_size),

                # DEFENSIVE/PITCHING STATS
                "runsGiven": deque(maxlen=rolling_window_size)
            })

            numGamesProcessed = 0
            for game in games:
                
                game_id = game[0]
                response = requests.get(f"{base_url}game/{game_id}/boxscore")
                game_data = response.json()

                # Extract team IDs
                home_team, home_team_id = fetchHomeTeam(game_data)
                away_team, away_team_id = fetchAwayTeam(game_data)

                # extract runs scored for both teams
                home_runs_scored, away_runs_scored = fetchRunsScored(home_team, away_team)

                # if the total number of games played for that team after updating becomes greater than N (rolling size window), 
                # then we actually store that game with features in the Features DB with rolling average equal to season average
                # till now
                if (team_season_stats[home_team_id]["gamesPlayed"] >= rolling_window_size and 
                    team_season_stats[away_team_id]["gamesPlayed"] >= rolling_window_size and home_runs_scored != away_runs_scored):
                    

                    features = buildFeatures(team_season_stats, team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored)
                    insertIntoFeaturesTable(cursor, game_id, features)
                    print('here, saving the game because both teams have enough info for rolling average')
                    print(game_id)

                # After feature extraction, update season totals to include this game for both teams
                updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored)   
                # also update rolling averages
                updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored)

                numGamesProcessed += 1
                print('processed game = ' + str(numGamesProcessed))

            print(team_season_stats)
            break

        conn.commit() 

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching MLB boxscore data: {http_err}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Other error occurred while engineering features and saving to DB: {e}")
        conn.rollback()
    finally:
        conn.close()


def createFeaturesTable(cursor):
    cursor.execute(CREATE_FEATURES_TABLE)

def insertIntoFeaturesTable(cursor, game_id, features_dict):
    print('inserting the game')
    features_json = json.dumps(features_dict)
    cursor.execute(INSERT_INTO_FEATURES, (game_id, features_json))

def selectSeasonGames(cursor, old_season):
    cursor.execute(SELECT_SEASON_GAMES_IN_ORDER, (old_season,))
    games = cursor.fetchall()
    return games

def buildFeatures(team_season_stats, team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored):

    home_season_stats = team_season_stats[home_team_id]
    away_season_stats = team_season_stats[away_team_id]

    home_rolling_stats = team_rolling_stats[home_team_id]
    away_rolling_stats = team_rolling_stats[away_team_id]

    # calculate season averages
    season_home_avg_runs_scored = home_season_stats["runsScored"] / home_season_stats["gamesPlayed"] if home_season_stats["gamesPlayed"] > 0 else 0
    season_home_avg_runs_given = home_season_stats["runsGiven"] / home_season_stats["gamesPlayed"] if home_season_stats["gamesPlayed"] > 0 else 0
    season_away_avg_runs_scored = away_season_stats["runsScored"] / away_season_stats["gamesPlayed"] if away_season_stats["gamesPlayed"] > 0 else 0
    season_away_avg_runs_given = away_season_stats["runsGiven"] / away_season_stats["gamesPlayed"] if away_season_stats["gamesPlayed"] > 0 else 0

    # calculate rolling averages
    rolling_home_avg_runs_scored = sum(home_rolling_stats["runsScored"]) / len(home_rolling_stats["runsScored"])
    rolling_home_avg_runs_given = sum(home_rolling_stats["runsGiven"]) / len(home_rolling_stats["runsGiven"])
    rolling_away_avg_runs_scored =  sum(away_rolling_stats["runsScored"]) / len(away_rolling_stats["runsScored"])
    rolling_away_avg_runs_given = sum(away_rolling_stats["runsGiven"]) / len(away_rolling_stats["runsGiven"])


    features = {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "season_home_avg_runs_scored": season_home_avg_runs_scored,
        "season_home_avg_runs_given": season_home_avg_runs_given,
        "season_away_avg_runs_scored": season_away_avg_runs_scored,
        "season_away_avg_runs_given": season_away_avg_runs_given,
        "rolling_home_avg_runs_scored": rolling_home_avg_runs_scored,
        "rolling_home_avg_runs_given": rolling_home_avg_runs_given,
        "rolling_away_avg_runs_scored": rolling_away_avg_runs_scored,
        "rolling_away_avg_runs_given": rolling_away_avg_runs_given,
        # TODO: might change this to not be a binary classificatino instead predict final score or probability
        # of win
        "label": 1 if home_runs_scored > away_runs_scored else 0 
    }

    print("homeRuns = " + str(home_runs_scored))
    print("awayRuns = " + str(away_runs_scored))
    print("label = " + str(features["label"]))

    return features

def updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored):
    # Home team update
    team_season_stats[home_team_id]["gamesPlayed"] += 1
    team_season_stats[home_team_id]["runsScored"] += home_runs_scored
    team_season_stats[home_team_id]["runsGiven"] += away_runs_scored

    # Away team update
    team_season_stats[away_team_id]["gamesPlayed"] += 1
    team_season_stats[away_team_id]["runsScored"] += away_runs_scored
    team_season_stats[away_team_id]["runsGiven"] += home_runs_scored

def updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored):

    # we can just append because dequeue has max_len of rolling window size so if it is at max (rollwing window), then 
    # it'll automatically pop the oldest game and append the newest game, keeping our rolling window of 5 most recent games

    # Home team update
    team_rolling_stats[home_team_id]["runsScored"].append(home_runs_scored)
    team_rolling_stats[home_team_id]["runsGiven"].append(away_runs_scored)

    # Away team update
    team_rolling_stats[away_team_id]["runsScored"].append(away_runs_scored)
    team_rolling_stats[away_team_id]["runsGiven"].append(home_runs_scored)

def fetchHomeTeam(game_data):

    home_team = game_data["teams"]["home"]
    home_team_id = home_team["team"]["id"]
    return (home_team, home_team_id)

def fetchAwayTeam(game_data):

    away_team = game_data["teams"]["away"]
    away_team_id = away_team["team"]["id"]
    return (away_team, away_team_id)

def fetchRunsScored(home_team, away_team):

    home_runs = home_team.get("teamStats", {}).get("batting", {}).get("runs", 0)
    away_runs = away_team.get("teamStats", {}).get("batting", {}).get("runs", 0)

    return (home_runs, away_runs)