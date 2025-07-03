import requests
import sqlite3
import logging
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)

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

CREATE_BOXSCORE_TABLE = """
    CREATE TABLE IF NOT EXISTS GameBoxScoreStats (
    game_id INTEGER PRIMARY KEY,

    -- Home Team Info
    home_team_id INTEGER,
    home_runs INTEGER,
    home_hits INTEGER,
    home_doubles INTEGER,
    home_triples INTEGER,
    home_home_runs INTEGER,
    home_strikeouts INTEGER,
    home_walks INTEGER,
    home_hit_by_pitch INTEGER,
    home_at_bats INTEGER,
    home_plate_appearances INTEGER,
    home_total_bases INTEGER,
    home_sac_flies INTEGER,
    home_sac_bunts INTEGER,
    home_obp REAL,
    home_slg REAL,
    home_ops REAL,
    home_avg REAL,
    home_rbi INTEGER,
    home_left_on_base INTEGER,
    home_caught_stealing INTEGER,
    home_stolen_bases INTEGER,
    home_stolen_base_percentage REAL,
    home_ground_into_double_play INTEGER,
    home_ground_into_triple_play INTEGER,
    home_pickoffs_batting INTEGER,

    home_earned_runs INTEGER,
    home_innings_pitched REAL,
    home_pitching_strikeouts INTEGER,
    home_pitching_walks INTEGER,
    home_pitching_hits INTEGER,
    home_pitching_home_runs INTEGER,
    home_pitching_era REAL,
    home_pitching_whip REAL,
    home_pitching_obp REAL,
    home_pitching_batters_faced INTEGER,
    home_pitching_strikes INTEGER,
    home_pitching_balls INTEGER,
    home_pitching_strike_pct REAL,
    home_pitching_pickoffs INTEGER,
    home_pitching_inherited_runners INTEGER,
    home_pitching_inherited_runners_scored INTEGER,

    home_errors INTEGER,
    home_assists INTEGER,
    home_putouts INTEGER,
    home_fielding_chances INTEGER,
    home_passed_ball INTEGER,
    home_fielding_caught_stealing INTEGER,
    home_fielding_stolen_bases INTEGER,
    home_fielding_stolen_base_pct REAL,
    home_fielding_pickoffs INTEGER,

    -- Away Team Info
    away_team_id INTEGER,
    away_runs INTEGER,
    away_hits INTEGER,
    away_doubles INTEGER,
    away_triples INTEGER,
    away_home_runs INTEGER,
    away_strikeouts INTEGER,
    away_walks INTEGER,
    away_hit_by_pitch INTEGER,
    away_at_bats INTEGER,
    away_plate_appearances INTEGER,
    away_total_bases INTEGER,
    away_sac_flies INTEGER,
    away_sac_bunts INTEGER,
    away_obp REAL,
    away_slg REAL,
    away_ops REAL,
    away_avg REAL,
    away_rbi INTEGER,
    away_left_on_base INTEGER,
    away_caught_stealing INTEGER,
    away_stolen_bases INTEGER,
    away_stolen_base_percentage REAL,
    away_ground_into_double_play INTEGER,
    away_ground_into_triple_play INTEGER,
    away_pickoffs_batting INTEGER,

    away_earned_runs INTEGER,
    away_innings_pitched REAL,
    away_pitching_strikeouts INTEGER,
    away_pitching_walks INTEGER,
    away_pitching_hits INTEGER,
    away_pitching_home_runs INTEGER,
    away_pitching_era REAL,
    away_pitching_whip REAL,
    away_pitching_obp REAL,
    away_pitching_batters_faced INTEGER,
    away_pitching_strikes INTEGER,
    away_pitching_balls INTEGER,
    away_pitching_strike_pct REAL,
    away_pitching_pickoffs INTEGER,
    away_pitching_inherited_runners INTEGER,
    away_pitching_inherited_runners_scored INTEGER,

    away_errors INTEGER,
    away_assists INTEGER,
    away_putouts INTEGER,
    away_fielding_chances INTEGER,
    away_passed_ball INTEGER,
    away_fielding_caught_stealing INTEGER,
    away_fielding_stolen_bases INTEGER,
    away_fielding_stolen_base_pct REAL,
    away_fielding_pickoffs INTEGER
);
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
        
        logger.debug("Creating Features table if it doesn't exist")
        createFeaturesTable(cursor)
        logger.debug("Creating GameBoxScoreStats table if it doesn't exist")
        createBoxScoreTable(cursor)

        cursor.execute("BEGIN TRANSACTION;")

        logger.debug("Attempting to engineer features for past seasons")

        old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
        for old_season in old_seasons:
            
            logger.debug(f"Engineering features for {old_season} season")
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
                "hits": 0,
                "atBats": 0,
                "walks": 0,
                "hitByPitch": 0,
                "sacFlies": 0, 
                "totalBases": 0,
                "strikeouts": 0,
                "plateAppearances": 0,

                # DEFENSIVE/PITCHING STATS
                "runsGiven": 0,

                
            })

            team_rolling_stats = defaultdict(lambda: {
                # keep a deque of the last N game stats for eeach team

                # OFFENSIVE/BATTING STATS
                "runsScored": deque(maxlen=rolling_window_size),
                "hits": deque(maxlen=rolling_window_size),
                "atBats": deque(maxlen=rolling_window_size),
                "walks": deque(maxlen=rolling_window_size),
                "hitByPitch": deque(maxlen=rolling_window_size),
                "sacFlies": deque(maxlen=rolling_window_size),
                "totalBases": deque(maxlen=rolling_window_size),
                "strikeouts": deque(maxlen=rolling_window_size),
                "plateAppearances": deque(maxlen=rolling_window_size),

                # DEFENSIVE/PITCHING STATS
                "runsGiven": deque(maxlen=rolling_window_size)
            })

            numGamesProcessed = 0
            for game in games:
                
                game_id = game[0]

                game_data = None

                if (boxScoreExists(cursor, game_id)):
                    print('box score already existed! skipped it')
                    continue
                else:
                    response = requests.get(f"{base_url}game/{game_id}/boxscore")
                    game_data = response.json()
                    insertIntoBoxScoreTable(cursor, game_id, game_data)

                # fetch all the stats from boxscore for each team
                home_stats = extractTeamStats(game_data["teams"]["home"], "home")
                away_stats = extractTeamStats(game_data["teams"]["away"], "away")

                # extract the ids
                home_team_id = home_stats["home_team_id"]
                away_team_id = away_stats["away_team_id"]

                # extract runs scored for both teams
                home_runs_scored = home_stats["home_runs"]
                away_runs_scored = away_stats["away_runs"]

                # if the total number of games played for that team after updating becomes greater than N (rolling size window), 
                # then we actually store that game with features in the Features DB with rolling average equal to season average
                # till now
                if (team_season_stats[home_team_id]["gamesPlayed"] >= rolling_window_size and 
                    team_season_stats[away_team_id]["gamesPlayed"] >= rolling_window_size and home_runs_scored != away_runs_scored):
                    
                    features = buildFeatures(team_season_stats, team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored)
                    insertIntoFeaturesTable(cursor, game_id, features)

                    if(team_season_stats[home_team_id]["gamesPlayed"] == 6 and team_season_stats[away_team_id]["gamesPlayed"] == 6):       
                        print('6 games played each, breaking here and printing game id')
                        print('gameId = ' + str(game_id))
                        break
                    
                # After saving the feature, update season totals to include this game for bobuith teams
                updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_stats, away_stats)   
                # also update rolling averages
                updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_stats, away_stats)

                numGamesProcessed += 1
                print('processed game = ' + str(numGamesProcessed))

            print(team_season_stats)
            print(team_rolling_stats)
            break
            # TODO: make sure next iteration resets the team season stats and rolling dictionaries
           
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

def createBoxScoreTable(cursor):
    cursor.execute(CREATE_BOXSCORE_TABLE)

def boxScoreExists(cursor, game_id):
    cursor.execute("SELECT 1 FROM GameBoxScoreStats WHERE game_id = ?", (game_id,))
    return cursor.fetchone() is not None

def insertIntoFeaturesTable(cursor, game_id, features_dict):
    features_json = json.dumps(features_dict)
    cursor.execute(INSERT_INTO_FEATURES, (game_id, features_json))

def insertIntoBoxScoreTable(cursor, game_id, game_data):
    home = extractTeamStats(game_data["teams"]["home"], "home")
    away = extractTeamStats(game_data["teams"]["away"], "away")

    data = {
        "game_id": game_id,
        **home,
        **away
    }

    # get the columns and corresponding values
    keys = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cursor.execute(f"INSERT OR IGNORE INTO GameBoxScoreStats ({keys}) VALUES ({placeholders})", tuple(data.values()))

def extractTeamStats(team, prefix):
    batting = team["teamStats"]["batting"]
    pitching = team["teamStats"]["pitching"]
    fielding = team["teamStats"]["fielding"]

    def safe_float(val): return float(val) if val not in (None, ".---", "-.--", "") else 0.0

    return {
        f"{prefix}_team_id": team["team"]["id"],
        f"{prefix}_runs": batting.get("runs", 0),
        f"{prefix}_hits": batting.get("hits", 0),
        f"{prefix}_doubles": batting.get("doubles", 0),
        f"{prefix}_triples": batting.get("triples", 0),
        f"{prefix}_home_runs": batting.get("homeRuns", 0),
        f"{prefix}_strikeouts": batting.get("strikeOuts", 0),
        f"{prefix}_walks": batting.get("baseOnBalls", 0),
        f"{prefix}_hit_by_pitch": batting.get("hitByPitch", 0),
        f"{prefix}_at_bats": batting.get("atBats", 0),
        f"{prefix}_plate_appearances": batting.get("plateAppearances", 0),
        f"{prefix}_total_bases": batting.get("totalBases", 0),
        f"{prefix}_sac_flies": batting.get("sacFlies", 0),
        f"{prefix}_sac_bunts": batting.get("sacBunts", 0),
        f"{prefix}_obp": safe_float(batting.get("obp", 0.0)),
        f"{prefix}_slg": safe_float(batting.get("slg", 0.0)),
        f"{prefix}_ops": safe_float(batting.get("ops", 0.0)),
        f"{prefix}_avg": safe_float(batting.get("avg", 0.0)),
        f"{prefix}_rbi": batting.get("rbi", 0),
        f"{prefix}_left_on_base": batting.get("leftOnBase", 0),
        f"{prefix}_caught_stealing": batting.get("caughtStealing", 0),
        f"{prefix}_stolen_bases": batting.get("stolenBases", 0),
        f"{prefix}_stolen_base_percentage": safe_float(batting.get("stolenBasePercentage", 0.0)),
        f"{prefix}_ground_into_double_play": batting.get("groundIntoDoublePlay", 0),
        f"{prefix}_ground_into_triple_play": batting.get("groundIntoTriplePlay", 0),
        f"{prefix}_pickoffs_batting": batting.get("pickoffs", 0),

        f"{prefix}_earned_runs": pitching.get("earnedRuns", 0),
        f"{prefix}_innings_pitched": safe_float(pitching.get("inningsPitched", "0.0")),
        f"{prefix}_pitching_strikeouts": pitching.get("strikeOuts", 0),
        f"{prefix}_pitching_walks": pitching.get("baseOnBalls", 0),
        f"{prefix}_pitching_hits": pitching.get("hits", 0),
        f"{prefix}_pitching_home_runs": pitching.get("homeRuns", 0),
        f"{prefix}_pitching_era": safe_float(pitching.get("era", 0.0)),
        f"{prefix}_pitching_whip": safe_float(pitching.get("whip", 0.0)),
        f"{prefix}_pitching_obp": safe_float(pitching.get("obp", 0.0)),
        f"{prefix}_pitching_batters_faced": pitching.get("battersFaced", 0),
        f"{prefix}_pitching_strikes": pitching.get("strikes", 0),
        f"{prefix}_pitching_balls": pitching.get("balls", 0),
        f"{prefix}_pitching_strike_pct": safe_float(pitching.get("strikePercentage", 0.0)),
        f"{prefix}_pitching_pickoffs": pitching.get("pickoffs", 0),
        f"{prefix}_pitching_inherited_runners": pitching.get("inheritedRunners", 0),
        f"{prefix}_pitching_inherited_runners_scored": pitching.get("inheritedRunnersScored", 0),

        f"{prefix}_errors": fielding.get("errors", 0),
        f"{prefix}_assists": fielding.get("assists", 0),
        f"{prefix}_putouts": fielding.get("putOuts", 0),
        f"{prefix}_fielding_chances": fielding.get("chances", 0),
        f"{prefix}_passed_ball": fielding.get("passedBall", 0),
        f"{prefix}_fielding_caught_stealing": fielding.get("caughtStealing", 0),
        f"{prefix}_fielding_stolen_bases": fielding.get("stolenBases", 0),
        f"{prefix}_fielding_stolen_base_pct": safe_float(fielding.get("stolenBasePercentage", 0.0)),
        f"{prefix}_fielding_pickoffs": fielding.get("pickoffs", 0)
    }

def selectSeasonGames(cursor, old_season):
    cursor.execute(SELECT_SEASON_GAMES_IN_ORDER, (old_season,))
    games = cursor.fetchall()
    return games

def buildFeatures(team_season_stats, team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored):

    # get the home + away season stats
    home_season_stats = team_season_stats[home_team_id]
    away_season_stats = team_season_stats[away_team_id]

    # get their rolling stats too
    home_rolling_stats = team_rolling_stats[home_team_id]
    away_rolling_stats = team_rolling_stats[away_team_id]

    # -------------------------------------------------
    #             CALCULATING SEASON AVERAGES
    # -------------------------------------------------

    # calculate home team season averages
    season_home_avg_runs_scored = home_season_stats["runsScored"] / home_season_stats["gamesPlayed"] if home_season_stats["gamesPlayed"] > 0 else 0
    season_home_avg_runs_given = home_season_stats["runsGiven"] / home_season_stats["gamesPlayed"] if home_season_stats["gamesPlayed"] > 0 else 0
    # Batting Average = hits / at Bats
    season_home_avg_batting_avg = home_season_stats["hits"] / home_season_stats["atBats"] if home_season_stats["atBats"] > 0 else 0
    # Calculate OBP
    season_home_avg_obp = calculate_obp(
        home_season_stats["hits"],
        home_season_stats["walks"],
        home_season_stats["hitByPitch"],
        home_season_stats["atBats"],
        home_season_stats["sacFlies"]
    )
    # Calculate SLG
    season_home_avg_slg = home_season_stats["totalBases"] / home_season_stats["atBats"] if home_season_stats["atBats"] > 0 else 0
    # Calculate OPS
    season_home_avg_ops = season_home_avg_obp + season_home_avg_slg
    # Calculate batting K%
    season_home_batting_k_pct = home_season_stats["strikeouts"] / home_season_stats["plateAppearances"] if home_season_stats["plateAppearances"] > 0 else 0

    # calculate away team season averages
    season_away_avg_runs_scored = away_season_stats["runsScored"] / away_season_stats["gamesPlayed"] if away_season_stats["gamesPlayed"] > 0 else 0
    season_away_avg_runs_given = away_season_stats["runsGiven"] / away_season_stats["gamesPlayed"] if away_season_stats["gamesPlayed"] > 0 else 0
    # Batting Average = hits / at Bats
    season_away_avg_batting_avg = away_season_stats["hits"] / away_season_stats["atBats"] if away_season_stats["atBats"] > 0 else 0
    # Calculate OBP 
    season_away_avg_obp = calculate_obp(
        away_season_stats["hits"],
        away_season_stats["walks"],
        away_season_stats["hitByPitch"],
        away_season_stats["atBats"],
        away_season_stats["sacFlies"]
    )
    # Calculate SLG
    season_away_avg_slg = away_season_stats["totalBases"] / away_season_stats["atBats"] if away_season_stats["atBats"] > 0 else 0
    # Calculate OPS
    season_away_avg_ops = season_away_avg_obp + season_away_avg_slg
    # Calculate batting K%
    season_away_batting_k_pct = away_season_stats["strikeouts"] / away_season_stats["plateAppearances"] if away_season_stats["plateAppearances"] > 0 else 0

    # -------------------------------------------------
    #             CALCULATING ROLLING AVERAGES
    # -------------------------------------------------

    # calculate home team rolling averages
    rolling_home_avg_runs_scored = sum(home_rolling_stats["runsScored"]) / len(home_rolling_stats["runsScored"])
    rolling_home_avg_runs_given = sum(home_rolling_stats["runsGiven"]) / len(home_rolling_stats["runsGiven"])
    # Batting Average = hits / at Bats
    rolling_home_avg_batting_avg = sum(home_rolling_stats["hits"]) / sum(home_rolling_stats["atBats"]) if sum(home_rolling_stats["atBats"]) > 0 else 0
    # Calculate OBP
    rolling_home_avg_obp = calculate_obp(
        sum(home_rolling_stats["hits"]),
        sum(home_rolling_stats["walks"]),
        sum(home_rolling_stats["hitByPitch"]),
        sum(home_rolling_stats["atBats"]),
        sum(home_rolling_stats["sacFlies"])
    )
    # Calculate SLG
    rolling_home_avg_slg = sum(home_rolling_stats["totalBases"]) / sum(home_rolling_stats["atBats"]) if sum(home_rolling_stats["atBats"]) > 0 else 0
    # Calculate OPS
    rolling_home_avg_ops = rolling_home_avg_obp + rolling_home_avg_slg
    # Calculate batting K%
    rolling_home_batting_k_pct = sum(home_rolling_stats["strikeouts"]) / sum(home_rolling_stats["plateAppearances"]) if sum(home_rolling_stats["plateAppearances"]) > 0 else 0

    # calculate away team rolling averages
    rolling_away_avg_runs_scored =  sum(away_rolling_stats["runsScored"]) / len(away_rolling_stats["runsScored"])
    rolling_away_avg_runs_given = sum(away_rolling_stats["runsGiven"]) / len(away_rolling_stats["runsGiven"])
    # Batting Average = hits / at Bats
    rolling_away_avg_batting_avg = sum(away_rolling_stats["hits"]) / sum(away_rolling_stats["atBats"]) if sum(away_rolling_stats["atBats"]) > 0 else 0
    # Calculate OBP
    rolling_away_avg_obp = calculate_obp(
        sum(away_rolling_stats["hits"]),
        sum(away_rolling_stats["walks"]),
        sum(away_rolling_stats["hitByPitch"]),
        sum(away_rolling_stats["atBats"]),
        sum(away_rolling_stats["sacFlies"])
    )
    # Calculate SLG
    rolling_away_avg_slg = sum(away_rolling_stats["totalBases"]) / sum(away_rolling_stats["atBats"]) if sum(away_rolling_stats["atBats"]) > 0 else 0
    # Calculate OPS
    rolling_away_avg_ops = rolling_away_avg_obp + rolling_away_avg_slg
    # Calculate batting K%
    rolling_away_batting_k_pct = sum(away_rolling_stats["strikeouts"]) / sum(away_rolling_stats["plateAppearances"]) if sum(away_rolling_stats["plateAppearances"]) > 0 else 0

    features = {

        # TEAM IDs
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,

        # SEASON AVG FEATURES
        "season_home_avg_runs_scored": season_home_avg_runs_scored,
        "season_home_avg_runs_given": season_home_avg_runs_given,
        "season_home_avg_batting_avg": season_home_avg_batting_avg,
        "season_home_avg_obp": season_home_avg_obp,
        "season_home_avg_slg": season_home_avg_slg,
        "season_home_avg_ops": season_home_avg_ops,
        "season_home_batting_k_pct": season_home_batting_k_pct,

        "season_away_avg_runs_scored": season_away_avg_runs_scored,
        "season_away_avg_runs_given": season_away_avg_runs_given,
        "season_away_avg_batting_avg": season_away_avg_batting_avg,
        "season_away_avg_obp": season_away_avg_obp,
        "season_away_avg_slg": season_away_avg_slg,
        "season_away_avg_ops": season_away_avg_ops,
        "season_away_batting_k_pct": season_away_batting_k_pct,

        # ROLLING AVERAGE
        "rolling_home_avg_runs_scored": rolling_home_avg_runs_scored,
        "rolling_home_avg_runs_given": rolling_home_avg_runs_given,
        "rolling_home_avg_batting_avg": rolling_home_avg_batting_avg,
        "rolling_home_avg_obp": rolling_home_avg_obp,
        "rolling_home_avg_slg": rolling_home_avg_slg,
        "rolling_home_avg_ops": rolling_home_avg_ops,
        "rolling_home_batting_k_pct": rolling_home_batting_k_pct,

        "rolling_away_avg_runs_scored": rolling_away_avg_runs_scored,
        "rolling_away_avg_runs_given": rolling_away_avg_runs_given,
        "rolling_away_avg_batting_avg": rolling_away_avg_batting_avg,
        "rolling_away_avg_obp": rolling_away_avg_obp,
        "rolling_away_avg_slg": rolling_away_avg_slg,
        "rolling_away_avg_ops": rolling_away_avg_ops,
        "rolling_away_batting_k_pct": rolling_away_batting_k_pct,

        # ML Classification/Regression Label
        # TODO: might change this to not be a binary classificatino instead predict final score or probability
        # of win
        "label": 1 if home_runs_scored > away_runs_scored else 0 
    }

    return features

def updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_stats, away_stats):
    # Home team update
    team_season_stats[home_team_id]["gamesPlayed"] += 1
    team_season_stats[home_team_id]["runsScored"] += home_stats["home_runs"]
    team_season_stats[home_team_id]["runsGiven"] += away_stats["away_runs"]
    team_season_stats[home_team_id]["hits"] += home_stats["home_hits"]
    team_season_stats[home_team_id]["atBats"] += home_stats["home_at_bats"]
    team_season_stats[home_team_id]["walks"] += home_stats["home_walks"]
    team_season_stats[home_team_id]["hitByPitch"] += home_stats["home_hit_by_pitch"]
    team_season_stats[home_team_id]["sacFlies"] += home_stats["home_sac_flies"]
    team_season_stats[home_team_id]["totalBases"] += home_stats["home_total_bases"]
    team_season_stats[home_team_id]["strikeouts"] += home_stats["home_strikeouts"]
    team_season_stats[home_team_id]["plateAppearances"] += home_stats["home_plate_appearances"]

    # Away team update
    team_season_stats[away_team_id]["gamesPlayed"] += 1
    team_season_stats[away_team_id]["runsScored"] += away_stats["away_runs"]
    team_season_stats[away_team_id]["runsGiven"] += home_stats["home_runs"]
    team_season_stats[away_team_id]["hits"] += away_stats["away_hits"]
    team_season_stats[away_team_id]["atBats"] += away_stats["away_at_bats"]
    team_season_stats[away_team_id]["walks"] += away_stats["away_walks"]
    team_season_stats[away_team_id]["hitByPitch"] += away_stats["away_hit_by_pitch"]
    team_season_stats[away_team_id]["sacFlies"] += away_stats["away_sac_flies"]
    team_season_stats[away_team_id]["totalBases"] += away_stats["away_total_bases"]
    team_season_stats[away_team_id]["strikeouts"] += away_stats["away_strikeouts"]
    team_season_stats[away_team_id]["plateAppearances"] += away_stats["away_plate_appearances"]


def updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_stats, away_stats):

    # we can just append because dequeue has max_len of rolling window size so if it is at max (rolling window), then 
    # it'll automatically pop the oldest game and append the newest game, keeping our rolling window of 5 most recent games

    # Home team update
    team_rolling_stats[home_team_id]["runsScored"].append(home_stats["home_runs"])
    team_rolling_stats[home_team_id]["runsGiven"].append(away_stats["away_runs"])
    team_rolling_stats[home_team_id]["hits"].append(home_stats["home_hits"])
    team_rolling_stats[home_team_id]["atBats"].append(home_stats["home_at_bats"])
    team_rolling_stats[home_team_id]["walks"].append(home_stats["home_walks"])
    team_rolling_stats[home_team_id]["hitByPitch"].append(home_stats["home_hit_by_pitch"])
    team_rolling_stats[home_team_id]["sacFlies"].append(home_stats["home_sac_flies"])
    team_rolling_stats[home_team_id]["totalBases"].append(home_stats["home_total_bases"])
    team_rolling_stats[home_team_id]["strikeouts"].append(home_stats["home_strikeouts"])
    team_rolling_stats[home_team_id]["plateAppearances"].append(home_stats["home_plate_appearances"])


    # Away team update
    team_rolling_stats[away_team_id]["runsScored"].append(away_stats["away_runs"])
    team_rolling_stats[away_team_id]["runsGiven"].append(home_stats["home_runs"])
    team_rolling_stats[away_team_id]["hits"].append(away_stats["away_hits"])
    team_rolling_stats[away_team_id]["atBats"].append(away_stats["away_at_bats"])
    team_rolling_stats[away_team_id]["walks"].append(away_stats["away_walks"])
    team_rolling_stats[away_team_id]["hitByPitch"].append(away_stats["away_hit_by_pitch"])
    team_rolling_stats[away_team_id]["sacFlies"].append(away_stats["away_sac_flies"])
    team_rolling_stats[away_team_id]["totalBases"].append(away_stats["away_total_bases"])
    team_rolling_stats[away_team_id]["strikeouts"].append(away_stats["away_strikeouts"])
    team_rolling_stats[away_team_id]["plateAppearances"].append(away_stats["away_plate_appearances"])

def calculate_obp(hits, walks, hbp, at_bats, sac_flies):

    # OBP = (Hits + Walks + Hit By Pitch) / (At Bats + Walks + Hit By Pitch + Sacrifice Flies)
    numerator = hits + walks + hbp
    denominator = at_bats + walks + hbp + sac_flies
    return numerator / denominator if denominator > 0 else 0