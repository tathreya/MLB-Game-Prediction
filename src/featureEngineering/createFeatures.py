import requests
import sqlite3
import logging
from collections import defaultdict, deque
import json
import os
from datetime import datetime, timezone, timedelta

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
    home_pitching_doubles INTEGER,
    home_pitching_triples INTEGER,
    home_pitching_hit_batsmen INTEGER,
    home_pitching_sac_flies INTEGER,
    home_pitching_at_bats INTEGER,
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
    away_pitching_doubles INTEGER,
    away_pitching_triples INTEGER,
    away_pitching_hit_batsmen INTEGER,
    away_pitching_sac_flies INTEGER,
    away_pitching_at_bats INTEGER,
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
    INSERT OR REPLACE INTO Features (
        game_id,
        features_json
        ) VALUES (
        ?, ?
    );
"""

SELECT_OLD_SEASON_GAMES_IN_ORDER = """
    SELECT *
    FROM OldGames
    WHERE season = ? AND status_code != 'Cancelled'
    ORDER BY date_time ASC
"""

SELECT_CURRENT_SEASON_GAMES_IN_ORDER = """
    SELECT *
    FROM CurrentSchedule
    WHERE season = ?
    AND status_code != 'Cancelled'
    AND DATE(datetime(date_time, '-4 hours')) <= DATE(datetime('now', '-4 hours'))
    ORDER BY date_time ASC;
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

        seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]
        for season in seasons:
            
            logger.debug(f"Engineering features for {season} season")

            # if it is the current season
            if (season == os.environ.get("CURRENT_SEASON")):

                games = selectCurrentSeasonGames(cursor, season)
            else:

                games = selectOldSeasonGames(cursor, season)
        
            print('starting to build features for season ' + str(season))
            print('there are this many games to process = ' + str(len(games)))

            # Outer dict maps team_id → that team's season stats
            team_season_stats = defaultdict(lambda: {

                # GENERAL STATS
                "gamesPlayed": 0,

                # OFFENSIVE/BATTING STATS
                "runsScored": 0,
                "battingHits": 0,
                "atBats": 0,
                "battingWalks": 0,
                "hitByPitch": 0,
                "sacFlies": 0, 
                "totalBases": 0,
                "strikeouts": 0,
                "plateAppearances": 0,
                "homeRuns": 0, 
                
                # DEFENSIVE/PITCHING STATS
                "runsGiven": 0,
                "pitchingHits": 0,
                "pitchingWalks": 0,
                "earnedRuns": 0,
                "inningsPitched": 0.0,
                "pitchingHitBatsmen": 0,
                "pitchingSacFlies": 0,
                "pitchingAtBats": 0,
                "pitchingDoubles": 0,
                "pitchingTriples": 0,
                "pitchingHomeRuns": 0,
                "pitchingStrikeOuts": 0,
                "pitchingBattersFaced": 0
            })

            team_rolling_stats = defaultdict(lambda: {
                # keep a deque of the last N game stats for eeach team

                # OFFENSIVE/BATTING STATS
                "runsScored": deque(maxlen=rolling_window_size),
                "battingHits": deque(maxlen=rolling_window_size),
                "atBats": deque(maxlen=rolling_window_size),
                "battingWalks": deque(maxlen=rolling_window_size),
                "hitByPitch": deque(maxlen=rolling_window_size),
                "sacFlies": deque(maxlen=rolling_window_size),
                "totalBases": deque(maxlen=rolling_window_size),
                "strikeouts": deque(maxlen=rolling_window_size),
                "plateAppearances": deque(maxlen=rolling_window_size),
                "homeRuns": deque(maxlen=rolling_window_size), 

                # DEFENSIVE/PITCHING STATS
                "runsGiven": deque(maxlen=rolling_window_size),
                "pitchingHits": deque(maxlen=rolling_window_size),
                "pitchingWalks": deque(maxlen=rolling_window_size),
                "earnedRuns": deque(maxlen=rolling_window_size),
                "inningsPitched": deque(maxlen=rolling_window_size),
                "pitchingHitBatsmen": deque(maxlen=rolling_window_size),
                "pitchingSacFlies": deque(maxlen=rolling_window_size),
                "pitchingAtBats": deque(maxlen=rolling_window_size),
                "pitchingDoubles": deque(maxlen=rolling_window_size),
                "pitchingTriples": deque(maxlen=rolling_window_size),
                "pitchingHomeRuns": deque(maxlen=rolling_window_size),
                "pitchingStrikeOuts": deque(maxlen=rolling_window_size),
                "pitchingBattersFaced": deque(maxlen=rolling_window_size)
            })

            numGamesProcessed = 0
            for game in games:
                
                game_id = game[0]

                if season == os.environ.get("CURRENT_SEASON"):
                    print(game_id)

                game_data = None

                if (boxScoreExists(cursor, game_id)):

                    if season == os.environ.get("CURRENT_SEASON"):
                        print('box score existed current season game, getting from DB')
                    game_data = reconstructGameDataFromSQL(cursor, game_id)
                  
                else:
                    response = requests.get(f"{base_url}game/{game_id}/boxscore")
                    game_data = response.json()
                    if season == os.environ.get("CURRENT_SEASON"):
                        print('box score did not exist for current season game, fetching from API')
                    
                    game_date = datetime.strptime(game[3], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)

                     # Only store if it's an older current season on game
                    if season != os.environ.get("CURRENT_SEASON") or (now - game_date > timedelta(days=14)):
                        if (season == os.environ.get("CURRENT_SEASON")):
                            print('hey, we found an old game (2 weeks an inserted into box score)')
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
                    
                # After saving the feature, update season totals to include this game for both teams
                updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_stats, away_stats)   
                # also update rolling averages
                updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_stats, away_stats)

                numGamesProcessed += 1
                if season == os.environ.get("CURRENT_SEASON"):
                    print('numGamesProcessed = ' + str(numGamesProcessed))
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

def reconstructGameDataFromSQL(cursor, game_id):
    cursor.execute("SELECT * FROM GameBoxScoreStats WHERE game_id = ?", (game_id,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"No box score found for game_id {game_id}")

    col_names = [description[0] for description in cursor.description]
    data = dict(zip(col_names, row))

    # Rebuild the structure similar to what the MLB API returns
    return {
        "teams": {
            "home": buildTeamStatsDict(data, "home"),
            "away": buildTeamStatsDict(data, "away")
        }
    }
def buildTeamStatsDict(data, prefix):
    # Reverse map flattened stats to MLB API-style nested dict
    return {
        "team": {"id": data[f"{prefix}_team_id"]},
        "teamStats": {
            "batting": {
                "runs": data[f"{prefix}_runs"],
                "hits": data[f"{prefix}_hits"],
                "doubles": data[f"{prefix}_doubles"],
                "triples": data[f"{prefix}_triples"],
                "homeRuns": data[f"{prefix}_home_runs"],
                "strikeOuts": data[f"{prefix}_strikeouts"],
                "baseOnBalls": data[f"{prefix}_walks"],
                "hitByPitch": data[f"{prefix}_hit_by_pitch"],
                "atBats": data[f"{prefix}_at_bats"],
                "plateAppearances": data[f"{prefix}_plate_appearances"],
                "totalBases": data[f"{prefix}_total_bases"],
                "sacFlies": data[f"{prefix}_sac_flies"],
                "sacBunts": data[f"{prefix}_sac_bunts"],
                "obp": data[f"{prefix}_obp"],
                "slg": data[f"{prefix}_slg"],
                "ops": data[f"{prefix}_ops"],
                "avg": data[f"{prefix}_avg"],
                "rbi": data[f"{prefix}_rbi"],
                "leftOnBase": data[f"{prefix}_left_on_base"],
                "caughtStealing": data[f"{prefix}_caught_stealing"],
                "stolenBases": data[f"{prefix}_stolen_bases"],
                "stolenBasePercentage": data[f"{prefix}_stolen_base_percentage"],
                "groundIntoDoublePlay": data[f"{prefix}_ground_into_double_play"],
                "groundIntoTriplePlay": data[f"{prefix}_ground_into_triple_play"],
                "pickoffs": data[f"{prefix}_pickoffs_batting"]
            },
            "pitching": {
                "earnedRuns": data[f"{prefix}_earned_runs"],
                "inningsPitched": data[f"{prefix}_innings_pitched"],
                "strikeOuts": data[f"{prefix}_pitching_strikeouts"],
                "baseOnBalls": data[f"{prefix}_pitching_walks"],
                "hits": data[f"{prefix}_pitching_hits"],
                "doubles": data[f"{prefix}_pitching_doubles"],
                "triples": data[f"{prefix}_pitching_triples"],
                "hitBatsmen": data[f"{prefix}_pitching_hit_batsmen"],
                "sacFlies": data[f"{prefix}_pitching_sac_flies"],
                "atBats": data[f"{prefix}_pitching_at_bats"],
                "homeRuns": data[f"{prefix}_pitching_home_runs"],
                "era": data[f"{prefix}_pitching_era"],
                "whip": data[f"{prefix}_pitching_whip"],
                "obp": data[f"{prefix}_pitching_obp"],
                "battersFaced": data[f"{prefix}_pitching_batters_faced"],
                "strikes": data[f"{prefix}_pitching_strikes"],
                "balls": data[f"{prefix}_pitching_balls"],
                "strikePercentage": data[f"{prefix}_pitching_strike_pct"],
                "pickoffs": data[f"{prefix}_pitching_pickoffs"],
                "inheritedRunners": data[f"{prefix}_pitching_inherited_runners"],
                "inheritedRunnersScored": data[f"{prefix}_pitching_inherited_runners_scored"]
            },
            "fielding": {
                "errors": data[f"{prefix}_errors"],
                "assists": data[f"{prefix}_assists"],
                "putOuts": data[f"{prefix}_putouts"],
                "chances": data[f"{prefix}_fielding_chances"],
                "passedBall": data[f"{prefix}_passed_ball"],
                "caughtStealing": data[f"{prefix}_fielding_caught_stealing"],
                "stolenBases": data[f"{prefix}_fielding_stolen_bases"],
                "stolenBasePercentage": data[f"{prefix}_fielding_stolen_base_pct"],
                "pickoffs": data[f"{prefix}_fielding_pickoffs"]
            }
        }
    }

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
        f"{prefix}_pitching_doubles": pitching.get("doubles", 0),
        f"{prefix}_pitching_triples": pitching.get("triples", 0),
        f"{prefix}_pitching_hit_batsmen": pitching.get("hitBatsmen", 0),
        f"{prefix}_pitching_sac_flies": pitching.get("sacFlies", 0),
        f"{prefix}_pitching_at_bats": pitching.get("atBats", 0),
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

def selectOldSeasonGames(cursor, old_season):
    cursor.execute(SELECT_OLD_SEASON_GAMES_IN_ORDER, (old_season,))
    games = cursor.fetchall()
    return games

def selectCurrentSeasonGames(cursor, current_season):
    cursor.execute(SELECT_CURRENT_SEASON_GAMES_IN_ORDER, (current_season,))
    games = cursor.fetchall()
    return games

def calculate_metrics(stats, games=None):
    if games is None:
        games = stats.get("gamesPlayed", 0)

    # ----------------------------- #
    #    BATTING / OFFENSIVE STATS  #
    # ----------------------------- #

    runs_scored = stats.get("runsScored", 0)
    batting_hits = stats.get("battingHits", 0)
    at_bats = stats.get("atBats", 0)
    batting_walks = stats.get("battingWalks", 0)
    hit_by_pitch = stats.get("hitByPitch", 0)
    sac_flies = stats.get("sacFlies", 0)
    total_bases = stats.get("totalBases", 0)
    plate_appearances = stats.get("plateAppearances", 0)
    home_runs = stats.get("homeRuns", 0)
    strikeouts = stats.get("strikeouts", 0)

    # average runs scored
    avg_runs_scored = runs_scored / games if games > 0 else 0
    # batting avg
    avg_batting_avg = batting_hits / at_bats if at_bats > 0 else 0

    # OBP, SLG, OPS
    avg_obp = calculate_obp(batting_hits, batting_walks, hit_by_pitch, at_bats, sac_flies)
    avg_slg = total_bases / at_bats if at_bats > 0 else 0
    avg_ops = avg_obp + avg_slg

    # batting K%, BB%, BABIP
    batting_k_pct = strikeouts / plate_appearances if plate_appearances > 0 else 0
    bb_pct = batting_walks / plate_appearances if plate_appearances > 0 else 0
    babip = calculate_babip(batting_hits, home_runs, at_bats, strikeouts, sac_flies)

    # ----------------------------- #
    #    PITCHING / DEFENSIVE STATS #
    # ----------------------------- #
    runs_given = stats.get("runsGiven", 0)
    earned_runs = stats.get("earnedRuns", 0)
    innings_pitched = stats.get("inningsPitched", 0)
    hits_allowed = stats.get("pitchingHits", 0)
    walks_allowed = stats.get("pitchingWalks", 0)
    pitching_hit_batsmen = stats.get("pitchingHitBatsmen", 0)
    pitching_at_bats = stats.get("pitchingAtBats", 0)
    pitching_sac_flies = stats.get("pitchingSacFlies", 0)
    pitching_strikeouts = stats.get("pitchingStrikeOuts", 0)
    pitching_batters_faced = stats.get("pitchingBattersFaced", 0)
    pitching_doubles = stats.get("pitchingDoubles", 0)
    pitching_triples = stats.get("pitchingTriples", 0)
    pitching_home_runs = stats.get("pitchingHomeRuns", 0)

    # runs given
    avg_runs_given = runs_given / games if games > 0 else 0

    # Opponent OBP, SLG and OPS
    opponent_obp = calculate_opponent_obp(
        hits_allowed,
        walks_allowed,
        pitching_hit_batsmen,
        pitching_at_bats,
        pitching_sac_flies
    )
    pitching_singles = hits_allowed - pitching_doubles - pitching_triples - pitching_home_runs
    total_bases_allowed = (
        1 * pitching_singles +
        2 * pitching_doubles +
        3 * pitching_triples +
        4 * pitching_home_runs
    )
    opponent_slg = total_bases_allowed / pitching_at_bats if pitching_at_bats > 0 else 0
    opponent_ops = opponent_obp + opponent_slg

    # ERA
    era = (earned_runs * 9) / innings_pitched if innings_pitched > 0 else 0
    # WHIP
    whip = (hits_allowed + walks_allowed) / innings_pitched if innings_pitched > 0 else 0
    # K/9: Strikeouts per 9 innings 
    k_per_9 = (pitching_strikeouts * 9) / innings_pitched if innings_pitched > 0 else 0
    # Pitching K%
    pitching_k_pct = pitching_strikeouts / pitching_batters_faced if pitching_batters_faced > 0 else 0
    # BB/9 (walks per 9)
    bb_per_9 = (walks_allowed * 9) / innings_pitched if innings_pitched > 0 else 0
    # HR/9 (home runs per 9)
    hr_per_9 = (pitching_home_runs * 9) / innings_pitched if innings_pitched > 0 else 0

    return {
        "runs_scored": avg_runs_scored,
        "batting_avg": avg_batting_avg,
        "obp": avg_obp,
        "slg": avg_slg,
        "ops": avg_ops,
        "batting_k_pct": batting_k_pct,
        "bb_pct": bb_pct,
        "babip": babip,
        "runs_given": avg_runs_given,
        "era": era,
        "whip": whip, 
        "opponent_obp": opponent_obp,
        "opponent_slg": opponent_slg,
        "opponent_ops": opponent_ops,
        "k_per_9": k_per_9,
        "pitching_k_pct": pitching_k_pct,
        "bb_per_9": bb_per_9,
        "hr_per_9": hr_per_9,
    }

def buildFeatures(team_season_stats, team_rolling_stats, home_team_id, away_team_id, home_runs_scored, away_runs_scored):

    features = {}

    for team_type, team_id in [("home", home_team_id), ("away", away_team_id)]:
        # Season stats
        season_stats = team_season_stats[team_id]
        season_metrics = calculate_metrics(season_stats)

        # Rolling stats: convert deque/list stats to sums
        rolling_stats = {
            stat_name: sum(stat_values)
            for stat_name, stat_values in team_rolling_stats[team_id].items()
        }
        number_rolling_games = len(team_rolling_stats[team_id]["runsScored"])
        rolling_metrics = calculate_metrics(rolling_stats, games=number_rolling_games)

        # Add team IDs 
        features[f"{team_type}_team_id"] = team_id

        # Add season metrics with prefix 
        for key, val in season_metrics.items():
            features[f"season_{team_type}_avg_{key}"] = val

        # Add rolling metrics with prefix 
        for key, val in rolling_metrics.items():
            features[f"rolling_{team_type}_avg_{key}"] = val

    # TODO: might change this to not be a binary classificatino instead predict final score or probability
    # of win
    features["label"] = 1 if home_runs_scored > away_runs_scored else 0

    return features

def updateTeamSeasonStats(team_season_stats, home_team_id, away_team_id, home_stats, away_stats):
    # Home team update
    team_season_stats[home_team_id]["gamesPlayed"] += 1
    team_season_stats[home_team_id]["runsScored"] += home_stats["home_runs"]
    team_season_stats[home_team_id]["runsGiven"] += away_stats["away_runs"]
    team_season_stats[home_team_id]["battingHits"] += home_stats["home_hits"]
    team_season_stats[home_team_id]["atBats"] += home_stats["home_at_bats"]
    team_season_stats[home_team_id]["battingWalks"] += home_stats["home_walks"]
    team_season_stats[home_team_id]["hitByPitch"] += home_stats["home_hit_by_pitch"]
    team_season_stats[home_team_id]["sacFlies"] += home_stats["home_sac_flies"]
    team_season_stats[home_team_id]["totalBases"] += home_stats["home_total_bases"]
    team_season_stats[home_team_id]["strikeouts"] += home_stats["home_strikeouts"]
    team_season_stats[home_team_id]["plateAppearances"] += home_stats["home_plate_appearances"]
    team_season_stats[home_team_id]["homeRuns"] += home_stats["home_home_runs"]

    team_season_stats[home_team_id]["earnedRuns"] += home_stats["home_earned_runs"]
    team_season_stats[home_team_id]["inningsPitched"] += home_stats["home_innings_pitched"]
    team_season_stats[home_team_id]["pitchingHits"] += home_stats["home_pitching_hits"]
    team_season_stats[home_team_id]["pitchingWalks"] += home_stats["home_pitching_walks"]
    team_season_stats[home_team_id]["pitchingHitBatsmen"] += home_stats["home_pitching_hit_batsmen"]
    team_season_stats[home_team_id]["pitchingSacFlies"] += home_stats["home_pitching_sac_flies"]
    team_season_stats[home_team_id]["pitchingAtBats"] += home_stats["home_pitching_at_bats"]
    team_season_stats[home_team_id]["pitchingDoubles"] += home_stats["home_pitching_doubles"]
    team_season_stats[home_team_id]["pitchingTriples"] += home_stats["home_pitching_triples"]
    team_season_stats[home_team_id]["pitchingHomeRuns"] += home_stats["home_pitching_home_runs"]
    team_season_stats[home_team_id]["pitchingStrikeOuts"] += home_stats["home_pitching_strikeouts"]
    team_season_stats[home_team_id]["pitchingBattersFaced"] += home_stats["home_pitching_batters_faced"]

    # Away team update
    team_season_stats[away_team_id]["gamesPlayed"] += 1
    team_season_stats[away_team_id]["runsScored"] += away_stats["away_runs"]
    team_season_stats[away_team_id]["runsGiven"] += home_stats["home_runs"]
    team_season_stats[away_team_id]["battingHits"] += away_stats["away_hits"]
    team_season_stats[away_team_id]["atBats"] += away_stats["away_at_bats"]
    team_season_stats[away_team_id]["battingWalks"] += away_stats["away_walks"]
    team_season_stats[away_team_id]["hitByPitch"] += away_stats["away_hit_by_pitch"]
    team_season_stats[away_team_id]["sacFlies"] += away_stats["away_sac_flies"]
    team_season_stats[away_team_id]["totalBases"] += away_stats["away_total_bases"]
    team_season_stats[away_team_id]["strikeouts"] += away_stats["away_strikeouts"]
    team_season_stats[away_team_id]["plateAppearances"] += away_stats["away_plate_appearances"]
    team_season_stats[away_team_id]["homeRuns"] += away_stats["away_home_runs"]

    team_season_stats[away_team_id]["earnedRuns"] += away_stats["away_earned_runs"]
    team_season_stats[away_team_id]["inningsPitched"] += away_stats["away_innings_pitched"]
    team_season_stats[away_team_id]["pitchingHits"] += away_stats["away_pitching_hits"]
    team_season_stats[away_team_id]["pitchingWalks"] += away_stats["away_pitching_walks"]
    team_season_stats[away_team_id]["pitchingHitBatsmen"] += away_stats["away_pitching_hit_batsmen"]
    team_season_stats[away_team_id]["pitchingSacFlies"] += away_stats["away_pitching_sac_flies"]
    team_season_stats[away_team_id]["pitchingAtBats"] += away_stats["away_pitching_at_bats"]
    team_season_stats[away_team_id]["pitchingDoubles"] += away_stats["away_pitching_doubles"]
    team_season_stats[away_team_id]["pitchingTriples"] += away_stats["away_pitching_triples"]
    team_season_stats[away_team_id]["pitchingHomeRuns"] += away_stats["away_pitching_home_runs"]
    team_season_stats[away_team_id]["pitchingStrikeOuts"] += away_stats["away_pitching_strikeouts"]
    team_season_stats[away_team_id]["pitchingBattersFaced"] += away_stats["away_pitching_batters_faced"]

def updateTeamRollingStats(team_rolling_stats, home_team_id, away_team_id, home_stats, away_stats):

    # we can just append because dequeue has max_len of rolling window size so if it is at max (rolling window), then 
    # it'll automatically pop the oldest game and append the newest game, keeping our rolling window of 5 most recent games

    # Home team update
    team_rolling_stats[home_team_id]["runsScored"].append(home_stats["home_runs"])
    team_rolling_stats[home_team_id]["runsGiven"].append(away_stats["away_runs"])
    team_rolling_stats[home_team_id]["battingHits"].append(home_stats["home_hits"])
    team_rolling_stats[home_team_id]["atBats"].append(home_stats["home_at_bats"])
    team_rolling_stats[home_team_id]["battingWalks"].append(home_stats["home_walks"])
    team_rolling_stats[home_team_id]["hitByPitch"].append(home_stats["home_hit_by_pitch"])
    team_rolling_stats[home_team_id]["sacFlies"].append(home_stats["home_sac_flies"])
    team_rolling_stats[home_team_id]["totalBases"].append(home_stats["home_total_bases"])
    team_rolling_stats[home_team_id]["strikeouts"].append(home_stats["home_strikeouts"])
    team_rolling_stats[home_team_id]["plateAppearances"].append(home_stats["home_plate_appearances"])
    team_rolling_stats[home_team_id]["homeRuns"].append(home_stats["home_home_runs"])
    
    team_rolling_stats[home_team_id]["earnedRuns"].append(home_stats["home_earned_runs"])
    team_rolling_stats[home_team_id]["inningsPitched"].append(home_stats["home_innings_pitched"])
    team_rolling_stats[home_team_id]["pitchingHits"].append(home_stats["home_pitching_hits"])
    team_rolling_stats[home_team_id]["pitchingWalks"].append(home_stats["home_pitching_walks"])
    team_rolling_stats[home_team_id]["pitchingHitBatsmen"].append(home_stats["home_pitching_hit_batsmen"])
    team_rolling_stats[home_team_id]["pitchingSacFlies"].append(home_stats["home_pitching_sac_flies"])
    team_rolling_stats[home_team_id]["pitchingAtBats"].append(home_stats["home_pitching_at_bats"])
    team_rolling_stats[home_team_id]["pitchingDoubles"].append(home_stats["home_pitching_doubles"])
    team_rolling_stats[home_team_id]["pitchingTriples"].append(home_stats["home_pitching_triples"])
    team_rolling_stats[home_team_id]["pitchingHomeRuns"].append(home_stats["home_pitching_home_runs"])
    team_rolling_stats[home_team_id]["pitchingStrikeOuts"].append(home_stats["home_pitching_strikeouts"])
    team_rolling_stats[home_team_id]["pitchingBattersFaced"].append(home_stats["home_pitching_batters_faced"])

    # Away team update
    team_rolling_stats[away_team_id]["runsScored"].append(away_stats["away_runs"])
    team_rolling_stats[away_team_id]["runsGiven"].append(home_stats["home_runs"])
    team_rolling_stats[away_team_id]["battingHits"].append(away_stats["away_hits"])
    team_rolling_stats[away_team_id]["atBats"].append(away_stats["away_at_bats"])
    team_rolling_stats[away_team_id]["battingWalks"].append(away_stats["away_walks"])
    team_rolling_stats[away_team_id]["hitByPitch"].append(away_stats["away_hit_by_pitch"])
    team_rolling_stats[away_team_id]["sacFlies"].append(away_stats["away_sac_flies"])
    team_rolling_stats[away_team_id]["totalBases"].append(away_stats["away_total_bases"])
    team_rolling_stats[away_team_id]["strikeouts"].append(away_stats["away_strikeouts"])
    team_rolling_stats[away_team_id]["plateAppearances"].append(away_stats["away_plate_appearances"])
    team_rolling_stats[away_team_id]["homeRuns"].append(away_stats["away_home_runs"])

    team_rolling_stats[away_team_id]["earnedRuns"].append(away_stats["away_earned_runs"])
    team_rolling_stats[away_team_id]["inningsPitched"].append(away_stats["away_innings_pitched"])
    team_rolling_stats[away_team_id]["pitchingHits"].append(away_stats["away_pitching_hits"])
    team_rolling_stats[away_team_id]["pitchingWalks"].append(away_stats["away_pitching_walks"])
    team_rolling_stats[away_team_id]["pitchingHitBatsmen"].append(away_stats["away_pitching_hit_batsmen"])
    team_rolling_stats[away_team_id]["pitchingSacFlies"].append(away_stats["away_pitching_sac_flies"])
    team_rolling_stats[away_team_id]["pitchingAtBats"].append(away_stats["away_pitching_at_bats"])
    team_rolling_stats[away_team_id]["pitchingDoubles"].append(away_stats["away_pitching_doubles"])
    team_rolling_stats[away_team_id]["pitchingTriples"].append(away_stats["away_pitching_triples"])
    team_rolling_stats[away_team_id]["pitchingHomeRuns"].append(away_stats["away_pitching_home_runs"])
    team_rolling_stats[away_team_id]["pitchingStrikeOuts"].append(away_stats["away_pitching_strikeouts"])
    team_rolling_stats[away_team_id]["pitchingBattersFaced"].append(away_stats["away_pitching_batters_faced"])

def calculate_obp(hits, walks, hbp, at_bats, sac_flies):

    # OBP = (Hits + Walks + Hit By Pitch) / (At Bats + Walks + Hit By Pitch + Sacrifice Flies)
    numerator = hits + walks + hbp
    denominator = at_bats + walks + hbp + sac_flies
    return numerator / denominator if denominator > 0 else 0

def calculate_opponent_obp(hits_allowed, walks_allowed, hit_batsmen, at_bats, sac_flies):

    # Opponent OBP = (Hits + Walks + Hit By Pitch) / (At Bats + Walks + Hit By Pitch + Sacrifice Flies)
    numerator = hits_allowed + walks_allowed + hit_batsmen
    denominator = at_bats + walks_allowed + hit_batsmen + sac_flies
    return numerator / denominator if denominator > 0 else 0

def calculate_babip(hits, home_runs, at_bats, strikeouts, sac_flies):

    # BABIP = (Hits - Home Runs) / (At-Bats - Strikeouts - Home Runs + Sac Flies)
    numerator = hits - home_runs
    denominator = at_bats - strikeouts - home_runs + sac_flies
    return numerator / denominator if denominator > 0 else 0