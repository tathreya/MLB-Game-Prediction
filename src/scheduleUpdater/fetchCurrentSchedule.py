import requests
import sqlite3
from datetime import date, datetime
import logging 

logger = logging.getLogger(__name__)

# ----------------------------- #
#        SQL STATEMENTS         #
# ----------------------------- #

CREATE_CURRENT_SCHEDULE_TABLE = """
    CREATE TABLE IF NOT EXISTS CurrentSchedule (
        game_id INTEGER PRIMARY KEY,
        season TEXT,
        game_type TEXT,
        date_time TEXT,
        home_team_id INTEGER,
        home_team TEXT,
        away_team_id INTEGER,
        away_team TEXT,
        home_score INTEGER,
        away_score INTEGER,
        status_code TEXT,
        venue_id INTEGER,
        day_night TEXT
    )
    """

INSERT_INTO_CURRENT_SCHEDULE = """
    INSERT OR IGNORE INTO CurrentSchedule (
        game_id,
        season,
        game_type,
        date_time,
        home_team_id,
        home_team,
        away_team_id,
        away_team,
        home_score,
        away_score,
        status_code,
        venue_id,
        day_night
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

UPDATE_CURRENT_SCHEDULE = """
    UPDATE CurrentSchedule
    SET
        season = ?,
        game_type = ?,
        date_time = ?,
        home_team_id = ?,
        home_team = ?,
        away_team_id = ?,
        away_team = ?,
        home_score = ?,
        away_score = ?,
        status_code = ?,
        venue_id = ?,
        day_night = ?
    WHERE game_id = ?
    """

# ----------------------------- #
#     FUNCTIONS START HERE      #
# ----------------------------- #

def fetchAndUpdateCurrentSchedule(season, base_url):
    """
    Fetches the current MLB schedule for a given season from the MLB API and updates the local SQLite database.
    It creates the CurrentSchedule table if it doesn't exist, inserts new games, and updates existing games if necessary.

    :param season: Season year as a string (e.g., "2024")
    :param base_url: Base URL of the MLB API
    :returns: None
    """
    try:

        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating CurrentSchedule table if it doesn't exist")

        createCurrentScheduleTable(cursor)
        
        cursor.execute("BEGIN TRANSACTION;")

        logger.debug("Attempting to store current MLB schedule in DB")
        
        params = {
            "sportId": 1,               # MLB
            "season": season,   # Season
            "gameType": "R",            # Regular season
        }
        
        all_season_dates = fetchCurrentScheduleFromAPI(base_url, params)

        last_regular_season_day = all_season_dates[-1]["date"]
        today_date = date.today().strftime("%Y-%m-%d")

        if (all_season_dates):
            if (today_date > last_regular_season_day):
                # TODO: fetch the playoff games
                logger.debug("Need to fetch playoff games")
            else:
                logger.debug("Playoffs not starting yet")


        entries_added = 0
        entries_updated = 0
        # iterate through each day
        for day in all_season_dates:

            # get all the games on that day
            games = day.get("games", [])

            for game in games:

                home_team_score = game.get("teams", {}).get("home", {}).get("score", None)
                away_team_score = game.get("teams", {}).get("away", {}).get("score", None)

                game_data = (game["gamePk"], game["season"], game["gameType"], game["gameDate"], 
                            game["teams"]["home"]["team"]["id"], game["teams"]["home"]["team"]["name"],
                            game["teams"]["away"]["team"]["id"], game["teams"]["away"]["team"]["name"],
                            home_team_score, away_team_score, game["status"]["detailedState"], 
                            game["venue"]["id"], game["dayNight"])

                # Check if entry with gamePk already exists in DB
                cursor.execute(
                    "SELECT * FROM CurrentSchedule WHERE game_id = ?",
                    (game["gamePk"],)
                )

                # get the fetched entry
                fetched_entry = cursor.fetchone()

                if fetched_entry:

                    # skip if the entry wasn't updated in API
                    if (fetched_entry == game_data):
                        continue
                    else:
                        # update the current schedule table
                        updateCurrentSchedule(game_data, cursor)

                        entries_updated += 1

                else:
                    insertIntoCurrentSchedule(game_data, cursor)
                    entries_added += 1
        
        conn.commit()
        logger.debug("Successfully stored and updated current MLB schedule in DB")
        logger.debug(f"Added {entries_added} entries to current MLB schedule DB")
        logger.debug(f"Updated {entries_updated} entries in current MLB schedule DB")
    
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching current MLB schedule API data: {http_err}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Other error occurred while saving current MLB schedule to DB: {e}")
        conn.rollback()
    finally:
        conn.close()

def createCurrentScheduleTable(cursor):
    """
    Creates the CurrentSchedule table in the SQLite database if it does not already exist.

    :param cursor: SQLite database cursor
    :returns: None
    """
    cursor.execute(CREATE_CURRENT_SCHEDULE_TABLE)
     
def fetchCurrentScheduleFromAPI(base_url, params):
    """
    Fetches the current MLB schedule data from the MLB API.

    :param base_url: Base URL of the MLB API
    :param params: Dictionary of parameters to send with the API request
    :returns: List of daily schedules (each containing games and metadata) from the API response
    """
    response = requests.get(base_url + "schedule", params=params)
    data = response.json()
    all_season_dates = data.get("dates", [])

    return all_season_dates

def updateCurrentSchedule(game_data, cursor):
    """
    Updates an existing game entry in the CurrentSchedule table with new data.

    :param game_data: Tuple containing game information to update
    :param cursor: SQLite database cursor
    :returns: None
    """
    updated_values = (
        game_data[1],  # season
        game_data[2],  # game_type
        game_data[3],  # date_time
        game_data[4],  # home_team_id
        game_data[5],  # home_team
        game_data[6],  # away_team_id
        game_data[7],  # away_team
        game_data[8],  # home_score
        game_data[9],  # away_score
        game_data[10], # status_code
        game_data[11], # venue_id
        game_data[12], # day_night
        game_data[0]   # id for WHERE clause
    )

    cursor.execute(UPDATE_CURRENT_SCHEDULE, updated_values)

def insertIntoCurrentSchedule(game_data, cursor):
    """
    Inserts a new game entry into the CurrentSchedule table.

    :param game_data: Tuple containing game information to insert
    :param cursor: SQLite database cursor
    :returns: None
    """
    cursor.execute(INSERT_INTO_CURRENT_SCHEDULE, game_data)