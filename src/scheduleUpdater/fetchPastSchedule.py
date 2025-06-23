import requests
import sqlite3
from dotenv import load_dotenv
from datetime import datetime
import logging 

load_dotenv()
logger = logging.getLogger(__name__)


def fetchAndUpdateOldSeason(season, base_url):
    try:

        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        # Create OldGames table if it doesn't exist
        create_statement = """
            CREATE TABLE IF NOT EXISTS OldGames (
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

        logger.debug("Creating OldGames table if it doesn't exist")
        cursor.execute(create_statement)

        cursor.execute("BEGIN TRANSACTION;")

        logger.debug(f"Attempting to store {season} MLB schedule in DB")

        params = {
            "sportId": 1,               # MLB
            "season": season,           # Season
            "gameType": "R",            # Regular season
        }
        
        response = requests.get(base_url + "schedule", params=params)
        data = response.json()
        all_season_dates = data.get("dates", [])

        # TODO: eventually fetch playoff games from old seasons

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
                    "SELECT * FROM OldGames WHERE game_id = ?",
                    (game["gamePk"],)
                )

                fetched_entry = cursor.fetchone()

                if fetched_entry:

                    # skip if the entry wasn't updated in API
                    if (fetched_entry == game_data):
                        continue
                    else:
                        # if it doesn't match (ie status or something changed), update it
                        api_date_str = game_data[3]           
                        db_date_str = fetched_entry[3]    
                    
                        api_date = datetime.fromisoformat(api_date_str.replace('Z', '+00:00'))
                        db_date = datetime.fromisoformat(db_date_str.replace('Z', '+00:00'))

                        # Skip the update if DB already has a later date
                        if db_date > api_date:
                            continue     

                        update_statement = """
                            UPDATE OldGames
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

                        cursor.execute(update_statement, updated_values)
                        entries_updated += 1

                else:
                    insert_statement = """
                        INSERT OR IGNORE INTO OldGames (
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
                                ) VALUES (
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                            );
                    """
                    cursor.execute(insert_statement, game_data)
                    entries_added += 1
        
        conn.commit()
        logger.debug(f"Successfully stored and updated {season} MLB schedule in DB")
        logger.debug(f"Added {entries_added} entries to {season} MLB schedule DB")
        logger.debug(f"Updated {entries_updated} entries in {season} MLB schedule DB")
    
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching {season} MLB schedule API data: {http_err}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Other error occurred while saving {season} MLB schedule to DB: {e}")
        conn.rollback()
    finally:
        conn.close()