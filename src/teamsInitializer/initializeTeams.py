import requests
import sqlite3
import logging 

logger = logging.getLogger(__name__)

def fetchMLBTeams(base_url):

    try:

        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating Teams table if it doesn't exist")
        create_statement = """
            CREATE TABLE IF NOT EXISTS Teams (
                id INTEGER PRIMARY KEY,
                name TEXT,
                abbreviation TEXT,
                short_name TEXT
            )
        """
        cursor.execute(create_statement)

        logger.debug("Attempting to initialize MLB Teams in DB")

        cursor.execute("BEGIN TRANSACTION;")
        
        response = requests.get(base_url + "teams")
        data = response.json()
        all_teams = data.get("teams")
        mlb_teams = [team for team in all_teams if team.get("sport", {}).get("name") == 'Major League Baseball']

        insert_statement = """
            INSERT OR IGNORE INTO Teams (
                id, 
                name, 
                abbreviation,
                short_name
            ) VALUES (?, ?, ?, ?)
        """

        for mlb_team in mlb_teams:
            team_to_insert = (mlb_team["id"], mlb_team["name"], mlb_team["abbreviation"], mlb_team["shortName"])
            cursor.execute(insert_statement, team_to_insert)

        conn.commit()
        
        logger.debug("MLB Teams successfully initialized in DB")

    except requests.exceptions.RequestException as err:
        logger.error(f"Error occurred while fetching MLB Teams API data: {err}")
        conn.rollback()  
    except sqlite3.DatabaseError as db_err:
        logger.error(f"Database error occurred when initializing MLB Teams: {db_err}")
        conn.rollback()  
    except Exception as e:
        logger.error(f"An error occurred when initializing MLB Teams: {e}")
        conn.rollback() 
    finally:
        conn.close()