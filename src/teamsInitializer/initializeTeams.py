import requests
import sqlite3
import logging 

logger = logging.getLogger(__name__)

insert_into_table_statement = """
    INSERT OR IGNORE INTO Teams (
        team_id, 
        name, 
        abbreviation,
        short_name
    ) VALUES (?, ?, ?, ?)
"""

create_table_statement = """
CREATE TABLE IF NOT EXISTS Teams (
    team_id INTEGER PRIMARY KEY,
    name TEXT,
    abbreviation TEXT,
    short_name TEXT
)
"""

def fetchMLBTeams(base_url):

    try:

        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating Teams table if it doesn't exist")
        createTeamsTable(cursor)

        logger.debug("Attempting to initialize MLB Teams in DB")

        cursor.execute("BEGIN TRANSACTION;")
        
        mlb_teams = fetchTeamsFromAPI(base_url)

        for mlb_team in mlb_teams:
            insertIntoTable(mlb_team, cursor)

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

def createTeamsTable(cursor):

    cursor.execute(create_table_statement)

def insertIntoTable(mlb_team, cursor):

    team_to_insert = (mlb_team["id"], mlb_team["name"], mlb_team["abbreviation"], mlb_team["shortName"])
    cursor.execute(insert_into_table_statement, team_to_insert)

def fetchTeamsFromAPI(base_url):

    response = requests.get(base_url + "teams")
    data = response.json()
    all_teams = data.get("teams")
    mlb_teams = [team for team in all_teams if team.get("sport", {}).get("name") == 'Major League Baseball']

    return mlb_teams