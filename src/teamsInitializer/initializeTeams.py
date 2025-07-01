import requests
import sqlite3
import logging 

logger = logging.getLogger(__name__)

# ----------------------------- #
#        SQL STATEMENTS         #
# ----------------------------- #

CREATE_TEAMS_TABLE = """
    CREATE TABLE IF NOT EXISTS Teams (
        team_id INTEGER PRIMARY KEY,
        name TEXT,
        abbreviation TEXT,
        short_name TEXT
    )
    """

INSERT_INTO_TEAMS = """
    INSERT OR IGNORE INTO Teams (
        team_id, 
        name, 
        abbreviation,
        short_name
        ) VALUES (?, ?, ?, ?)
    """

# ----------------------------- #
#     FUNCTIONS START HERE      #
# ----------------------------- #

def fetchMLBTeams(base_url):

    """
    Fetch MLB team data from the API and store it in the Teams table in the SQLite database.

    :param base_url: The base URL of the MLB API.
    :return: None
    """

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
    """
    Creates the Teams table if it does not already exist in the SQLite database.

    :param cursor: SQLite database cursor
    :returns: None
    """
    cursor.execute(CREATE_TEAMS_TABLE)

def insertIntoTable(mlb_team, cursor):
    """
    Inserts an MLB team record into the Teams table.

    :param mlb_team: Dictionary representing an MLB team with keys 'id', 'name', 
                     'abbreviation', and 'shortName'
    :param cursor: SQLite database cursor
    :returns: None
    """
    team_to_insert = (mlb_team["id"], mlb_team["name"], mlb_team["abbreviation"], mlb_team["shortName"])
    cursor.execute(INSERT_INTO_TEAMS, team_to_insert)

def fetchTeamsFromAPI(base_url):
    """
    Fetches the list of MLB teams from the MLB API.

    :param base_url: Base URL of the MLB API
    :returns: List of dictionaries, each representing an MLB team filtered by sport name 'Major League Baseball'
    """
    response = requests.get(base_url + "teams")
    data = response.json()
    all_teams = data.get("teams")
    mlb_teams = [team for team in all_teams if team.get("sport", {}).get("name") == 'Major League Baseball']

    return mlb_teams