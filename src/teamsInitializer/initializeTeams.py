import requests
import os
from dotenv import load_dotenv
import sqlite3

load_dotenv()

base_url = os.getenv("MLB_API_BASE_URL")

def fetchMLBTeams():

    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()
    insert_statement = """
        INSERT OR IGNORE INTO Teams (
            id, 
            name, 
            abbreviation,
            shortName
        ) VALUES (?, ?, ?, ?)
    """
    try:
        response = requests.get(base_url + "teams")
        data = response.json()
        all_teams = data.get("teams")
        mlb_teams = [team for team in all_teams if team.get("sport", {}).get("name", {}) == 'Major League Baseball']

        for mlb_team in mlb_teams:
            team_to_insert = (mlb_team["id"], mlb_team["name"], mlb_team["abbreviation"], mlb_team["shortName"])
            cursor.execute(insert_statement, team_to_insert)

        conn.commit()
  

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching API data: {http_err}")  
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred while fetching API data: {err}")  
    except Exception as e:
         print("Error occured writing teams to DB:", e)
    finally:
        conn.close()