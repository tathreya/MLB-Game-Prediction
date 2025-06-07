import requests
import sqlite3

import os
from dotenv import load_dotenv

load_dotenv()

base_url = os.getenv("MLB_API_BASE_URL")
current_season = os.getenv("CURRENT_SEASON")

def fetchAndUpdateCurrentSchedule():

    params = {
        "sportId": 1,        # MLB
        "season": current_season,   # Season
        "gameType": "R",     # Regular season
    }

    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    insert_statement = """
        INSERT OR IGNORE INTO CurrentSchedule (
            id, 
            date, 
            home_team
            away_team,
            status,
            season,
            season_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """


    try:
        response = requests.get(base_url + "schedule", params=params)
        data = response.json()

        # dictionary of form {key = date: value = games on that day}
        # schedule = {}
        
        i = 0
        # iterate through each day
        for day in data.get("dates", []):

            # TODO: remove eventually
            if (i == 1):
                break

            # get the date
            date = day["date"]
            print(date)
            # get all the games on that day
            games = day.get("games", [])

            for game in games:
                print(game["gamePk"])
                print(game["season"])
                print(game["gameType"])
                print(game["gameDate"])


            i = i + 1

            #schedule[date] = games 
      

           
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching API data: {http_err}")  
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred while fetching API data: {err}")
    except Exception as e:
        print(f"Other error occurred while fetching saving games to DB: {e}")  

  
    # TODO: if current date is past the last entry in schedule, fetch playoff shcedule instead
    
