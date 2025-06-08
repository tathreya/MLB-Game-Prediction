import requests
import sqlite3
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

base_url = os.getenv("MLB_API_BASE_URL")
current_season = os.getenv("CURRENT_SEASON")

def fetchAndUpdateCurrentSchedule():

    params = {
        "sportId": 1,               # MLB
        "season": current_season,   # Season
        "gameType": "R",            # Regular season
    }

    conn = sqlite3.connect("databases/MLB_Betting.db")
    cursor = conn.cursor()

    insert_statement = """
       INSERT OR IGNORE INTO CurrentSchedule (
            id,
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
    try:
        response = requests.get(base_url + "schedule", params=params)
        data = response.json()
        all_season_dates = data.get("dates", [])

        # TODO: if current date is past the last entry in schedule, fetch playoff shcedule instead
        last_regular_season_day = all_season_dates[-1]["date"]
        today_date = date.today().strftime("%Y-%m-%d")

        
        if (all_season_dates):
            if (today_date > last_regular_season_day):
                # TODO: fetch the playoff games
                print("fetching playoff games")
            else:
                print("not playoffs")

        i = 0

        # iterate through each day
        for day in all_season_dates:

            # TODO: remove eventually
            if (i == 1):
                break

            # get all the games on that day
            games = day.get("games", [])

            for game in games:

                game_data = (game["gamePk"], game["season"], game["gameType"], game["gameDate"], 
                            game["teams"]["home"]["team"]["id"], game["teams"]["home"]["team"]["name"],
                            game["teams"]["away"]["team"]["id"], game["teams"]["away"]["team"]["name"],
                            game["teams"]["home"]["score"], game["teams"]["away"]["score"],
                             game["status"]["detailedState"], game["venue"]["id"], game["dayNight"])
                
                print(game_data)
                
             
                #, if it doesn't then we gotta update the DB entry because something 
                # was updated, ie the game finished 

                # Check if entry with gamePk already exists in DB
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM CurrentSchedule WHERE id = ?)",
                    (game["gamePk"],)
                )
                exists = cursor.fetchone()[0] 

                if exists:
                    
                    print("Game already in DB")

                    #  check if all the data from API matches all the data in entry already in the table, if it does
                    #  then skip no need to add its already there

                    # if it doesn't match (ie status or something changed), update it

                else:
                    print("Game not found, adding the game")
                    cursor.execute(insert_statement, game_data)
                
            i = i + 1
        
        conn.commit()
    
           
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching API data: {http_err}")  
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred while fetching API data: {err}")
    except Exception as e:
        print(f"Other error occurred while fetching saving games to DB: {e}")  

  

    
