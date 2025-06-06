import requests

import os
from dotenv import load_dotenv

load_dotenv()

base_url = os.getenv("MLB_API_BASE_URL")
current_season = os.getenv("CURRENT_SEASON")

def fetchPastMLBSchedule():

    print('hello')

def fetchCurrentMLBSchedule():

    params = {
        "sportId": 1,        # MLB
        "season": current_season,   # Season
        "gameType": "R",     # Regular season
    }

    try:
        response = requests.get(base_url + "schedule", params=params)
        data = response.json()

        # dictionary of form {key = date: value = games on that day}
        schedule = {}
        
        i = 0
        # iterate through each day
        for day in data.get("dates", []):

            # get the date
            date = day["date"]
            # get all the games on that day
            games = day.get("games", [])
            schedule[date] = games 
            # TODO: remove eventually
            if (i == 2):
                break

        for game in schedule.get("2025-03-27"):
            print(game)

        # TODO: create the DB schema

           
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching API data: {http_err}")  
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred while fetching API data: {err}")  

  
    # TODO: if current date is past the last entry in schedule, fetch playoff shcedule instead
    




def _main():
    fetchCurrentMLBSchedule()

_main()