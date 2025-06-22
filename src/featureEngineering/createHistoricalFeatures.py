import requests
import sqlite3
import logging
from collections import defaultdict 

logger = logging.getLogger(__name__)

"""""

1. Loop over each season (2015–2024):
    - For each game in the season (skipping the first N games, where N is the rolling window size):

2. For each team in the game (home and away):
    - Fetch the previous N games played before the current game date to compute rolling stats.
    - Fetch all games played earlier in the same season to compute season-to-date stats.
    - Compute relevant team-level stats:
        • Rolling stats (last N games):
            - OBP (On-base Percentage)
            - SLG (Slugging Percentage)
            - ERA (Earned Run Average)
            - WHIP, etc.
        • Season stats (from start of season to current game date):
            - OBP, SLG, ERA, etc.

3. Store results in a new GameFeatures table:
    - game_id
    - home_team_id, away_team_id
    - All computed feature columns (rolling + season stats for both teams)
    - Final label (e.g., home team win = 1 / 0, or run differential for regression)
"""

def engineerFeatures(rolling_window_size, base_url):

    try:
        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating Features table if it doesn't exist")
        # TODO figure out what columns for this table
        # create_statement = """
        #     CREATE TABLE IF NOT EXISTS Features (
        #         id INTEGER PRIMARY KEY,
        #         season TEXT,
        #         game_type TEXT,
        #         date_time TEXT,
        #         home_team_id INTEGER,
        #         home_team TEXT,
        #         away_team_id INTEGER,
        #         away_team TEXT,
        #         home_score INTEGER,
        #         away_score INTEGER,
        #         status_code TEXT,
        #         venue_id INTEGER,
        #         day_night TEXT
        #     )
        # """

        cursor.execute("BEGIN TRANSACTION;")

        logger.debug("Attempting to engineer features for past seasons")

        # Outer dict maps team_id → that team's season stats
        team_season_stats = defaultdict(lambda: {
            "games_played": 0,
            "runs": 0,
            # "hits": 0,
            # "at_bats": 0,
            # "walks": 0,
            # "strikeouts": 0,
            # "hit_by_pitch": 0,
            # "plate_appearances": 0,
            # "total_bases": 0,
            # "doubles": 0,
            # "triples": 0,
            # "home_runs": 0,
            # "sac_flies": 0,
            # "innings_pitched": 0.0,
            # "earned_runs": 0,
            # "walks_allowed": 0,
            # "hits_allowed": 0,
            # "strikeouts_pitched": 0,
            # "home_runs_allowed": 0,
            # "errors": 0,
            # "putouts": 0,
            # "assists": 0,
        })

        old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
        for old_season in old_seasons:

            if (old_season != '2015'):
                break
            select_season_statement = """
                SELECT *
                FROM OldGames
                WHERE season = ?
                ORDER BY date_time ASC
            """
            cursor.execute(select_season_statement, (old_season,))
            games = cursor.fetchall()
            print(len(games))
            print(games[0])

            for index, game in enumerate(games):
                
                game_id = game[0]
                print(game_id)
                response = requests.get(f"{base_url}game/{game_id}/boxscore")
                game_data = response.json()
                home_team_id = game_data["teams"]["home"]["team"]["id"]
                away_team_id = game_data["teams"]["away"]["team"]["id"]
                print(home_team_id)
                print(away_team_id)

                # TODO: update the team_season_stats dictionary with relevant data
                
                # if the total number of games played for that team after updating becomes greater than N (rolling size window), 
                # then we actually store that game with features in the Features DB with rolling average equal to season average
                # till now

                # otherwise, we just update the dictionary and don't store that game in the Features DB

                # TODO: how to update rolling average? maybe after total games played % N is 1, reset the rolling stats back to 0 

                break

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching MLB boxscore data: {http_err}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Other error occurred while engineering features and saving to DB: {e}")
        conn.rollback()
    finally:
        conn.close()
   
    