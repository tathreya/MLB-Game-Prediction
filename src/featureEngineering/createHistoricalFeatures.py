import requests
import sqlite3
import logging 

logger = logging.getLogger(__name__)

"""""
Steps:
-------

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

        old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
        for old_season in old_seasons:

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

    except Exception as e:
        logger.error(f"Other error occurred while engineering features and saving to DB: {e}")
        conn.rollback()
    finally:
        conn.close()
   
    