
from teamsInitializer.initializeTeams import fetchMLBTeams
from scheduleUpdater.fetchCurrentSchedule import fetchAndUpdateCurrentSchedule
from scheduleUpdater.fetchOldSeasons import fetchAndUpdateOldSeason
from featureEngineering.createHistoricalFeatures import engineerFeatures
import logging
import os 
from dotenv import load_dotenv
load_dotenv()

# Set up basic configuration
logging.basicConfig(
    filename='app.log',
    filemode='w', 
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

current_season = os.getenv("CURRENT_SEASON")
base_url = os.getenv("MLB_API_BASE_URL")


def main():
    """
    Run the full MLB data preparation and feature engineering pipeline.

    :calls: 
        - fetchMLBTeams(base_url): Loads all MLB team metadata.
        - fetchAndUpdateOldSeason(season, base_url): Loads historical game data for past seasons.
        - fetchAndUpdateCurrentSchedule(current_season, base_url): Loads the current season's game schedule.
        - engineerFeatures(rolling_window_size, base_url): Computes and stores features using a rolling window.
    
    :param rolling_window_size: Number of games to include in rolling stats
    :param base_url: Base URL of the MLB API (from environment).
    
    :returns: None
    """

    print('here inside main')
    fetchMLBTeams(base_url)
    old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
    for season in old_seasons:
        fetchAndUpdateOldSeason(season, base_url)
    fetchAndUpdateCurrentSchedule(current_season, base_url)
    print('calling engineer feature')
    engineerFeatures(rolling_window_size=5, base_url = base_url)


if __name__ == "__main__":
    main()