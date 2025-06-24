
from teamsInitializer.initializeTeams import fetchMLBTeams
from scheduleUpdater.fetchCurrentSchedule import fetchAndUpdateCurrentSchedule
from scheduleUpdater.fetchOldSeasons import fetchAndUpdateOldSeason
from featureEngineering.createHistoricalFeatures import engineerFeatures
import logging
import os 


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
    print('here inside main')
    fetchMLBTeams(base_url)
    old_seasons = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
    # TODO: dont havae a loop here, do that inside the fetch past schedule code
    for season in old_seasons:
        fetchAndUpdateOldSeason(season, base_url)
    fetchAndUpdateCurrentSchedule(current_season, base_url)
    engineerFeatures(rolling_window_size=5, base_url = base_url)


if __name__ == "__main__":
    main()