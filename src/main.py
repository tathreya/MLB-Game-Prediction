
from teamsInitializer.initializeTeams import fetchMLBTeams
from scheduleUpdater.fetchCurrentSchedule import fetchAndUpdateCurrentSchedule
import logging

# Set up basic configuration
logging.basicConfig(
    filename='app.log',
    filemode='w', 
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print('here inside main')
    fetchMLBTeams()
    fetchAndUpdateCurrentSchedule()

if __name__ == "__main__":
    main()