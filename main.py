
from src.teamsInitializer.initializeTeams import fetchMLBTeams

from src.scheduleUpdater.fetchCurrentSchedule import fetchAndUpdateCurrentSchedule
def main():
    print('here inside main')
    fetchMLBTeams()
    fetchAndUpdateCurrentSchedule()

if __name__ == "__main__":
    main()