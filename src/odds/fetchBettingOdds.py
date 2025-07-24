from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import logging
from datetime import datetime
import pytz
import sys


logger = logging.getLogger(__name__)

# ---------------------------------#
#      SQL AND GLOBAL STATEMENTS   #
# ---------------------------------#

ABBR_TO_TEAM_NAME = {
    'LAA': 'Los Angeles Angels',
    'SD': 'San Diego Padres',
    'SF': 'San Francisco Giants',
    'ATH': 'Athletics',
    'SEA': 'Seattle Mariners',
    'NYY': 'New York Yankees',
    'PHI': 'Philadelphia Phillies',
    'CHC': 'Chicago Cubs',
    'WAS': 'Washington Nationals',
    'MIL': 'Milwaukee Brewers',
    'TOR': 'Toronto Blue Jays',
    'CLE': 'Cleveland Guardians',
    'MIA': 'Miami Marlins',
    'ATL': 'Atlanta Braves',
    'BOS': 'Boston Red Sox',
    'TEX': 'Texas Rangers',
    'AZ': 'Arizona Diamondbacks',
    'CIN': 'Cincinnati Reds',
    'BAL': 'Baltimore Orioles',
    'MIN': 'Minnesota Twins',
    'LAD': 'Los Angeles Dodgers',
    'HOU': 'Houston Astros',
    'KC': 'Kansas City Royals',
    'STL': 'St. Louis Cardinals',
    'COL': 'Colorado Rockies',
    'PIT': 'Pittsburgh Pirates',
    'CHW': 'Chicago White Sox',
    'TB': 'Tampa Bay Rays',
    'NYM': 'New York Mets',
    'DET': 'Detroit Tigers'
}
CREATE_ODDS_TABLE = """
    CREATE TABLE IF NOT EXISTS Odds (
        game_id INTEGER PRIMARY KEY,
        home_team TEXT,
        away_team TEXT,
        home_team_odds TEXT,
        away_team_odds TEXT
    )
    """

INSERT_INTO_ODDS = """
    INSERT OR IGNORE INTO Odds (
        game_id, 
        home_team, 
        away_team, 
        home_team_odds, 
        away_team_odds 
        ) VALUES (?, ?, ?, ?, ?)
    """

SELECT_GAMEDAY_DATES_BEFORE_NOW = """
        SELECT 
            DATE(datetime(date_time, '-4 hours')) AS local_game_date
        FROM 
            CurrentSchedule
        WHERE 
            datetime(date_time, '-4 hours') < datetime('now', 'localtime')
        GROUP BY 
            local_game_date
        ORDER BY 
            local_game_date ASC;
    """

SELECT_GAME_ID_BY_DATE_AND_TEAMS = """
    SELECT game_id
    FROM CurrentSchedule
    WHERE 
        home_team = ? AND 
        away_team = ? AND 
        date_time = ?
"""

# ----------------------------- #
#     FUNCTIONS START HERE      #
# ----------------------------- #

def accept_cookies(page):
    try:
        # click on the cookie accept
        page.click('#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll', timeout=5000)
    except:
        print("Cookie banner not found or already accepted.")

def convert_api_date_to_iso(date_str):

    try:
        # Remove weekday (e.g., 'Sunday, ') if it exists
        if ',' in date_str:
            date_str = date_str.split(',', 1)[1].strip()
        
        # Strip timezone string (e.g., remove "EDT")
        date_str = re.sub(r'\s+[A-Z]{2,4}$', '', date_str).strip()

        # Parse naive datetime (no timezone info)
        dt_naive = datetime.strptime(date_str, "%B %d, %Y - %I:%M %p")

        # Localize to US Eastern time
        eastern = pytz.timezone("US/Eastern")
        dt_localized = eastern.localize(dt_naive)

        # Convert to UTC
        dt_utc = dt_localized.astimezone(pytz.utc)

        # Format to ISO string
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        print(f"Could not convert date: {date_str} — {e}")
        return None
    
def select_fanduel_sportsbook(page):
    fanduel_selected = False
    
    try:
        # Make sure dropdown is open
        dropdown_button = page.query_selector('button[data-toggle="dropdown"]')
        if dropdown_button:
            # Check if dropdown is already expanded
            is_expanded = page.get_attribute('button[data-toggle="dropdown"]', 'aria-expanded')
            if is_expanded != 'true':
                dropdown_button.click(timeout=3000)
               
                page.wait_for_timeout(1000)
        
        # Look for FanDuel logo in dropdown menu items
        dropdown_items = page.query_selector_all('ul.dropdown-menu li, ul.dropdown-menu a, ul.dropdown-menu button')
        
        for item in dropdown_items:
            # Check if this item contains FanDuel logo
            fanduel_img = item.query_selector('img[alt*="FanDuel"], img[src*="fanduel"], img[alt*="fanduel"]')
            if fanduel_img:
                item.click()
                fanduel_selected = True
                break
        
    except Exception as e:
        print(f"Couldn't select fanduel odds: {str(e)}")
    
    return fanduel_selected

def get_game_links(page, date):
    pattern = re.compile(r'^/scores/mlb-baseball/matchup/(\d+)/$')
    hrefs = set()

    leagues_element = page.query_selector('#leagues')
    if not leagues_element:
        print("Error: Element with id 'leagues' not found!")
        return hrefs

    leagues_html = leagues_element.inner_html()
    soup = BeautifulSoup(leagues_html, 'html.parser')
    for a in soup.find_all('a', href=True):
        if pattern.match(a['href']):
            hrefs.add(a['href'])

    print(f"Saved {len(hrefs)} game hrefs for {date}")
    return hrefs

def click_money_line_tab(page):
    try:
        page.evaluate('''() => {
            const moneyLineBtn = document.querySelector('li[data-format="money-line"]');
            if (moneyLineBtn) {
                moneyLineBtn.click();
                return true;
            }
            return false;
        }''')
    except:
        print("Couldn't click on Money Line tab.")

def extract_game_date(page_html):
    try:
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Look for div with id="gameDate"
        game_date_div = soup.find('div', id='gameDate')
        if game_date_div:
            date_text = game_date_div.get_text(strip=True)
            return date_text
        
        # Extract from title tag as fallback
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            # Look for date pattern in title (e.g., "Sunday, July 13, 2025 - 4:10 PM EDT")
            date_pattern = re.search(r'([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+-\s+\d{1,2}:\d{2}\s+[AP]M\s+[A-Z]{3})', title_text)
            if date_pattern:
                date_text = date_pattern.group(1)
                return date_text
        
        print("Could not find game date")
        return None
        
    except Exception as e:
        print(f"Error extracting game date: {str(e)}")
        return None

def extract_opening_odds(page_html):
    try:
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Look for "Opener" text in the HTML (using 'string' instead of deprecated 'text')
        opener_elements = soup.find_all(string=re.compile(r'opener', re.IGNORECASE))
        if not opener_elements:
            print("Could not find 'Opener' text in the page")
            return None
        
        # Find the parent container that contains the opener
        opener_section = None
        for opener_text in opener_elements:
            parent = opener_text.parent
            # Walk up the DOM to find a container that likely holds the odds table
            for _ in range(10):
                if parent and (parent.find_all('b') or len(parent.get_text().strip()) > 50):
                    opener_section = parent
                    break
                parent = parent.parent if parent else None
            if opener_section:
                break
        
        if not opener_section:
            print("Could not locate opener section")
            return None
        
        # Look for team abbreviations in bold elements
        team_pattern = re.compile(r'\b[A-Z]{2,4}\b')
        bold_elements = opener_section.find_all('b')
        
        teams = []
        for bold_elem in bold_elements:
            text = bold_elem.get_text(strip=True)
            if team_pattern.match(text) and text not in ['TIME', 'OPENER']:
                teams.append(text)
        
        if len(teams) < 2:
            print("Could not identify team abbreviations")
            return None
        
        away_team = teams[0]  # First team is typically away
        home_team = teams[1]  # Second team is typically home
        
        # Find odds patterns (+/- followed by numbers)
        odds_pattern = re.compile(r'[+-]\d+')
        section_text = opener_section.get_text()
        odds_matches = odds_pattern.findall(section_text)
        
        if len(odds_matches) < 2:
            print("Could not find sufficient odds data")
            return None
        
        # Use the first two odds found (this worked in your test)
        away_odds = odds_matches[0]
        home_odds = odds_matches[1]

        game_date = convert_api_date_to_iso(extract_game_date(page_html))
        
        opening_odds = {
            'game_date': game_date,
            'away_team': away_team,
            'away_odds': away_odds,
            'home_team': home_team,
            'home_odds': home_odds
        }
        
        return opening_odds
        
    except Exception as e:
        print(f"Error extracting opening odds: {str(e)}")
        return None

def fetchOddsFromOneGame(date):

    odds_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={date}"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(odds_url)

        accept_cookies(page)

        full_html = page.content()
        if "No odds available at this time for this league" in full_html:
            print("No odds available for this day")
            return

        hrefs = get_game_links(page, date)

        list_of_all_odds = []

        for link in hrefs:
            game_id = re.search(r"\d+", link).group()
            print(game_id)

            game_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/line-history/{game_id}/"

            page.goto(game_url)

            click_money_line_tab(page)
        
            select_fanduel_sportsbook(page)

            updated_html = page.content()

            opening_odds = extract_opening_odds(updated_html)
            list_of_all_odds.append(opening_odds)
        
        browser.close()
        return list_of_all_odds
    
def createOddsTable(cursor):
    cursor.execute(CREATE_ODDS_TABLE)

def saveOddsToDB():

    try:
        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating Odds table if it doesn't exist")
        createOddsTable(cursor)

        cursor.execute(SELECT_GAMEDAY_DATES_BEFORE_NOW)

        # Fetch all dates that games were played
        dates = cursor.fetchall()
        
        for date in dates:

            date_of_games = date[0]
            print('processing date = ' + str(date_of_games))

            # TODO: fetch games from Current Schedule from that day (make sure to convert ones from DB to EST) and get all the game_ids
            # check if ALL game_ids are already in Odds table if so then skip otherwise then fetch the odds
            

            # TODO: fixing the buggy/missing games

            all_odds = fetchOddsFromOneGame(date[0])

            for game_odds in all_odds:

                if game_odds is None:
                    print("Skipping a game with missing odds data")
                    continue

                # convert away team and home team abbreviation from Odds API to the FULL TEAM name from CurrentSchedule
                converted_away_team = ABBR_TO_TEAM_NAME.get(game_odds['away_team'])
                converted_home_team = ABBR_TO_TEAM_NAME.get(game_odds['home_team'])

                print(converted_away_team)
                print(converted_home_team)
                
                # use date and query CurrentSchedule for the corresponding game_id for that game for quick lookup 
                game_date = game_odds['game_date']

                cursor.execute(
                    SELECT_GAME_ID_BY_DATE_AND_TEAMS,
                    (converted_home_team, converted_away_team, game_date)
                )
                result = cursor.fetchone()

                if (result):
                    game_id = result[0]
                    print('HEY! We are adding this game to Odds Table!')

                    cursor.execute(
                        INSERT_INTO_ODDS,
                        (
                            game_id,
                            game_odds['home_team'],
                            game_odds['away_team'],
                            game_odds['home_odds'],
                            game_odds['away_odds']
                        )
                    )
                else:
                    print('did not find game on date with matching home and away teams')

        # commit changes to Odds DB
        conn.commit()
    except sqlite3.DatabaseError as db_err:
        logger.error(f"Database error occurred when fetching Odds: {db_err}")
        conn.rollback()  
    except Exception as e:
        logger.error(f"An error occurred when initializing fetching odds: {e}")
        conn.rollback() 
    finally:
        conn.close()

def main():
   
    sys.stdout = open('odds_scraper_output.log', 'w', encoding='utf-8')
    saveOddsToDB()

if __name__ == "__main__":
    main()
