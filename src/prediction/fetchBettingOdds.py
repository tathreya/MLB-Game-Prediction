from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import logging

logger = logging.getLogger(__name__)

# ----------------------------- #
#        SQL STATEMENTS         #
# ----------------------------- #

CREATE_ODDS_TABLE = """
    CREATE TABLE IF NOT EXISTS Odds (
        game_id INTEGER PRIMARY KEY,
        home_team TEXT,
        away_team TEXT,
        home_team_odds TEXT,
        away_team_odds TEXT,
    )
    """

INSERT_INTO_ODDS = """
    INSERT OR IGNORE INTO Odds (
        game_id 
        home_team 
        away_team 
        home_team_odds 
        away_team_odds 
        ) VALUES (?, ?, ?, ?, ?)
    """

SELECT_GAMES_BEFORE_NOW = """
        SELECT *
        FROM CurrentSchedule
        WHERE date_time < datetime('now')
        ORDER BY date_time ASC;
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
    
def select_fanduel_sportsbook(page):
    fanduel_selected = False
    
    try:
        # Make sure dropdown is open
        dropdown_button = page.query_selector('button[data-toggle="dropdown"]')
        if dropdown_button:
            # Check if dropdown is already expanded
            is_expanded = page.get_attribute('button[data-toggle="dropdown"]', 'aria-expanded')
            if is_expanded != 'true':
                dropdown_button.click()
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
        
        opening_odds = {
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

            game_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/line-history/{game_id}/"

            page.goto(game_url)

            click_money_line_tab(page)

            select_fanduel_sportsbook(page)

            updated_html = page.content()
            opening_odds = extract_opening_odds(updated_html)
            list_of_all_odds.append(opening_odds)
        
        print(list_of_all_odds)
    
def createOddsTable(cursor):
    cursor.execute(CREATE_ODDS_TABLE)

def saveOddsToDB():
    try:
        conn = sqlite3.connect("databases/MLB_Betting.db")
        cursor = conn.cursor()

        logger.debug("Creating Odds table if it doesn't exist")
        createOddsTable(cursor)

        logger.debug("Attempting to initialize MLB Teams in DB")
    except sqlite3.DatabaseError as db_err:
        logger.error(f"Database error occurred when initializing MLB Teams: {db_err}")
        conn.rollback()  
    except Exception as e:
        logger.error(f"An error occurred when initializing MLB Teams: {e}")
        conn.rollback() 
    finally:
        conn.close()


def main():
    # fetchOddsFromOneGame("2025-07-13")

if __name__ == "__main__":
    main()
