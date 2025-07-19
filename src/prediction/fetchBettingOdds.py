from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import re

def fetchOddsFromOneGame(date):

    odds_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={date}"
    pattern = re.compile(r'^/scores/mlb-baseball/matchup/(\d+)/$')
    hrefs = set()


    # resp = requests.get(odds_url)
    # soup = BeautifulSoup(resp.text, "html.parser")
    # with open(f"sbr_odds_{date}.txt", "w", encoding="utf-8") as f:
    #     f.write(str(soup.find("div", id="leagues")))
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(odds_url)

        # Accept cookie banner by clicking the button
        try:
            page.click('#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll', timeout=5000)
            print("success")
            page.wait_for_timeout(2000) 
        except:
            print("Cookie banner not found or already accepted.")

        full_html = page.content()
        if "No odds available at this time for this league" in full_html:
            print("No odds available for this league.")
            return
        else:
            print("Odds are available.")

        leagues_element = page.query_selector('#leagues')
        leagues_html = None
        if leagues_element:
            leagues_html = leagues_element.inner_html()
        else:
            print("Error: Element with id 'leagues' not found!")
            return

        soup = BeautifulSoup(leagues_html, 'html.parser')
        for a in soup.find_all('a', href=True):
            if pattern.match(a['href']):
                hrefs.add(a['href'])
        
        print(f"Saved {len(hrefs)} game hrefs for {date}")
        print(hrefs)

        for link in hrefs:
            game_id = re.search(r"\d+", link).group()
            print(game_id)

            odds_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/line-history/{game_id}/"

            page.goto(odds_url)

            page.wait_for_timeout(5000)

            # Click the money line tab to get odds
            try:
                page.evaluate('''() => {
                    const moneyLineBtn = document.querySelector('li[data-format="money-line"]');
                    if (moneyLineBtn) {
                        moneyLineBtn.click();
                        return true;
                    }
                    return false;
                }''')
                print("Money Line clicked using JavaScript")
                page.wait_for_timeout(5000)
            except:
                print("Couldn't click on Moneyline")
                return
            
            # TODO: Switch to Fanduel from DropDown

            # TODO: fetch odds closest to tip off or opening lines 

            # TODO: once it works --> run it on all games from 2025 season and store the odds in a table somewhere in case

            break
        
        full_html = page.content()
        with open(f"sbr_odds_{date}.txt", "w", encoding="utf-8") as f:
            f.write(full_html)

def main():
    fetchOddsFromOneGame("2025-07-13")

if __name__ == "__main__":
    main()
