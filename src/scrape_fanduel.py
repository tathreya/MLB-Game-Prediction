from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

def parse_mlb_odds(game, game_id):
    home = game.get("home_team")
    away = game.get("away_team")

    def extract_moneyline(team):
        if not team or "odds_raw" not in game:
            return None
            
        for o in game["odds_raw"]:
            if team in o and "Moneyline" in o:
                m = re.search(r",\s*([+-]\d+)", o)
                if m:
                    return int(m.group(1))
        return None

    return {
        "game_id": game_id,
        "game_time": game.get("game_time"),
        "game_url": game.get("game_url"),
        "home_team": home,
        "away_team": away,
        "home_odds": extract_moneyline(home),
        "away_odds": extract_moneyline(away),
    }

def scrape_fanduel_mlb_odds_sync():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
            ]
        )
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.6312.4 Safari/537.36"
        ))
        
        page.goto("https://sportsbook.fanduel.com/navigation/mlb", timeout=60000)

        try:
            page.wait_for_load_state("networkidle")
            page.wait_for_selector("div[aria-label='Expand event statistics']", timeout=20000)
        except Exception as e:
            html = page.content()
            with open("debug_fail_fanduel_mlb.html", "w", encoding="utf-8") as f:
                f.write(html)
            browser.close()
            raise RuntimeError("FanDuel MLB content may have changed or failed to load.")

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")

    raw_games = []
    for main in soup.select("main"):
        for div in main.select("div[aria-label='Expand event statistics']"):
            game_data = {}
            parent = div.find_parent("li")
            if not parent:
                continue

            team_spans = parent.select("span[aria-label]")
            team_names = [span['aria-label'].strip() for span in team_spans if span['aria-label'].strip()]
            if len(team_names) >= 2:
                game_data["away_team"] = team_names[0]
                game_data["home_team"] = team_names[1]

            odds_buttons = parent.select("div[role='button'][aria-label]")
            if odds_buttons:
                game_data["odds_raw"] = [btn["aria-label"].strip() for btn in odds_buttons]

            time_tag = parent.find("time")
            if time_tag and time_tag.has_attr("datetime"):
                game_data["game_time"] = time_tag["datetime"]

            a_tag = parent.find("a", href=True)
            if a_tag:
                game_data["game_url"] = "https://sportsbook.fanduel.com" + a_tag["href"]

            raw_games.append(game_data)

    # THIS IS THE FIX: Parse the games BEFORE returning them to Flask
    parsed_games = [parse_mlb_odds(g, i+1) for i, g in enumerate(raw_games)]
    return parsed_games

if __name__ == "__main__":
    from pprint import pprint
    games = scrape_fanduel_mlb_odds_sync()
    pprint(games)