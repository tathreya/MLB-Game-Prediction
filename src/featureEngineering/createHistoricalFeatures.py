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