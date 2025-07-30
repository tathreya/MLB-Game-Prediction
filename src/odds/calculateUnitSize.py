def calculateUnitSize(model_home_confidence, model_away_confidence, home_vegas_odds, away_vegas_odds):
    """
    Given model confidence and Vegas odds, compute expected value (EV) for both teams.
    Return:
    - best_team: 'home' or 'away' if positive EV, otherwise None
    - unit_size: scaled by 5 * ROI if EV > 0
    - ROI: expected ROI from betting on the better side
    """

    # Net payout per $1
    home_payout = moneyLineToPayout(home_vegas_odds)
    away_payout = moneyLineToPayout(away_vegas_odds)

    home_ev = model_home_confidence * home_payout + model_away_confidence * -1
    away_ev = model_home_confidence * -1 + model_away_confidence * away_payout

    # if both EVs are less than 0 we don't bet on it
    if (home_ev <= 0 and away_ev <= 0):
        return None, 0, 0
    
    # returns which team to bet on, the unit size, and the expected ROI
    if home_ev > away_ev:
        roi = home_ev / home_payout
        return 'home', round(roi * 5, 3), round(roi * 100, 2)
    else:
        roi = away_ev / away_payout
        return 'away', round(roi * 5, 3), round(roi * 100, 2)

# convert moneyline to net profit per $1 bet
def moneyLineToPayout(odds):

    if isinstance(odds, str):
        odds = odds.strip()
        if odds.startswith('+'):
            # remove the '+' sign
            odds = odds[1:]  
        odds = int(odds)

    if odds < 0:
        return 100 / -odds
    else:
        return odds / 100