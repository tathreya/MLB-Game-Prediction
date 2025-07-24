def calculateUnitSize(model_home_confidence, model_away_confidence, home_vegas_odds, away_vegas_odds):
    """
    Given model confidence and Vegas odds, compute expected value (EV) for both teams.
    Return:
    - best_team: 'home' or 'away' if positive EV, otherwise None
    - unit_size: scaled by 5 * ROI if EV > 0
    - ROI: expected ROI from betting on the better side
    """

    # Net payout per $1
    home_payout = moneyline_to_payout(home_vegas_odds)
    away_payout = moneyline_to_payout(away_vegas_odds)

    home_ev = model_home_confidence * home_payout + model_away_confidence * -1
    away_ev = model_home_confidence * -1 + model_away_confidence * away_payout

    # if both EVs are less than 0 we don't bet on it
    if (home_ev <= 0 and away_ev <= 0):
        return None, 0, 0
    
    if home_ev > away_ev:
        roi = home_ev / home_payout
        return 'home', round(roi * 5, 3), round(roi * 100, 2)
    else:
        roi = away_ev / away_payout
        return 'away', round(roi * 5, 3), round(roi * 100, 2)

# convert moneyline to net profit per $1 bet
def moneyline_to_payout(odds):
    if odds < 0:
        return 100 / -odds
    else:
        return odds / 100

def main():
    team, unit_size, expected_roi = calculateUnitSize(0.6, 0.4, 210, -250)
    print(team)
    print(unit_size)
    print(expected_roi)
if __name__ == "__main__":
    main()



