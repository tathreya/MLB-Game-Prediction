# TODO:
# STEP 1: now need to get all features (games) that have occured before today by querying CurrentSchedule for game_ids of such games
# # order by ascending date time so earliest games first, then finding the corresponding features from Feature Table
# STEP 2: Iterate thru all the features in order by date_time and for each feature, join with the Odds table to get the odds
# for each game.
# STEP 3: export the current model 
# Step 4: make prediction on each game, get the prediction probabilities for home and away and then plug into the unit size function to get
# unit size prediction, if no prediciton given, skip it, if yes, then depending on the actual outcome of game, either subtract the unit size
# or add how much you won
