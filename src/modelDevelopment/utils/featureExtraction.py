import pandas as pd
import re

def buildFeatures(df_json, method = "diff"):

    # Extract features
    features_df = pd.json_normalize(df_json["features_json"])
    # Extract label
    y = features_df["label"]

    if method == "diff":
        diff_cols = []
        home_cols = [col for col in features_df.columns if re.match(r"(season|rolling)_home_avg_", col)]
        for home_col in home_cols:
            away_col = home_col.replace("home", "away")
            diff_col = home_col.replace("_home", "") + "_diff"
            features_df[diff_col] = features_df[home_col] - features_df[away_col]
            diff_cols.append(diff_col)
        
        final_features = features_df[diff_cols]

        # IF YOU WANT TO DROP IRRELEVANT FEATURES DO IT HERE
      
        columns_to_drop = [
            "season_avg_ops_diff",
            "season_avg_opponent_ops_diff",
            "rolling_avg_ops_diff",
            "rolling_avg_opponent_ops_diff"
        ]
        final_features = final_features.drop(columns=columns_to_drop)

        print(len(final_features))
        return final_features, y, list(final_features.columns)

    elif method == "raw":
        all_cols = [col for col in features_df.columns if re.match(r"(season|rolling)_(home|away)_avg_", col)]
        final_features = features_df[all_cols]

        # Drop irrelevant features
        columns_to_drop = [
            "season_home_avg_ops", "season_away_avg_ops",
            "season_home_avg_opponent_ops", "season_away_avg_opponent_ops",
            "rolling_home_avg_ops", "rolling_away_avg_ops",
            "rolling_home_avg_opponent_ops", "rolling_away_avg_opponent_ops"
        ]
        final_features = final_features.drop(columns=columns_to_drop)

        print(len(final_features))
        return final_features, y, list(final_features.columns)

    else:
        raise ValueError("method must be 'diff' or 'raw'")