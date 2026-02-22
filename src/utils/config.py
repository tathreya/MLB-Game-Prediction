"""
Centralized configuration for MLB AI Betting system
All season-related configuration comes from environment variables
"""

import os
from dotenv import load_dotenv

load_dotenv()

def get_current_season():
    """Get current season from environment"""
    return os.getenv("CURRENT_SEASON")

def get_old_seasons():
    """Generate old seasons list dynamically from current season"""
    current = int(get_current_season())
    # Return all seasons from 2015 up to current season - 1
    return [str(year) for year in range(2015, current)]

def get_all_seasons():
    """Generate all seasons list including current season"""
    current = int(get_current_season())
    # Return all seasons from 2015 up to current season
    return [str(year) for year in range(2015, current + 1)]

def get_mlb_api_base_url():
    """Get MLB API base URL from environment"""
    return os.getenv("MLB_API_BASE_URL", "https://statsapi.mlb.com/api/v1/")
