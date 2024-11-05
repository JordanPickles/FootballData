import pandas as pd 
import understatapi
import time
from datetime import datetime

class ShotDataScraper:
    def __init__(self):
        """
        Initializes the ShotDataScraper class by creating an Understat API client instance.
        """
        self.client = understatapi.UnderstatClient()

    def match_id_retrieval(self, league_list, season):
        """
        Retrieves match IDs for completed matches from the specified leagues and season.

        Parameters:
        league_list (list): A list of league names (e.g., ['EPL', 'La_Liga']) to retrieve match data from.
        season (str): The season for which to retrieve match data (e.g., '2024').

        Returns:
        list: A list of dictionaries containing match IDs and corresponding league names for all past matches.
        """
        match_id_list = []
        for league in league_list:
            league_matches = self.client.league(league=league).get_match_data(season=season)

            for match in league_matches:
                game_date = datetime.strptime(match['datetime'], '%Y-%m-%d %H:%M:%S')
                today = datetime.today()
                if game_date < today and match['goals']['h'] != None: #Accounts for future games and games that have been postponed
                    match_id_list.append({'id': match['id'], 'league': league})

        return match_id_list                

    def match_shots(self, match_id_list):
        """
        Retrieves and processes shot data for a list of match IDs.

        Parameters:
        match_id_list (list): A list of dictionaries containing match IDs and corresponding league names.

        Returns:
        DataFrame: A concatenated pandas DataFrame of all shot data for the given matches.
                   The DataFrame contains shot data for both home and away teams.
        """
        match_dataframes = []
        for id in match_id_list:
            match_data = self.client.match(id['id']).get_shot_data()
            df_home = pd.DataFrame(match_data['h'])
            df_away = pd.DataFrame(match_data['a'])

            # Concatenate the home and away data for this match
            df_match = pd.concat([df_home, df_away], ignore_index=True)
            df_match['league'] = id['league']
            # Collect each match's DataFrame into the list
            match_dataframes.append(df_match)
                
        df_all_matches = pd.concat(match_dataframes, ignore_index=True)
        return df_all_matches

    def collect_shot_data(self):
        """
        Collects and processes shot data for a predefined list of leagues and season.

        Returns:
        DataFrame: A pandas DataFrame containing shot data for all the retrieved matches.
        """
        league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
        season = '2024'
        match_id_list = self.match_id_retrieval(league_list, season)
        df_shot_data = self.match_shots(match_id_list)

        return df_shot_data
    
    def clean_shot_data(self, df_shot_data):
        """
        Cleans and converts shot data, adjusting positional values and data types to be suitable for Statsbomb pitch images.

        Parameters:
        df_shot_data (DataFrame): A pandas DataFrame containing shot data to be cleaned.

        Returns:
        DataFrame: A cleaned DataFrame with X and Y shot positions scaled to field dimensions,
                  and numerical columns converted to appropriate data types.
        """
        for col in ['X', 'Y', 'xG']:
            df_shot_data[col] = df_shot_data[col].astype(str).str.replace("'", "", regex=False)
            df_shot_data[col] = df_shot_data[col].astype(float)
        
        df_shot_data['X'] = df_shot_data['X']*120
        df_shot_data['Y'] = df_shot_data['Y']*80
        return df_shot_data

if __name__ == '__main__': 
    scraper = ShotDataScraper()
    df_shot_data = scraper.collect_shot_data()
    df_clean_shot_data = scraper.clean_shot_data(df_shot_data)
    df_clean_shot_data.to_csv('/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/UnderstatData/shot_data_2024.csv', index=False)
    print(df_shot_data)