import pandas as pd 
import numpy as np
import understatapi
import time
import yaml
from datetime import datetime

class UnderstatDataScraper:
    def __init__(self):
        """
        Initializes the UnderstatDataScraper class by creating an Understat API client instance.
        """
        self.client = understatapi.UnderstatClient()

    def match_id_retrieval(self, league_list, season, max_date):
        """
        Retrieves match IDs and match data for specified leagues and season.
        Parameters:
            league_list (list): A list of league identifiers to retrieve match data from.
            season (int): The season year to retrieve match data for.
            max_date (datetime): The maximum date to consider for retrieving match data.
        Returns:
            tuple: A tuple containing:
                - df_all_match_data (pd.DataFrame): A DataFrame containing detailed match data.
                - match_id_list (list): A list of dictionaries with match IDs and corresponding league identifiers.
        Raises:
            KeyError: If a key is missing in the match data dictionary.
        """

        match_id_list = []
        match_data_list = []
        for league in league_list:
            league_matches = self.client.league(league=league).get_match_data(season=season)
        
            for match in league_matches:
                try:
                    game_date = datetime.strptime(match['datetime'], '%Y-%m-%d %H:%M:%S')
                    today = datetime.today()
                    if game_date < today and game_date > max_date and match['goals']['h'] != None: # Finds the games that are not already in the database to optimise the code
                        match_id_list.append({'id': match['id'], 'league': league})
                        
                        match_data_list.append({
                            'match_id': int(match['id']),
                            'datetime': datetime.strptime(match['datetime'], '%Y-%m-%d %H:%M:%S'),
                            'league': str(league),
                            'home_team_id': int(match['h']['id']),
                            'home_team_name': str(match['h']['title']),
                            'home_team_name_short': str(match['h']['short_title']),
                            'away_team_id': int(match['a']['id']),   
                            'away_team_name': str(match['a']['title']),
                            'away_team_name_short': str(match['a']['short_title']),
                            'home_team_goals': int(match['goals']['h']),
                            'away_team_goals': int(match['goals']['a']),
                            'home_team_xg': float(match['xG']['h']),
                            'away_team_xg': float(match['xG']['a']),
                            'win_forecast': float(match['forecast']['w']),
                            'draw_forecast': float(match['forecast']['d']),
                            'loss_forecast': float(match['forecast']['l']),
                            'season': int(season)
                            } #Parses through the data returned and appends the data to a list which is converted to a df later in the code
                        )
                except KeyError as e:
                    print(f"KeyError: {e} in match: {match}")
                
        df_all_match_data = pd.DataFrame(match_data_list)

        return df_all_match_data, match_id_list                

    def match_shots(self, match_id_list, season):
        """
        Retrieves and processes shot data for a list of matches in a given season.
        Parameters:
            match_id_list (list): A list of dictionaries containing match IDs and league information.
            season (str): The season for which the match data is being retrieved.
        Returns:
            pd.DataFrame: A DataFrame containing concatenated shot data for all matches, 
                          with additional columns for league and season.
        Raises:
            Exception: If an error occurs while retrieving or processing match data, 
                       it will be caught and printed, and the function will continue with the next match.
        """
        match_dataframes = []
        for id in match_id_list:
            try:
                match_data = self.client.match(id['id']).get_shot_data()
                df_home = pd.DataFrame(match_data['h'])
                df_away = pd.DataFrame(match_data['a'])

                # Concatenates the home and away data for this match
                df_match = pd.concat([df_home, df_away], ignore_index=True)
                df_match['league'] = id['league']
                df_match['season'] = season
                match_dataframes.append(df_match)
            except Exception as e:
                print(f"Error: {e} in match: {id}")


        df_all_matches = pd.concat(match_dataframes, ignore_index=True)
        
        return df_all_matches
    
    def clean_shot_data(self, df_shot_data):
        """
        This Module Cleans and processes the shot data DataFrame.
        Parameters:
            df_shot_data (pd.DataFrame): DataFrame containing raw shot data.
        Returns:
            pd.DataFrame: Cleaned and processed DataFrame with shot data.
        """
        for col in ['X', 'Y', 'xG']:
            df_shot_data[col] = df_shot_data[col].astype(str).str.replace("'", "", regex=False) #Removes any apostrophe's from the column
        
        df_shot_data = df_shot_data.rename(columns={
            'id':'shot_id',
            'shotType':'shot_type',
            'lastAction':'last_action',
            'xG':'xg',
            'X':'x',
            'Y':'y',}) # Renames columns
        
        # Finds the team of the player taking the shot
        df_shot_data['player_team'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['h_team'], df_shot_data['a_team'])
        df_shot_data['team_against'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['a_team'], df_shot_data['h_team'])
        
        df_shot_data = df_shot_data.drop(columns=['h_team', 'a_team', 'h_goals', 'a_goals']) # Drops columns not required

        df_shot_data = df_shot_data.astype({
            'shot_id': 'int',
            'minute': 'int',
            'result': 'str',
            'x': 'float',
            'y': 'float',
            'xg': 'float',
            'player': 'str',
            'h_a': 'str',
            'player_id': 'int',
            'situation': 'str',
            'season': 'int',
            'shot_type': 'str',
            'last_action': 'str',
            'player_team': 'str',
            'player_assisted': 'str',
            'date': 'datetime64[ns]',
            'league': 'str',
            'team_against': 'str',
            'match_id': 'int'
        })
        df_shot_data['x'] = df_shot_data['x']*120 #Converts data into coordinates suitable to use on pitch maps
        df_shot_data['y'] = df_shot_data['y']*80

        return df_shot_data
    
    def team_data(self, teams, season):
        """
        Retrieves and processes data for a list of teams for a given season.
        Parameters:
            teams (list): A list of team identifiers.
            season (str): The season for which to retrieve the data.
        Returns:
            pd.DataFrame: A DataFrame containing the data for the specified teams and season.
        """
        for team in teams:
            team_data = self.client.team(team=team).get_team_data(season=season)
            df_team_data = pd.DataFrame(team_data)
        
        return df_team_data
    
if __name__ == '__main__': 
    scraper = UnderstatDataScraper()
    df_shot_data = scraper.collect_shot_data()
    df_clean_shot_data = scraper.clean_shot_data(df_shot_data)
    df_clean_shot_data.to_csv('/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/UnderstatData/shot_data_2024.csv', index=False)
    print(df_shot_data)