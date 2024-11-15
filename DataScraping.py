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

    def match_id_retrieval(self, league_list, season):
        match_id_list = []
        match_data_list = []
        for league in league_list:
            league_matches = self.client.league(league=league).get_match_data(season=season)
        
            for match in league_matches:
                try:
                    game_date = datetime.strptime(match['datetime'], '%Y-%m-%d %H:%M:%S')
                    today = datetime.today()
                    if game_date < today and match['goals']['h'] != None:
                        match_id_list.append({'id': match['id'], 'league': league})
                        
                        match_data_list.append({
                            'match_id': match['id'],
                            'datetime': match['datetime'],
                            'league': league,
                            'home_team_id': match['h']['id'],
                            'home_team_name': match['h']['title'],
                            'home_team_name_short': match['h']['short_title'],
                            'away_team_id': match['a']['id'],   
                            'away_team_name': match['a']['title'],
                            'away_team_name_short': match['a']['short_title'],
                            'home_team_goals': match['goals']['h'],
                            'away_team_goals': match['goals']['a'],
                            'home_team_xG': match['xG']['h'],
                            'away_team_xG': match['xG']['a'],
                            'win_forecast': match['forecast']['w'],
                            'draw_forecast': match['forecast']['d'],
                            'loss_forecast': match['forecast']['l']
                            }
                        )
                except KeyError as e:
                    print(f"KeyError: {e} in match: {match}")
                
        df_all_match_data = pd.DataFrame(match_data_list)

        return df_all_match_data, match_id_list                

    def match_shots(self, match_id_list):

        match_dataframes = []
        for id in match_id_list:
            try:
                match_data = self.client.match(id['id']).get_shot_data()
                df_home = pd.DataFrame(match_data['h'])
                df_away = pd.DataFrame(match_data['a'])

                # Concatenate the home and away data for this match
                df_match = pd.concat([df_home, df_away], ignore_index=True)
                df_match['league'] = id['league']
                # Collect each match's DataFrame into the list
                match_dataframes.append(df_match)
            except Exception as e:
                print(f"Error: {e} in match: {id}")


        df_all_matches = pd.concat(match_dataframes, ignore_index=True)
        
        return df_all_matches
    def collect_shot_data(self):

        league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
        season = '2024'
        match_id_list = self.match_id_retrieval(league_list, season)
        df_shot_data = self.match_shots(match_id_list)

        return df_shot_data
    
    def clean_shot_data(self, df_shot_data):
 
        for col in ['X', 'Y', 'xG']:
            df_shot_data[col] = df_shot_data[col].astype(str).str.replace("'", "", regex=False)
            df_shot_data[col] = df_shot_data[col].astype(float)
        
        df_shot_data['X'] = df_shot_data['X']*120
        df_shot_data['Y'] = df_shot_data['Y']*80

        df_shot_data = df_shot_data.rename(columns={
            'id':'shot_id',
            'shotType':'shot_type',
            'lastAction':'last_action'})
        
        df_shot_data['player_team'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['h_team'], df_shot_data['a_team'])
        df_shot_data['team_against'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['a_team'], df_shot_data['h_team'])
        
        df_shot_data = df_shot_data.drop(columns=['h_team', 'a_team', 'h_goals', 'a_goals'])

        return df_shot_data

if __name__ == '__main__': 
    scraper = UnderstatDataScraper()
    df_shot_data = scraper.collect_shot_data()
    df_clean_shot_data = scraper.clean_shot_data(df_shot_data)
    df_clean_shot_data.to_csv('/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/UnderstatData/shot_data_2024.csv', index=False)
    print(df_shot_data)