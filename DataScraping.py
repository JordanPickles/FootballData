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
        match_id_list = []
        match_data_list = []
        for league in league_list:
            league_matches = self.client.league(league=league).get_match_data(season=season)
        
            for match in league_matches:
                try:
                    game_date = datetime.strptime(match['datetime'], '%Y-%m-%d %H:%M:%S')
                    today = datetime.today()
                    if game_date < today and game_date > max_date and match['goals']['h'] != None:
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
                            }
                        )
                except KeyError as e:
                    print(f"KeyError: {e} in match: {match}")
                
        df_all_match_data = pd.DataFrame(match_data_list)

        return df_all_match_data, match_id_list                

    def match_shots(self, match_id_list, season):

        match_dataframes = []
        for id in match_id_list:
            try:
                match_data = self.client.match(id['id']).get_shot_data()
                df_home = pd.DataFrame(match_data['h'])
                df_away = pd.DataFrame(match_data['a'])

                # Concatenate the home and away data for this match
                df_match = pd.concat([df_home, df_away], ignore_index=True)
                df_match['league'] = id['league']
                df_match['season'] = season
                # Collect each match's DataFrame into the list
                match_dataframes.append(df_match)
            except Exception as e:
                print(f"Error: {e} in match: {id}")


        df_all_matches = pd.concat(match_dataframes, ignore_index=True)
        
        return df_all_matches

    
    def clean_shot_data(self, df_shot_data):
 
        for col in ['X', 'Y', 'xG']:
            df_shot_data[col] = df_shot_data[col].astype(str).str.replace("'", "", regex=False)
        

        df_shot_data = df_shot_data.rename(columns={
            'id':'shot_id',
            'shotType':'shot_type',
            'lastAction':'last_action',
            'xG':'xg',
            'X':'x',
            'Y':'y',})
        
        df_shot_data['player_team'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['h_team'], df_shot_data['a_team'])
        df_shot_data['team_against'] = np.where(df_shot_data['h_a'] == 'h', df_shot_data['a_team'], df_shot_data['h_team'])
        
        df_shot_data = df_shot_data.drop(columns=['h_team', 'a_team', 'h_goals', 'a_goals'])

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
        df_shot_data['x'] = df_shot_data['x']*120
        df_shot_data['y'] = df_shot_data['y']*80

        return df_shot_data
    
    def team_data(self, teams, season):
        for team in teams:
            team_data = self.client.team(team=team).get_team_data(season=season)
            df_team_data = pd.DataFrame(team_data)
        
        return df_team_data
    
    def league_table(self, df_match_data, league_list):
        df_league_table = pd.DataFrame(columns=['team_id', 'team_name', 'points', 'game_week', 'goals_for', 'goals_against', 'goal_difference'])
        
        for league in league_list:
            df_league_data = df_match_data['league'] == league
            distinct_teams = df_match_data['home_team_id'].unique()

            for team in distinct_teams:
                home_team_matches = df_match_data[df_match_data['home_team_id'] == team]
                away_team_matches = df_match_data[df_match_data['away_team_id'] == team]
                team_matches = pd.concat([home_team_matches, away_team_matches], ignore_index=True)
                team_matches = team_matches.sort_values(by='datetime')
                total_points = 0
                game_week = 0
                goals_for = 0
                goals_against = 0
                goal_difference = 0
                team_name = team_matches['home_team_name'].iloc[0]
                team_id = team_matches['home_team_id'].iloc[0]
                
                
                for match in team_matches:
                    game_week += 1
                    if match['home_team_id'] == team:
                        
                        goals_for += match['home_team_goals']
                        goals_against += match['away_team_goals']   
                        goal_difference += goals_for - goals_against 

                        if match['home_team_goals'] > match['away_team_goals']:
                            total_points += 3
                        elif match['home_team_goals'] == match['away_team_goals']:
                            total_points += 1
                        else: total_points += 0

                    
                    else:
                        goals_for += match['away_team_goals']
                        goals_against += match['home_team_goals']
                        goal_difference += goals_for - goals_against
                        if match['away_team_goals'] > match['home_team_goals']:
                            total_points += 3
                        elif match['away_team_goals'] == match['home_team_goals']:
                            total_points += 1
                        else: total_points += 0

                    df_league_table = df_league_table.append({
                        'team_id': team_id,
                        'team_name': team_name,
                        'points': total_points,
                        'game_week': game_week,
                        'goals_for': goals_for,
                        'goals_against': goals_against,
                        'goal_difference': goal_difference
                    }, ignore_index=True)
            
        return df_league_table

if __name__ == '__main__': 
    scraper = UnderstatDataScraper()
    df_shot_data = scraper.collect_shot_data()
    df_clean_shot_data = scraper.clean_shot_data(df_shot_data)
    df_clean_shot_data.to_csv('/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/UnderstatData/shot_data_2024.csv', index=False)
    print(df_shot_data)