
CREATE OR REPLACE VIEW league_table_view AS
WITH combined_table AS (
  SELECT  
    league,
	home_team_id  as team_id,
    home_team_name as team_name,
	'home' as match_type,
    datetime,
    home_team_goals as goals_for,
	sum(0 - away_team_goals) as goals_against,
	sum(home_team_goals - away_team_goals) as goal_difference,
    CASE
      WHEN home_team_goals > away_team_goals THEN 3
      WHEN home_team_goals = away_team_goals THEN 1
      ELSE 0
    END AS points
  FROM dim_match
  GROUP BY league, home_team_id, home_team_name, datetime, home_team_goals, away_team_goals

UNION ALL
SELECT  
    league,
	away_team_id as team_id,
    away_team_name as team_name,
	'away' as match_type,
    datetime,
    away_team_goals as goals_for,
	sum(0 - home_team_goals) as goals_against,
	sum(away_team_goals - home_team_goals) as goal_difference,
    CASE
      WHEN away_team_goals > home_team_goals THEN 3
      WHEN away_team_goals = home_team_goals THEN 1
      ELSE 0
    END AS points
  FROM dim_match
  GROUP BY league, away_team_id, away_team_name, datetime, home_team_goals, away_team_goals

),


game_week_table AS (
  SELECT
	league,
    team_id,
    team_name,
	match_type,
	datetime,
	points,
	goals_for,
	goals_against,
	goal_difference,
	ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY datetime ASC) AS game_week
  FROM combined_table
),

league_table as(

  SELECT
	league,
    team_id,
    team_name,
	match_type,
	game_week,
	SUM(points) OVER (PARTITION BY team_id ORDER BY game_week ASC) AS cumulative_points,
	SUM(goals_for) OVER (PARTITION BY team_id ORDER BY game_week ASC) AS cumulative_goals_for,
	SUM(goals_against) OVER (PARTITION BY team_id ORDER BY game_week ASC) AS cumulative_goals_against,
	SUM(goal_difference) OVER (PARTITION BY team_id ORDER BY game_week ASC) AS cumulative_goal_difference
	
  FROM game_week_table

)
SELECT
	CONCAT(team_id, league, game_week) as uuid,
	league,
	team_id,
	team_name,
	cumulative_points as points,
	game_week,
	ROW_NUMBER() OVER (PARTITION BY league, game_week ORDER BY cumulative_points DESC, cumulative_goal_difference DESC, cumulative_goals_for DESC, cumulative_goals_against DESC) AS league_position,
	cumulative_goal_difference as goal_difference,
	cumulative_goals_for as goals_for,
	cumulative_goals_against as goals_against
FROM league_table
ORDER BY game_week DESC, league_position ASC;
