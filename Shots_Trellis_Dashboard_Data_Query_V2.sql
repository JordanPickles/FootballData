WITH tb1 as(
Select 
	dim_shot.match_id,
	dim_shot.shot_id, 
	dim_shot.result, 
	dim_shot.h_a, 
	dim_shot.player_team as team, 
	'For' as for_against_type,  
	dim_shot.x as x, 
	dim_shot.y as y,
	dim_shot.xg as xg
from dim_shot
where dim_shot.league in ('EPL') and dim_shot.result not in ('OwnGoal')

union all 

Select 
	dim_shot.match_id,
	dim_shot.shot_id, 
	dim_shot.result, 
	dim_shot.h_a, 
	dim_shot.team_against as team, 
	'Against' as for_against_type,
	sum(120 - dim_shot.x) as x,
	sum(80 - dim_shot.y) as y,
	sum(0 - dim_shot.xg) as xg
from dim_shot
where dim_shot.league in ('EPL') and dim_shot.result not in ('OwnGoal')
group by dim_shot.shot_id, dim_shot.result, dim_shot.xg, dim_shot.h_a, team, for_against_type

union all

Select 
	dim_shot.match_id,
	dim_shot.shot_id, 
	dim_shot.result, 
	dim_shot.h_a, 
	dim_shot.player_team as team, 
	'Against' as for_against_type,  
	dim_shot.x as x, 
	dim_shot.y as y,
	sum(0 - dim_shot.xg) as xg
from dim_shot
where dim_shot.league in ('EPL') and dim_shot.result in ('OwnGoal')
group by dim_shot.shot_id, dim_shot.result, dim_shot.xg, dim_shot.h_a, team, for_against_type
	
union all 

Select 
	dim_shot.match_id,
	dim_shot.shot_id, 
	dim_shot.result, 
	dim_shot.h_a, 
	dim_shot.team_against as team, 
	'For' as for_against_type,
	sum(120 - dim_shot.x) as x,
	sum(80 - dim_shot.y) as y,
	dim_shot.xg as xg
from dim_shot
where dim_shot.league in ('EPL') and dim_shot.result in ('OwnGoal')
group by dim_shot.shot_id, dim_shot.result, dim_shot.xg, dim_shot.h_a, team, for_against_type

),

tb2 as(
SELECT 
	team_id,
	team_name,
	MAX(points) as total_points,
	rank() Over (ORDER BY MAX(points) desc, MAX(goal_difference) desc, max(goals_for) desc) AS current_league_position,
	max(game_week) as games_played,
	max(goals_for) as current_goals_scored,
	min(goals_against) as current_goals_against
from league_table_view
where league in ('EPL')
group by team_id, team_name)

Select 
	tb1.shot_id, 
	tb1.result, 
	tb1.xg,
	tb1.h_a, 
	tb1.team, 
	tb1.for_against_type,  
	tb1.x, 
	tb1.y, 
	tb2.total_points,
	tb2.games_played,
	tb2.current_league_position,
	tb2.current_goals_scored,
	tb2.current_goals_against
from tb1
left join 
	tb2 on tb1.team = tb2.team_name;	
