
with tb1 as(
select 
	concat(match_id, home_team_id) as uuid,
	league,
	match_id,
	home_team_id as team_id,
	home_team_name as team_name,
	datetime
from dim_match
where league in ('EPL')

union
select 
	concat(match_id, away_team_id) as uuid,
	league,
	match_id,
	away_team_id as team_id,
	away_team_name as team_name,
	datetime
from dim_match
where league in ('EPL')),

tb2 as (
select 
	uuid,
	league,
	team_name,
	match_id,
	team_id,
	datetime,
	ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY datetime ASC) AS game_week
from tb1
order by game_week desc),

tb3 as(
select *,
	concat(match_id, home_team_id) as home_team_match_uuid,
	concat(match_id, away_team_id) as away_team_match_uuid
from dim_match),

tb4 as(
select 
	tb3.*, 
	tb21.game_week as home_team_game_week, 
	tb22.game_week as away_team_game_week,
	concat(tb3.home_team_id, tb3.league, tb21.game_week) as home_team_game_week_uuid,
	concat(tb3.away_team_id, tb3.league, tb22.game_week) as away_team_game_week_uuid,
	concat(tb3.match_id, tb3.home_team_name) as home_team_name_match_uuid,
	concat(tb3.match_id, tb3.away_team_name) as away_team_name_match_uuid
	
from tb3
	left join tb2 as tb21 on tb3.home_team_match_uuid = tb21.uuid
	left join tb2 as tb22 on tb3.away_team_match_uuid = tb22.uuid
	),
	
tb5 as (
select tb4.*,
	lt.league_position as home_team_league_position,
	lt1.league_position as away_team_league_position
from tb4
	left join league_table_view as lt on tb4.home_team_game_week_uuid = lt.uuid
	left join league_table_view as lt1 on tb4.home_team_game_week_uuid = lt.uuid)
	
	
	
Select dim_shot.*,  
	case 
		when tb5.home_team_name = dim_shot.player_team then tb5.home_team_league_position
		else tb5.away_team_league_position
	end as league_position,
	case 
		when tb5.home_team_name = dim_shot.player_team then tb5.home_team_game_week
		else tb5.away_team_game_week
	end as game_week
from dim_shot
left join 
	tb5 on dim_shot.match_id = tb5.match_id
where dim_shot.league in ('EPL');
