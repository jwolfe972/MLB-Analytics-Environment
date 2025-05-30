
SELECT pitch.PITCH_ID, player.PITCHER_NAME, PITCH_TYPE, RELEASE_SPEED, RELEASE_SPIN_RATE,
pitch.TYPE, pitch.zone, pitch.pfx_x, pitch.pfx_z, pitch.pitch_name, pitch.pitch_name, pitch.description,
AVG(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS avg_pitch_type_speed,
MAX(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS max_pitch_type_speed
FROM PITCH_INFO_DIM pitch
LEFT JOIN FactPitchByPitchInfo fact ON fact.PITCH_ID = pitch.PITCH_ID
LEFT JOIN PITCHER_INFO_DIM player on player.PITCHER_ID = fact.PITCHER_ID
WHERE player.PITCHER_NAME LIKE '%Eovaldi%'



-- get woba leaders
-- get woba leaders
-- get woba leaders
WITH other_baseball_stats as (

SELECT HITTER_ID ,SUM(IBB) AS IBB,SUM(RBIS) AS RBIS,SUM(WAR) AS WAR , MAX(GAME_DATE) AS GAME_DATE
FROM
BATTER_EXTRA_STATS
WHERE EXTRACT( YEAR  FROM GAME_DATE) = 2025 and game_date BETWEEN '2025-04-01' AND '2025-04-15'
GROUP BY HITTER_ID


),
batting_stats AS (
    SELECT
        batter.HITTER_ID,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single') THEN 1 ELSE 0 END) AS hit_count,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'double_play', 'field_error', 'fielders_choice', 'fielders_choice_out', 'field_out', 'force_out', 'grounded_into_double_play', 'strikeout', 'strikeout_double_play', 'triple_play') THEN 1 ELSE 0 END) AS total_ab,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'walk', 'hit_by_pitch') THEN 1 ELSE 0 END) AS num_OBP,
        SUM(CASE WHEN play.events IN ('sac_fly', 'sac_fly_double_play') THEN 1 ELSE 0 END) AS SF,
        SUM(CASE WHEN play.events IN ('walk', 'hit_by_pitch') THEN 1 ELSE 0 END) AS walks,
		SUM(CASE WHEN play.events IN ('walk') THEN 1 ELSE 0 END) AS only_walks,
		SUM(CASE WHEN play.events IN ('hit_by_pitch') THEN 1 ELSE 0 END) AS only_hit_by_pitch,
		SUM(CASE WHEN play.events IN ('single') THEN 1 ELSE 0 END) AS num_singles,
		SUM(CASE WHEN play.events IN ('double') THEN 1 ELSE 0 END) AS num_doubles,
		SUM(CASE WHEN play.events IN ('triple') THEN 1 ELSE 0 END) AS num_triples,
		SUM(CASE WHEN play.events IN ('home_run') THEN 1 ELSE 0 END) AS num_hrs,
		SUM(CASE WHEN play.events IS NOT NULL THEN 1 ELSE 0 END) as num_pa
    FROM PLAY_INFO_DIM play
    LEFT JOIN PITCH_INFO_FACT fact ON fact.PLAY_ID = play.PLAY_ID
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = fact.BATTER_ID
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE game.game_type = 'R' AND EXTRACT(YEAR FROM game.game_date) = 2025 AND game_date BETWEEN '2025-04-01' AND '2025-04-15'
    GROUP BY batter.HITTER_ID
),
latest_team AS (
    SELECT
        fact.BATTER_ID,
        fact.HITTER_TEAM AS most_recent_team,
        game.game_date,
        ROW_NUMBER() OVER (PARTITION BY fact.BATTER_ID ORDER BY game.game_date DESC) AS rn
    FROM PITCH_INFO_FACT fact
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE EXTRACT(YEAR FROM game.game_date) = 2025 AND  game_date BETWEEN '2025-04-01' AND '2025-04-15'
),

woba_constants as (
SELECT * FROM WOBA_CONSTANTS
WHERE SEASON = 2025

),
stats_table AS (
    SELECT
        batter.HITTER_NAME,
        stats.hit_count,
        stats.total_ab,
        stats.walks,
        latest.most_recent_team,
		stats.num_pa,
		other.IBB,
		other.RBIS,
		other.WAR,
        stats.num_HRs,
        other.GAME_DATE,
        woba.Season,
        EXTRACT(YEAR FROM latest.game_date) as tm_year,
		CASE
			WHEN stats.total_ab > 0 THEN ROUND( CAST( (1*stats.num_singles + 2*stats.num_doubles + 3*stats.num_triples + 4*num_hrs) AS DECIMAL ) / CAST(stats.total_ab AS DECIMAL)   , 4)
			ELSE NULL
		END AS SLG,
        CASE
            WHEN stats.total_ab > 0 THEN ROUND((CAST(stats.hit_count AS DECIMAL) / CAST(stats.total_ab AS DECIMAL)), 4)
            ELSE NULL
        END AS BA,
        CASE
            WHEN (stats.total_ab + stats.walks + stats.SF + other.IBB) > 0 THEN ROUND(( (CAST(num_OBP AS DECIMAL) + other.IBB ) / (stats.total_ab + stats.walks + stats.SF + other.IBB)), 4)
            ELSE NULL
        END AS OBP,
		CASE
			WHEN (stats.total_ab + stats.only_walks  + stats.SF + only_hit_by_pitch ) > 0
				THEN ROUND(((woba.WBB *  stats.only_walks) + (woba.whbp * stats.only_hit_by_pitch) + (woba.w1b * stats.num_singles) + (woba.w2b * stats.num_doubles)
				+ (woba.w3b * stats.num_triples) + (woba.whr * stats.num_hrs)
				) / (stats.total_ab + stats.only_walks  + stats.SF + only_hit_by_pitch ),3)
				else NULL
		END AS wOBA
    FROM batting_stats stats
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = stats.HITTER_ID
    LEFT JOIN latest_team latest ON latest.BATTER_ID = stats.HITTER_ID AND latest.rn = 1
	LEFT JOIN other_baseball_stats other ON other.HITTER_ID = batter.HITTER_ID
	LEFT JOIN woba_constants woba on woba.Season = EXTRACT( YEAR FROM other.GAME_DATE)
 WHERE stats.num_pa >= 40 --AND EXTRACT(YEAR FROM other.GAME_DATE) = 2025 
)



SELECT hitter_name AS "Player", most_recent_team as "Tm", (num_pa + IBB) as "PA", ba as "BA", obp as "OBP", slg as "SLG", ROUND(obp+slg, 4) AS "OPS", woba as "wOBA", hit_count as "H", num_HRs as "HRs", walks as "BB", IBB AS "IBB", RBIS AS "RBIs", WAR as "WAR", GAME_DATE -- Season, tm_year
FROM stats_table
WHERE obp IS NOT NULL AND ba is not null AND slg IS NOT NULL
ORDER BY  "wOBA" DESC;

-- get number of cycles hit in particular season
with cycle_watch as (

SELECT pitch.BATTER_ID,
pitch.GAME_ID,
SUM(CASE WHEN play.events IN ('single') THEN 1 ELSE 0 END) as num_singles,
SUM(CASE WHEN play.events IN ('double') THEN 1 ELSE 0 END) as num_doubles,
SUM(CASE WHEN play.events IN ('triple') THEN 1 ELSE 0 END) as num_triples,
SUM(CASE WHEN play.events IN ('home_run') THEN 1 ELSE 0 END) as num_HRs
FROM PITCH_INFO_FACT pitch
LEFT JOIN HITTER_INFO_DIM hit on hit.HITTER_ID = pitch.BATTER_ID
LEFT JOIN GAME_INFO_DIM game on game.GAME_PK = pitch.GAME_ID
LEFT JOIN PLAY_INFO_DIM play on play.PLAY_ID = pitch.PLAY_ID
GROUP BY pitch.BATTER_ID, pitch.GAME_ID
)
SELECT cycle.BATTER_ID, hit.HITTER_NAME, game.GAME_DATE, game.game_year
FROM cycle_watch cycle
LEFT JOIN HITTER_INFO_DIM hit on hit.HITTER_ID = cycle.BATTER_ID
LEFT JOIN GAME_INFO_DIM game on game.GAME_PK = cycle.GAME_ID
WHERE num_singles > 0 AND num_doubles > 0 AND num_triples > 0 AND num_HRs > 0
ORDER BY game.game_year DESC







with players_hits_in_games as (

SELECT pitch.batter_id, pitch.game_id, SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single') THEN 1 ELSE 0 END) as number_of_hits
FROM pitch_info_fact pitch
LEFT JOIN play_info_dim play ON pitch.play_id = play.play_id
GROUP BY pitch.batter_id, pitch.game_id
),
players_hits_and_last_game as (

SELECT players.batter_id, players.game_id, players.number_of_hits,
LAG(players.number_of_hits) OVER(PARTITION BY players.batter_id ORDER BY game.game_date ASC) AS previous_game_hits,
GAME.GAME_DATE
FROM players_hits_in_games players
LEFT JOIN game_info_dim game on game.game_pk = players.game_id
WHERE game.game_year = 2025
),

streak_identifier AS (
    SELECT batter_id, game_id, number_of_hits, previous_game_hits, game_date,
    CASE
        WHEN previous_game_hits > 0 THEN 1
        ELSE 0
    END AS hit_in_game_before,
	CASE
		WHEN number_of_hits > 0 THEN 1
		ELSE 0
	END AS hit_in_game
    FROM players_hits_and_last_game
),
streak_change as (

   SELECT batter_id, game_id, number_of_hits, previous_game_hits, game_date,hit_in_game_before, hit_in_game,
   CASE
   		WHEN hit_in_game_before != hit_in_game
		   	THEN 1
		ELSE 0
	END AS streak_change
	FROM streak_identifier

),
streak_positioner as (

 SELECT batter_id, game_id, number_of_hits, previous_game_hits, game_date,hit_in_game_before, hit_in_game,
 streak_change,
 SUM(streak_change) OVER(PARTITION BY batter_id ORDER BY game_date ASC) as streak_position_number
 FROM streak_change


),
hit_streak as (
 SELECT batter_id, game_id, number_of_hits, previous_game_hits, game_date,hit_in_game_before, hit_in_game,
 streak_change, streak_position_number, ROW_NUMBER() OVER(PARTITION BY batter_id,streak_position_number ) as max_hit_streak
	FROM streak_positioner
)


SELECT batter_id, hitters.hitter_name, MAX(max_hit_streak) as best_hit_streak
FROM hit_streak
LEFT JOIN hitter_info_dim hitters on hitters.hitter_id = hit_streak.batter_id
GROUP BY batter_id, hitters.hitter_name
ORDER BY MAX(max_hit_streak) DESC;

