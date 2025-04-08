
SELECT pitch.PITCH_ID, player.PITCHER_NAME, PITCH_TYPE, RELEASE_SPEED, RELEASE_SPIN_RATE,
pitch.TYPE, pitch.zone, pitch.pfx_x, pitch.pfx_z, pitch.pitch_name, pitch.pitch_name, pitch.description,
AVG(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS avg_pitch_type_speed,
MAX(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS max_pitch_type_speed
FROM PITCH_INFO_DIM pitch
LEFT JOIN FactPitchByPitchInfo fact ON fact.PITCH_ID = pitch.PITCH_ID
LEFT JOIN PITCHER_INFO_DIM player on player.PITCHER_ID = fact.PITCHER_ID
WHERE player.PITCHER_NAME LIKE '%Eovaldi%'



-- get woba leaders
WITH other_baseball_stats as (

SELECT * FROM
BATTER_EXTRA_STATS
WHERE GAME_YEAR = 2025


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
    WHERE game.game_type = 'R' AND game.game_year = 2025
    GROUP BY batter.HITTER_ID
),
latest_team AS (
    SELECT
        fact.BATTER_ID,
        fact.HITTER_TEAM AS most_recent_team,
        ROW_NUMBER() OVER (PARTITION BY fact.BATTER_ID ORDER BY game.game_date DESC) AS rn
    FROM PITCH_INFO_FACT fact
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE game.game_year = 2025
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
		other.NUM_INTENT_WALKS,
		other.NUM_RBIS,
		other.WAR,
        stats.num_HRs,
		CASE
			WHEN stats.total_ab > 0 THEN ROUND( CAST( (1*stats.num_singles + 2*stats.num_doubles + 3*stats.num_triples + 4*num_hrs) AS DECIMAL ) / CAST(stats.total_ab AS DECIMAL)   , 4)
			ELSE NULL
		END AS SLG,
        CASE
            WHEN stats.total_ab > 0 THEN ROUND((CAST(stats.hit_count AS DECIMAL) / CAST(stats.total_ab AS DECIMAL)), 4)
            ELSE NULL
        END AS BA,
        CASE
            WHEN (stats.total_ab + stats.walks + stats.SF + other.NUM_INTENT_WALKS) > 0 THEN ROUND(( (CAST(num_OBP AS DECIMAL) + other.NUM_INTENT_WALKS ) / (stats.total_ab + stats.walks + stats.SF + other.NUM_INTENT_WALKS)), 4)
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
	LEFT JOIN woba_constants woba on woba.Season = other.GAME_YEAR
 WHERE stats.num_pa > 8  * 3.1 AND other.GAME_YEAR = 2025
)
SELECT hitter_name AS "Player", most_recent_team as "Tm", (num_pa + NUM_INTENT_WALKS) as "PA", ba as "BA", obp as "OBP", slg as "SLG", ROUND(obp+slg, 4) AS "OPS", woba as "wOBA", hit_count as "H", num_HRs as "HRs", walks as "BB", NUM_INTENT_WALKS AS "IBB", NUM_RBIS AS "RBIs", WAR as "WAR"
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
