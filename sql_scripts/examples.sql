
SELECT pitch.PITCH_ID, player.PITCHER_NAME, PITCH_TYPE, RELEASE_SPEED, RELEASE_SPIN_RATE,
pitch.TYPE, pitch.zone, pitch.pfx_x, pitch.pfx_z, pitch.pitch_name, pitch.pitch_name, pitch.description,
AVG(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS avg_pitch_type_speed,
MAX(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS max_pitch_type_speed
FROM PITCH_INFO_DIM pitch
LEFT JOIN FactPitchByPitchInfo fact ON fact.PITCH_ID = pitch.PITCH_ID
LEFT JOIN PITCHER_INFO_DIM player on player.PITCHER_ID = fact.PITCHER_ID
WHERE player.PITCHER_NAME LIKE '%Eovaldi%'


WITH other_baseball_stats as (

SELECT * FROM
BATTER_EXTRA_STATS
WHERE GAME_YEAR = $SEASON


),
batting_stats AS (
    SELECT 
        batter.HITTER_ID, 
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single') THEN 1 ELSE 0 END) AS hit_count,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'double_play', 'field_error', 'fielders_choice', 'fielders_choice_out', 'field_out', 'force_out', 'grounded_into_double_play', 'strikeout', 'strikeout_double_play', 'triple_play') THEN 1 ELSE 0 END) AS total_ab,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'walk', 'hit_by_pitch') THEN 1 ELSE 0 END) AS num_OBP,
        SUM(CASE WHEN play.events IN ('sac_fly', 'sac_fly_double_play') THEN 1 ELSE 0 END) AS SF,
        SUM(CASE WHEN play.events IN ('walk', 'hit_by_pitch') THEN 1 ELSE 0 END) AS walks,
		SUM(CASE WHEN play.events IN ('single') THEN 1 ELSE 0 END) AS num_singles,
		SUM(CASE WHEN play.events IN ('double') THEN 1 ELSE 0 END) AS num_doubles,
		SUM(CASE WHEN play.events IN ('triple') THEN 1 ELSE 0 END) AS num_triples,
		SUM(CASE WHEN play.events IN ('home_run') THEN 1 ELSE 0 END) AS num_hrs,
		SUM(CASE WHEN play.events IS NOT NULL THEN 1 ELSE 0 END) as num_pa
    FROM PLAY_INFO_DIM play
    LEFT JOIN PITCH_INFO_FACT fact ON fact.PLAY_ID = play.PLAY_ID
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = fact.BATTER_ID
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE game.game_type = 'R' AND game.game_year = $SEASON
    GROUP BY batter.HITTER_ID
),
latest_team AS (
    SELECT
        fact.BATTER_ID,
        fact.HITTER_TEAM AS most_recent_team,
        ROW_NUMBER() OVER (PARTITION BY fact.BATTER_ID ORDER BY game.game_date DESC) AS rn
    FROM PITCH_INFO_FACT fact
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE game.game_year = $SEASON
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
        END AS OBP
    FROM batting_stats stats
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = stats.HITTER_ID
    LEFT JOIN latest_team latest ON latest.BATTER_ID = stats.HITTER_ID AND latest.rn = 1
	LEFT JOIN other_baseball_stats other ON other.HITTER_ID = batter.HITTER_ID
 WHERE stats.num_pa > $Num_Games  * 3.1 AND other.GAME_YEAR = $SEASON
)
SELECT hitter_name AS "Player", most_recent_team as "Tm", num_pa as "PA", ba as "BA", obp as "OBP", slg as "SLG", ROUND(obp+slg, 4) AS "OPS", hit_count as "H", walks as "BB", NUM_INTENT_WALKS AS "IBB", NUM_RBIS AS "RBIs", WAR as "WAR"
FROM stats_table
WHERE obp IS NOT NULL AND ba is not null AND slg IS NOT NULL
ORDER BY  "OPS" DESC;
