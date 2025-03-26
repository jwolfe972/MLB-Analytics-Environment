
SELECT pitch.PITCH_ID, player.PITCHER_NAME, PITCH_TYPE, RELEASE_SPEED, RELEASE_SPIN_RATE,
pitch.TYPE, pitch.zone, pitch.pfx_x, pitch.pfx_z, pitch.pitch_name, pitch.pitch_name, pitch.description,
AVG(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS avg_pitch_type_speed,
MAX(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS max_pitch_type_speed
FROM PITCH_INFO_DIM pitch
LEFT JOIN FactPitchByPitchInfo fact ON fact.PITCH_ID = pitch.PITCH_ID
LEFT JOIN PITCHER_INFO_DIM player on player.PITCHER_ID = fact.PITCHER_ID
WHERE player.PITCHER_NAME LIKE '%Eovaldi%'


-- get batting avgs  and obp with most recent team (NOTE: For OBP calculation it is an estimate since the pitch data doesnt have intentional walks)
WITH batting_stats AS (
    SELECT 
        batter.HITTER_ID, 
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single') THEN 1 ELSE 0 END) AS hit_count,
        SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'double_play', 'field_error', 'fielders_choice', 'fielders_choice_out', 'field_out', 'force_out', 'grounded_into_double_play', 'strikeout','strikeout_double_play', 'triple_play'  ) THEN 1 ELSE 0 END) AS total_ab,
		SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'walk', 'hit_by_pitch' ) THEN 1 ELSE 0 END) as num_OBP,
		SUM(CASE WHEN play.events IN ('sac_fly',  'sac_fly_double_play')  THEN 1 ELSE 0 END) as SF,
		SUM(CASE WHEN play.events IN ('walk',  'hit_by_pitch')  THEN 1 ELSE 0 END) as walks
    FROM PLAY_INFO_DIM play
    LEFT JOIN PITCH_INFO_FACT fact ON fact.PLAY_ID = play.PLAY_ID
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = fact.BATTER_ID
    LEFT JOIN GAME_INFO_DIM game ON game.GAME_PK = fact.GAME_ID
    WHERE game.game_type = 'R' AND game.game_year = 2024
    GROUP BY batter.HITTER_ID
),
latest_team AS (
    SELECT
        fact.BATTER_ID,
        fact.HITTER_TEAM AS most_recent_team,
        ROW_NUMBER() OVER (PARTITION BY fact.BATTER_ID ORDER BY game.game_date DESC) AS rn
    FROM PITCH_INFO_FACT fact
	LEFT JOIN GAME_INFO_DIM game on game.GAME_PK = fact.GAME_ID
		WHERE game.game_year = 2024
)
, stats_table AS (
    SELECT 
        batter.HITTER_NAME, 
        stats.hit_count, 
        stats.total_ab, 
		stats.walks,
        latest.most_recent_team,
        ROUND((CAST(stats.hit_count AS DECIMAL) / CAST(stats.total_ab AS DECIMAL)), 3) AS BA,
		ROUND( (CAST(num_OBP AS DECIMAL) / ( stats.total_ab+ stats.walks + stats.SF ) ), 3) AS OBP
    FROM batting_stats stats
    LEFT JOIN HITTER_INFO_DIM batter ON batter.HITTER_ID = stats.HITTER_ID
    LEFT JOIN latest_team latest ON latest.BATTER_ID = stats.HITTER_ID AND latest.rn = 1
    WHERE stats.total_ab > 162 * 3.1
)
SELECT hitter_name, most_recent_team, ba, obp, hit_count, total_ab, walks
FROM stats_table
ORDER BY obp DESC;







