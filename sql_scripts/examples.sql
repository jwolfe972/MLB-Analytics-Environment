
SELECT pitch.PITCH_ID, player.PITCHER_NAME, PITCH_TYPE, RELEASE_SPEED, RELEASE_SPIN_RATE,
pitch.TYPE, pitch.zone, pitch.pfx_x, pitch.pfx_z, pitch.pitch_name, pitch.pitch_name, pitch.description,
AVG(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS avg_pitch_type_speed,
MAX(RELEASE_SPEED) OVER (PARTITION BY PITCH_TYPE) AS max_pitch_type_speed
FROM PITCH_INFO_DIM pitch
LEFT JOIN FactPitchByPitchInfo fact ON fact.PITCH_ID = pitch.PITCH_ID
LEFT JOIN PITCHER_INFO_DIM player on player.PITCHER_ID = fact.PITCHER_ID
WHERE player.PITCHER_NAME LIKE '%Eovaldi%'


-- get batting avgs
with batting_stats as (

SELECT batter.HITTER_ID, SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single') THEN 1 ELSE 0 END) AS hit_count,
SUM(CASE WHEN play.events IN ('home_run', 'triple', 'double', 'single', 'double_play', 'field_error', 'fielders_choice', 'fielders_choice_out', 'field_out', 'force_out', 'grounded_into_double_play', 'strikeout','strikeout_double_play', 'triple_play'  ) THEN 1 ELSE 0 END) AS total_ab
FROM PLAY_INFO_DIM play
LEFT JOIN FactPitchByPitchInfo fact on fact.PLAY_ID = play.PLAY_ID
LEFT JOIN HITTER_INFO_DIM batter on batter.HITTER_ID = fact.BATTER_ID
LEFT JOIN GAME_INFO_DIM game on game.GAME_PK = fact.GAME_ID
WHERE game.game_type = 'R' AND game.game_year = 2024
GROUP BY batter.HITTER_ID




)

SELECT batter.HITTER_NAME, ROUND(( CAST(stats.hit_count AS DECIMAL) /  CAST(stats.total_ab AS DECIMAL) ),3) AS BA
FROM batting_stats stats
LEFT JOIN HITTER_INFO_DIM batter on batter.HITTER_ID = stats.HITTER_ID
where stats.total_ab > 162*3.1
ORDER BY ROUND(( CAST(stats.hit_count AS DECIMAL) /  CAST(stats.total_ab AS DECIMAL) ),3) desc
