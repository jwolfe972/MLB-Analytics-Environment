

CREATE TABLE IF NOT EXISTS PITCHER_INFO_DIM (
    PITCHER_ID INT PRIMARY KEY,
    PITCHER_NAME VARCHAR(100) NOT NULL,
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS HITTER_INFO_DIM (
    HITTER_ID INT PRIMARY KEY,
    HITTER_NAME VARCHAR(100) NOT NULL,
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE  IF NOT EXISTS HIT_INFO_DIM (
    HIT_ID VARCHAR(100) PRIMARY KEY,
    HIT_LOCATION INT,
    BB_TYPE VARCHAR(20),
    HC_X NUMERIC(5,2),
    HC_Y NUMERIC(5,2),
    HIT_DISTANCE INT,
    LAUNCH_SPEED NUMERIC(5,2),
    LAUNCH_ANGLE INT,
    LAUNCH_SPEED_ANGLE INT,
    ESTIMATED_BA_SPEED_ANGLE NUMERIC(4,3),
    ESTIMATED_WOBA_SPEED_ANGLE NUMERIC(4,3),
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS GAME_INFO_DIM (
    GAME_PK INT PRIMARY KEY,
    GAME_DATE DATE NOT NULL,
    GAME_TYPE CHAR(1) NOT NULL,
    HOME_TEAM VARCHAR(10) NOT NULL,
    AWAY_TEAM VARCHAR(10) NOT NULL,
    GAME_YEAR INT NOT NULL,
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS PLAY_INFO_DIM (
    PLAY_ID VARCHAR(200) PRIMARY KEY,
    EVENTS TEXT NOT NULL,
    DES TEXT,
    ON_3B INT,
    ON_2B INT,
    ON_1B INT,
    OUTS_WHEN_UP INT NOT NULL,
    INNING INT NOT NULL,
    INNING_TOPBOT CHAR(3) NOT NULL,
    FIELDER_2 INT NOT NULL,
    FIELDER_3 INT NOT NULL,
    FIELDER_4 INT NOT NULL,
    FIELDER_5 INT NOT NULL,
    FIELDER_6 INT NOT NULL,
    FIELDER_7 INT NOT NULL,
    FIELDER_8 INT NOT NULL,
    FIELDER_9 INT NOT NULL,
    WOBA_VALUE NUMERIC(4,3),
    WOBA_DENOM NUMERIC(4,3),
    BABIP_VALUE SMALLINT,
    ISO_VALUE SMALLINT,
    AT_BAT_NUMBER SMALLINT NOT NULL,
    HOME_SCORE SMALLINT NOT NULL,
    AWAY_SCORE SMALLINT NOT NULL,
    BAT_SCORE SMALLINT NOT NULL,
    FLD_SCORE SMALLINT NOT NULL,
    POST_HOME_SCORE SMALLINT NOT NULL,
    POST_AWAY_SCORE SMALLINT NOT NULL,
    POST_BAT_SCORE SMALLINT NOT NULL,
    IF_ALIGNMENT VARCHAR(100),
    OF_ALIGNMENT VARCHAR(100),
    DELTA_HOME_WIN_EXP NUMERIC(6,3),
    DELTA_RUN_EXP NUMERIC(6,3),
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE  IF NOT EXISTS PITCH_INFO_FACT (
    PITCH_ID VARCHAR(200) PRIMARY KEY,
    PITCHER_ID INT NOT NULL,
    BATTER_ID INT NOT NULL,
    HIT_ID VARCHAR(100),
    GAME_ID INT NOT NULL,
    PLAY_ID VARCHAR(200),
    COUNT VARCHAR(6) NOT NULL,
    BASES VARCHAR(8) NOT NULL,
    BASES_AFTER VARCHAR(8),
    RS_ON_PLAY SMALLINT NOT NULL,
    PITCH_TYPE VARCHAR(25),
    DESCRIPTION VARCHAR(200),
    RELEASE_SPEED NUMERIC(5,2),
    RELEASE_POS_X NUMERIC(4,2),
    RELEASE_POS_Z NUMERIC(4,2),
    ZONE SMALLINT,
    TYPE CHAR(1),
    PFX_X NUMERIC(4,2),
    PFX_Z NUMERIC(4,2),
    PLATE_X NUMERIC(4,2),
    PLATE_Z NUMERIC(4,2),
    VELOCITY_PITCH_FPS_X NUMERIC(5,2),
    VELOCITY_PITCH_FPS_Y NUMERIC(5,2),
    VELOCITY_PITCH_FPS_Z NUMERIC(5,2),
    ACCEL_PITCH_FPS_X NUMERIC(5,2),
    ACCEL_PITCH_FPS_Y NUMERIC(5,2),
    ACCEL_PITCH_FPS_Z NUMERIC(5,2),
    TOP_OF_ZONE NUMERIC(4,2),
    BOTTOM_OF_ZONE NUMERIC(4,2),
    RELEASE_SPIN_RATE INT,
    RELEASE_EXTENSION NUMERIC(4,2),
    RELEASE_POS_Y NUMERIC(4,2),
    PITCH_NAME VARCHAR(50),
    EFFECTIVE_SPEED NUMERIC(5,2),
    SPIN_AXIS INT,
    PITCH_NUMBER_AB INT NOT NULL,
    PITCHER_TEAM VARCHAR(25) NOT NULL,
    HITTER_TEAM VARCHAR(10) NOT NULL,
    HITTER_STAND CHAR(1) NOT NULL,
    PITCHER_THROW CHAR(1) NOT NULL,
    DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (PITCHER_ID) REFERENCES PITCHER_INFO_DIM (PITCHER_ID),
    FOREIGN KEY (BATTER_ID) REFERENCES HITTER_INFO_DIM (HITTER_ID),
    FOREIGN KEY (HIT_ID) REFERENCES HIT_INFO_DIM (HIT_ID),
    FOREIGN KEY (GAME_ID) REFERENCES GAME_INFO_DIM (GAME_PK),
    FOREIGN KEY (PLAY_ID) REFERENCES PLAY_INFO_DIM (PLAY_ID)
);

-- CREATE TABLE IF NOT EXISTS FactPitchByPitchInfo (
--     INSTANCE_ID SERIAL PRIMARY KEY,
--     PITCHER_ID INT NOT NULL,
--     BATTER_ID INT NOT NULL,
--     PITCH_ID VARCHAR(200) NOT NULL,
--     HIT_ID VARCHAR(100),
--     GAME_ID INT NOT NULL,
--     PLAY_ID VARCHAR(200),
--     COUNT VARCHAR(6) NOT NULL,
--     BASES VARCHAR(8) NOT NULL,
--     BASES_AFTER VARCHAR(8),
--     RS_ON_PLAY SMALLINT NOT NULL,
--     DATE_TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (PITCHER_ID) REFERENCES PITCHER_INFO_DIM (PITCHER_ID),
--     FOREIGN KEY (BATTER_ID) REFERENCES HITTER_INFO_DIM (HITTER_ID),
--     FOREIGN KEY (PITCH_ID) REFERENCES PITCH_INFO_DIM (PITCH_ID),
--     FOREIGN KEY (HIT_ID) REFERENCES HIT_INFO_DIM (HIT_ID),
--     FOREIGN KEY (GAME_ID) REFERENCES GAME_INFO_DIM (GAME_PK),
--     FOREIGN KEY (PLAY_ID) REFERENCES PLAY_INFO_DIM (PLAY_ID)
-- );


CREATE TABLE IF NOT EXISTS baseball_stats (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each row (auto-increment)
    Date DATE,
    Offensive_Team VARCHAR(50),
    Defensive_Team VARCHAR(50),
    Total_Games FLOAT,
    Total_Runs FLOAT,
    RBIs FLOAT,
    AVG FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    WRC_PLUS FLOAT,
    WAR FLOAT,
    K_Percentage FLOAT,
    BB_Percentage FLOAT,
    BSR FLOAT,
    Opposing_K_9 FLOAT,
    Opposing_HR_9 FLOAT,
    Opposing_BB_9 FLOAT,
    ERA FLOAT,
    Opposing_WAR FLOAT,
    AVG_5_Players FLOAT,
    OBP_5_Players FLOAT,
    SLG_5_Players FLOAT,
    WAR_5_Players FLOAT,
    WRC_PLUS_5_Players FLOAT,
    K_Percentage_5_Players FLOAT,
    BB_Percentage_5_Players FLOAT,
    Opposing_K_9_5_Players FLOAT,
    Opposing_BB_9_5_Players FLOAT,
    ERA_5_Players FLOAT,
    Opposing_WAR_5_Players FLOAT,
    AVG_Week FLOAT,
    OBP_Week FLOAT,
    SLG_Week FLOAT,
    WAR_Week FLOAT,
    WRC_PLUS_Week FLOAT,
    K_Percentage_Week FLOAT,
    BB_Percentage_Week FLOAT,
    Opposing_K_9_Week FLOAT,
    Opposing_BB_9_Week FLOAT,
    ERA_Week FLOAT,
    Opposing_WAR_Week FLOAT,
    Runs_Scored FLOAT,
    Win BOOLEAN,
    UNIQUE(Date, Offensive_Team, Defensive_Team, Total_Games)
);

CREATE TABLE IF NOT EXISTS game_predictions(

    GAME_PK INT NOT NULL PRIMARY KEY,
    TEAM_WINNER_ABBREV VARCHAR(5) NOT NULL
);


CREATE TABLE IF NOT EXISTS BATTER_EXTRA_STATS(
    HITTER_ID INT NOT NULL,
    GAME_YEAR INT NOT NULL,
    NUM_INTENT_WALKS INT NOT NULL,
    NUM_RBIS INT NOT NULL,
    NUM_RUNS INT NOT NULL,
    NUM_SB INT NOT NULL,
    WAR NUMERIC(5,2) NOT NULL,
    LAST_UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(HITTER_ID, GAME_YEAR),
    FOREIGN KEY (HITTER_ID) REFERENCES HITTER_INFO_DIM(HITTER_ID)
);

CREATE TABLE IF NOT EXISTS PITCHER_EXTRA_STATS(

    PITCHER_ID INT NOT NULL,
    GAME_YEAR INT NOT NULL,
    WINS INT NOT NULL,
    LOSSES INT NOT NULL,
    ERA NUMERIC(7,3) NOT NULL,
    IP NUMERIC(5,2) NOT NULL,
    SO INT NOT NULL,
    BB INT NOT NULL,
    K_9 NUMERIC(5,2) NOT NULL,
    WHIP  NUMERIC(7,3) NOT NULL,
    BABIP NUMERIC(4,3) NOT NULL,
    STUFF_PLUS NUMERIC(5,1),
    FIP NUMERIC(7,3) NOT NULL,
    SV INT NOT NULL,
    WAR NUMERIC(5,2) NOT NULL,
    LAST_UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(PITCHER_ID, GAME_YEAR),
    FOREIGN KEY(PITCHER_ID) REFERENCES PITCHER_INFO_DIM(PITCHER_ID)



);


CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.LAST_UPDATED = CURRENT_TIMESTAMP;  -- Update the LAST_UPDATED field to the current time
    RETURN NEW;  -- Return the modified row
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_batter_last_updated ON BATTER_EXTRA_STATS;


CREATE TRIGGER update_batter_last_updated
BEFORE UPDATE ON BATTER_EXTRA_STATS
FOR EACH ROW
EXECUTE FUNCTION update_last_updated();



DROP TRIGGER IF EXISTS update_batter_last_updated ON PITCHER_EXTRA_STATS;


CREATE TRIGGER update_batter_last_updated
BEFORE UPDATE ON PITCHER_EXTRA_STATS
FOR EACH ROW
EXECUTE FUNCTION update_last_updated();




