"""
MLB Baseball Savant/ Fangraphs ETL DAG

@Author Jordan Wolfe
@LastUpdated 2025-04-14

Purpose of This Program:

This program is an Apache Airflow DAG that automates the process of pulling Statcast Pitch-By-Pitch Data and
also scraping batting and pitching statistics from Fangraphs for storing this data for Analysis, ML, and BI Purposes.

To Run this Program after starting the containers using the docker compose up -d command, make sure to first Follow
the steps in the Setting_Up_Slack_Connection_Airflow.pdf file for creating a slack connection for sending notifications
on the status of your DAG runs (Make sure to name the slack connection -> slack_conn).


Upon proceeding to the Airflow UI on localhost:8081 or any changed port all you would need to do is unpause the DAG named
'baseball-savant-etl-dag' to start the scheduled process. This program is set to run on cycle daily at 9:30 CST. every morning.
to have it run. The default start and end dates are for the current 2025 season as I am writing this, but feel free to
adjust the `START_DATE` and `END_DATE` in the same format for future seasons past 2025 and for past seasons as well
(Limit only 1 full season per DAG load for RAM purposes).
"""
import time
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from airflow.operators.empty import EmptyOperator
import pyodbc
from pybaseball import statcast
from pybaseball import cache
import os
import psycopg2
from airflow.operators.python_operator import PythonOperator
import numpy as np
import pendulum
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pytz
from unidecode import unidecode
import requests
from bs4 import BeautifulSoup

cache.disable()
########################################################################################################################
 # VARIABLES
########################################################################################################################
START_DATE = '2025-01-01'
#END_DATE = '2023-12-31'
END_DATE = datetime.now().strftime('%Y-%m-%d')

start_date_dt = datetime.strptime(START_DATE, '%Y-%m-%d')

end_date_dt = datetime.strptime(END_DATE, '%Y-%m-%d')

END_YEAR = end_date_dt.year

START_YEAR = start_date_dt.year

TABLE_TABLE_COLUMN_INSERT_DICT = {

    'HITTER_INFO_DIM': {
        'columns': '(HITTER_ID, HITTER_NAME)',
        'values': '(%s,%s)'

    },
    'PITCHER_INFO_DIM': {
        'columns': '(PITCHER_ID, PITCHER_NAME)',
        'values': '(%s,%s)'
    },
    'GAME_INFO_DIM': {
        'columns': '(GAME_PK, GAME_DATE, GAME_TYPE, HOME_TEAM, AWAY_TEAM, GAME_YEAR)',
        'values': '(%s, %s, %s, %s, %s, %s)'
    },
'PITCH_INFO_FACT': {
    'columns': '''(
        PITCH_ID, PITCHER_ID, BATTER_ID, HIT_ID, GAME_ID, PLAY_ID, COUNT, BASES, BASES_AFTER, RS_ON_PLAY, PITCH_TYPE, DESCRIPTION, RELEASE_SPEED, RELEASE_POS_X, RELEASE_POS_Z, ZONE, TYPE,
        PFX_X, PFX_Z, PLATE_X, PLATE_Z, VELOCITY_PITCH_FPS_X, VELOCITY_PITCH_FPS_Y, VELOCITY_PITCH_FPS_Z,
        ACCEL_PITCH_FPS_X, ACCEL_PITCH_FPS_Y, ACCEL_PITCH_FPS_Z, TOP_OF_ZONE, BOTTOM_OF_ZONE,
        RELEASE_SPIN_RATE, RELEASE_EXTENSION, RELEASE_POS_Y, PITCH_NAME, EFFECTIVE_SPEED,
        SPIN_AXIS, PITCH_NUMBER_AB, PITCHER_TEAM, HITTER_TEAM, HITTER_STAND, PITCHER_THROW
    )''',
    'values': '''(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )'''
},
'HIT_INFO_DIM': {

    'columns': '(HIT_ID, HIT_LOCATION , BB_TYPE, HC_X, HC_Y, HIT_DISTANCE, LAUNCH_SPEED, LAUNCH_ANGLE, LAUNCH_SPEED_ANGLE, ESTIMATED_BA_SPEED_ANGLE, ESTIMATED_WOBA_SPEED_ANGLE)',
    'values': '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
},

'PLAY_INFO_DIM': {
    'columns': '''(
        PLAY_ID, EVENTS, DES, ON_3B, ON_2B, ON_1B, OUTS_WHEN_UP, INNING, INNING_TOPBOT,
        FIELDER_2, FIELDER_3, FIELDER_4, FIELDER_5, FIELDER_6, FIELDER_7, FIELDER_8, FIELDER_9,
        WOBA_VALUE, WOBA_DENOM, BABIP_VALUE, ISO_VALUE, AT_BAT_NUMBER, HOME_SCORE, AWAY_SCORE,
        BAT_SCORE, FLD_SCORE, POST_HOME_SCORE, POST_AWAY_SCORE, POST_BAT_SCORE,
        IF_ALIGNMENT, OF_ALIGNMENT, DELTA_HOME_WIN_EXP, DELTA_RUN_EXP
    )''',
    'values': '''(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )'''
},


'BATTER_EXTRA_STATS': {
    
    'columns': '(HITTER_ID, GAME_DATE, IBB, RBIS, RUNS, SB, WAR)',
    'values': '(%s, %s, %s, %s, %s, %s, %s)',
    'conflict': '(HITTER_ID, GAME_DATE)',
    'conflict_updates': ['IBB', 'RBIS', 'RUNS', 'SB', 'WAR']
},


'PITCHER_EXTRA_STATS': {
    
    'columns': '(PITCHER_ID, GAME_DATE, WINS, LOSSES, IP, ER, SO, BB, HBP, IBB, SV, WAR)',
    'values': '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
    'conflict': '(PITCHER_ID, GAME_DATE)',
    'conflict_updates': ['WINS', 'LOSSES',  'IP', 'ER', 'SO', 'BB', 'HBP', 'IBB', 'SV', 'WAR' ]
},

'WOBA_CONSTANTS': {

    'columns': '(Season, wOBA, wOBAScale, wBB, wHBP, w1B, w2B, w3B, wHR, runSB, runCS, R_PA, R_W, cFIP)',
    'values': '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
    'conflict': '(Season)',
    'conflict_updates': ['wOBA', 'wOBAScale', 'wBB', 'wHBP', 'w1B', 'w2B', 'w3B', 'wHR', 'runSB', 'runCS', 'R_PA', 'R_W', 'cFIP']
}

}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 3, 21, tz="America/Chicago")
}

########################################################################################################################
# Functions for Extracting Data
########################################################################################################################
def load_fangraphs_woba_constants():
    # Step 1: Fetch the page content
    url = 'https://www.fangraphs.com/guts.aspx?type=cn'
    response = requests.get(url)

    # Step 2: Parse the page content with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Step 3: Find the table
    table = soup.find('table', {'class': 'rgMasterTable'})

    # Step 4: Extract column headers
    headers = [header.get_text() for header in table.find_all('th')]

    # Step 5: Extract rows of data
    rows = []
    for row in table.find_all('tr')[1:]:  # Skip the header row
        cells = row.find_all('td')
        row_data = [cell.get_text() for cell in cells]
        rows.append(row_data)

    # Step 6: Create a DataFrame
    df = pd.DataFrame(rows, columns=headers)
    df['Season'] = df['Season'].astype(int)

    df = df[df['Season'] >= 2015]


    print(df)

    return df

def get_batter_stats_by_game(SEASON):
    date_range = pd.date_range(start=f'{SEASON}-03-01', end=f'{SEASON}-10-01')
    full_df = pd.DataFrame()
    for date in date_range:
        
        if datetime.now().date() > date.date():
            player_id = []
            ibb = []
            rbi = []
            sb = []
            war = []
            runs = []
        
            date = date.strftime("%Y-%m-%d")
            print(date)
            url = f'https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=bat&lg=all&qual=0&season={SEASON}&season1={SEASON}&startdate={date}&enddate={date}&month=1000&pageitems=20000&ind=0&postseason='
            response = requests.get(url)

            k = response.json()
            
        
            if 'data' in k.keys():
                for row in k['data']:
                    player_id.append(row['xMLBAMID'])
                    ibb.append(row['IBB'])
                    rbi.append(row['RBI'])
                    sb.append(row['SB'])
                    war.append(row['WAR'])
                    runs.append(row['R'])
                    
                df_dict = {'player': player_id,
                        'ibb': ibb,
                        'rbi': rbi,
                        'sb': sb,
                        'war': war,
                        'runs': runs}
                
                df = pd.DataFrame(df_dict)
                df['date'] = date
                print(df)
                full_df = pd.concat([full_df, df])

    playoffs_range = pd.date_range(start=f'{SEASON}-10-01', end=f'{SEASON}-11-30')
    for date in playoffs_range:
                
        if datetime.now().date() > date.date():
            player_id = []
            ibb = []
            rbi = []
            sb = []
            war = []
            runs = []
        
            date = date.strftime("%Y-%m-%d")
            print(date)
            url = f'https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=bat&lg=all&qual=0&season={SEASON}&season1={SEASON}&startdate={date}&enddate={date}&month=1000&pageitems=20000&ind=0&postseason=1'
            response = requests.get(url)

            k = response.json()
            
        
            if 'data' in k.keys():
                for row in k['data']:
                    player_id.append(row['xMLBAMID'])
                    ibb.append(row['IBB'])
                    rbi.append(row['RBI'])
                    sb.append(row['SB'])
                    war.append(row['WAR'])
                    runs.append(row['R'])
                    
                df_dict = {'player': player_id,
                        'ibb': ibb,
                        'rbi': rbi,
                        'sb': sb,
                        'war': war,
                        'runs': runs}
                
                df = pd.DataFrame(df_dict)
                df['date'] = date
                print(df)
                full_df = pd.concat([full_df, df])
        
    return full_df

def get_pitcher_stats_by_game(SEASON):
    date_range = pd.date_range(start=f'{SEASON}-03-01', end=f'{SEASON}-10-01')
    full_df = pd.DataFrame()
    for date in date_range:
        
        if datetime.now().date() > date.date():
            player_id = []
            wins = []
            losses = []
            ip = []
            so = []
            bb = []
            hbp = []
        #    fip = []
            er = []
        #    stuff_plus = []
            sv = []
            war = []
            ibb = []
        
            date = date.strftime("%Y-%m-%d")
            print(date)
            url = f'https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=pit&lg=all&qual=0&season={SEASON}&season1={SEASON}&startdate={date}&enddate={date}&month=1000&pageitems=20000&ind=0&postseason='
            response = requests.get(url)

            k = response.json()
            
        
            if 'data' in k.keys():
                for row in k['data']:
                    player_id.append(row['xMLBAMID'])
                    wins.append(row['W'])
                    losses.append(row['L'])
                    ip.append(row['IP'])
                    so.append(row['SO'])
                    bb.append(row['BB'])
                    hbp.append(row['HBP'])
             #       fip.append(row['FIP'])
                    sv.append(row['SV'])
                    er.append(row['ER'])
                    war.append(row['WAR'])
                    ibb.append(row['IBB'])
                    
                    
                df_dict = {'player': player_id,
                           'wins': wins,
                           'losses': losses,
                           'ip': ip,
                           'so': so,
                           'bb': bb,
                           'hbp': hbp,
                           'sv': sv,
                           'er': er,
                           'war': war,
                           'ibb': ibb}
                
                df = pd.DataFrame(df_dict)
                df['date'] = date
                print(df)
                full_df = pd.concat([full_df, df])
   
   
    playoffs_range = pd.date_range(start=f'{SEASON}-10-01', end=f'{SEASON}-11-30')
    for date in playoffs_range:
        
        if datetime.now().date() > date.date():
            player_id = []
            wins = []
            losses = []
            ip = []
            so = []
            bb = []
            hbp = []
        #    fip = []
            er = []
        #    stuff_plus = []
            sv = []
            war = []
            ibb = []
        
            date = date.strftime("%Y-%m-%d")
            print(date)
            url = f'https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=pit&lg=all&qual=0&season={SEASON}&season1={SEASON}&startdate={date}&enddate={date}&month=1000&pageitems=20000&ind=0&postseason=1'
            response = requests.get(url)

            k = response.json()
            
        
            if 'data' in k.keys():
                for row in k['data']:
                    player_id.append(row['xMLBAMID'])
                    wins.append(row['W'])
                    losses.append(row['L'])
                    ip.append(row['IP'])
                    so.append(row['SO'])
                    bb.append(row['BB'])
                    hbp.append(row['HBP'])
             #       fip.append(row['FIP'])
                    sv.append(row['SV'])
                    er.append(row['ER'])
                    war.append(row['WAR'])
                    ibb.append(row['IBB'])
                    
                    
                df_dict = {'player': player_id,
                           'wins': wins,
                           'losses': losses,
                           'ip': ip,
                           'so': so,
                           'bb': bb,
                           'hbp': hbp,
                           'sv': sv,
                           'er': er,
                           'war': war,
                           'ibb': ibb}
                
                df = pd.DataFrame(df_dict)
                df['date'] = date
                print(df)
                full_df = pd.concat([full_df, df])
   
        
    return full_df


def load_statcast_data():
    # cache.enable()
    if START_YEAR == END_YEAR:
        data = statcast(START_DATE, END_DATE)
        # data = statcast('2023-03-01', '2023-09-30')
        print(data.head(10))

        return data
    else:
        print(f'Only Query 1 full Year worth of data at a time please...')
        raise
########################################################################################################################
########################################################################################################################
# Functions for Loading Current Data
########################################################################################################################
def test_postgres_connection():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )
        
        # Create a cursor and test the connection
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        
        print("Connected to PostgreSQL successfully.")
        return 'success'  # Return the connection object if successful

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

def load_all_game_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT game_pk FROM GAME_INFO_DIM;"
        game_pks = pd.read_sql(sql, conn)

        print("Game PKS Loaded")
        return game_pks  # Return the connection object if successful

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_all_hitter_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT hitter_id, hitter_name FROM HITTER_INFO_DIM;"
        hitter_pks = pd.read_sql(sql, conn)

        print("Hitter PKS Loaded")
        return hitter_pks  # Return the connection object if successful

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_all_pitcher_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT pitcher_id, pitcher_name FROM PITCHER_INFO_DIM;"
        pitcher_pks = pd.read_sql(sql, conn)

        print("Pitcher PKS Loaded")
        return pitcher_pks  # Return the connection object if successful

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_all_pitch_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT PITCH_ID FROM PITCH_INFO_FACT;"
        pitch_pks = pd.read_sql(sql, conn)

        print("Pitch PKS Loaded")
        return pitch_pks

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_all_hit_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT HIT_ID FROM HIT_INFO_DIM;"
        hit_pks = pd.read_sql(sql, conn)

        print("Hit PKS Loaded")
        return hit_pks

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_all_play_pk():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT PLAY_ID FROM PLAY_INFO_DIM;"
        play_pks = pd.read_sql(sql, conn)

        print("Play PKS Loaded")
        return play_pks

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def load_pitch_table_game_pks():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        sql = "SELECT DISTINCT PITCH_ID from PITCH_INFO_FACT;"
        fct_game_pks = pd.read_sql(sql, conn)

        print("Fact tbl game PKS Loaded")
        return fct_game_pks

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for error


def run_sql_file(file_path: str):
    """Executes an SQL file in PostgreSQL using psycopg2."""

    # Establish a connection
    conn = psycopg2.connect(
        dbname="MLB_DATA",
        user="user",
        password="password",
        host="postgres",
        port="5432"
    )

    cursor = conn.cursor()

    try:
        # Read the SQL file
        with open(file_path, "r", encoding="utf-8") as sql_file:
            sql_script = sql_file.read()

        # Execute the SQL script
        cursor.execute(sql_script)

        # Commit the transaction
        conn.commit()
        print(f"Successfully executed SQL file: {file_path}")

    except Exception as e:
        conn.rollback()  # Rollback on error
        print(f"Error executing SQL file: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


########################################################################################################################
# Functions for loading Fangraphs Stats
########################################################################################################################

def loading_other_batter_stats_non_null():
    data = get_batter_stats_by_game(START_YEAR)

    return data[['player', 'date', 'ibb', 'rbi', 'runs', 'sb', 'war']]

def loading_other_pitching_stats_non_null():
    data = get_pitcher_stats_by_game(SEASON=START_YEAR)
    return data[['player', 'date', 'wins', 'losses', 'ip', 'er', 'so', 'bb', 'hbp', 'ibb', 'sv', 'war']]

########################################################################################################################
# Data Transformation Functions
#######################################################################################################################
def remove_accent_marks(value):
    # Apply unidecode to the 'name' column
    return unidecode(value)

# Used for pitcher's name
def transform_name(name):
    last_name, first_name = map(str.strip, name.split(','))
    return f"{first_name} {last_name}"


def hit_value_column(value):
    value_dict = {
        'single': 1,
        'double': 2,
        'triple': 3,
        'home_run': 4,
        "walk": .5,
        "hit_by_pitch": .5
    }
    return value_dict.get(value, 0)

def transform_game_data(df: pd.DataFrame, games: pd.DataFrame):
    # Filter out existing games

    df = df[~df['game_pk'].isin(games['game_pk'])]

    # Filter out spring training and exhibitions
    df = df[~df['game_type'].isin(['S', 'E'])]


    game_info = df[['game_pk', 'game_date', 'game_type', 'home_team', 'away_team', 'game_year']].drop_duplicates()
   # game_info['game_date'] = game_info['game_date'].astype(str)
    return game_info

def transform_hitter_data(df: pd.DataFrame, hitters: pd.DataFrame):

    df = df[~df['game_type'].isin(['S', 'E'])]
    # Hitter Info for Dim
    hitter_info = df[~df['batter'].isin(hitters['hitter_id']) & ~df['des'].str.contains("challenge", case=False, na=False)]
    hitter_info = hitter_info[~hitter_info['batter'].isin(hitters['hitter_id']) & ~hitter_info['des'].str.contains("review", case=False, na=False)]
    hitter_info = hitter_info[~hitter_info['batter'].isin(hitters['hitter_id']) & ~hitter_info['des'].str.contains("steal", case=False, na=False)]
    hitter_info = hitter_info[~hitter_info['batter'].isin(hitters['hitter_id']) & ~hitter_info['des'].str.contains("umpire", case=False, na=False)]
    hitter_info = hitter_info[~hitter_info['batter'].isin(hitters['hitter_id']) & ~hitter_info['des'].str.contains("caught", case=False, na=False)]
    hitter_info = hitter_info[~hitter_info['batter'].isin(hitters['hitter_id']) & ~hitter_info['des'].str.contains("pickoff", case=False, na=False)]
    hitter_info['hitter_name'] = hitter_info['des'].str.split().str[:2].str.join(' ')
    hitter_info = hitter_info[['batter', 'hitter_name']].drop_duplicates()
    hitter_info.rename(columns={'batter': 'hitter_id'}, inplace=True)
    hitter_info = hitter_info.drop_duplicates(subset=['hitter_id'], keep='first')
    return hitter_info

def transform_pitcher_data(df: pd.DataFrame, pitchers: pd.DataFrame):

    df = df[~df['game_type'].isin(['S', 'E'])]
    # Pitcher Info for Dim
    pitcher_info = df[~df['pitcher'].isin(pitchers['pitcher_id']) ]
    pitcher_info = pitcher_info[['pitcher', 'player_name']].drop_duplicates()
    pitcher_info.rename(columns={'pitcher': 'pitcher_id', 'player_name': 'pitcher_name'}, inplace=True)
    return pitcher_info

def transform_pitcher_data_for_hitter_table(df: pd.DataFrame, pitchers: pd.DataFrame, hitters: pd.DataFrame, new_hitters: pd.DataFrame):
    
    df = df[~df['game_type'].isin(['S', 'E'])]
    # Pitcher Info for Dim
    pitcher_info = df[~df['pitcher'].isin(pitchers['pitcher_id']) ]
    pitcher_info = pitcher_info[['pitcher', 'player_name']].drop_duplicates()
    pitcher_info.rename(columns={'pitcher': 'pitcher_id', 'player_name': 'pitcher_name'}, inplace=True)
    
    pitcher_info_for_hitter_table = pitcher_info[~pitcher_info['pitcher_id'].isin(hitters['hitter_id']) ]
    pitcher_info_for_hitter_table = pitcher_info_for_hitter_table[~pitcher_info_for_hitter_table['pitcher_id'].isin(new_hitters['hitter_id']) ]
    
    pitcher_info_for_hitter_table.rename(columns={'pitcher_id': 'hitter_id', 'pitcher_name': 'hitter_name'})
    
    return pitcher_info_for_hitter_table

def transform_pitch_data(full_pitch_by_pitch: pd.DataFrame, pitch_df: pd.DataFrame):

    # create pitch ID
    full_pitch_by_pitch["PITCH_ID"] = (
    full_pitch_by_pitch["game_pk"].astype(str) +
    full_pitch_by_pitch["pitcher"].astype(str) +
    full_pitch_by_pitch["batter"].astype(str) +
    full_pitch_by_pitch["at_bat_number"].astype(str) +
    full_pitch_by_pitch["pitch_number"].astype(str) +
    full_pitch_by_pitch["inning"].astype(str) +
    full_pitch_by_pitch["inning_topbot"].astype(str)
    )


    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch['game_type'].isin(['S', 'E'])]


    full_pitch_by_pitch['HITTER_TEAM'] = full_pitch_by_pitch.apply(lambda x: x['away_team'] if x['inning_topbot'] == 'Top' else x['home_team'], axis=1)
    full_pitch_by_pitch['PITCHER_TEAM'] = full_pitch_by_pitch.apply(lambda x: x['home_team'] if x['inning_topbot'] == 'Top' else x['away_team'], axis=1)

    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch["PITCH_ID"].isin(pitch_df['pitch_id'])]
    full_pitch_by_pitch = full_pitch_by_pitch.drop_duplicates(subset=['PITCH_ID'], keep='first')

    full_pitch_by_pitch['COUNT'] = full_pitch_by_pitch['balls'].astype(str) + '-' + full_pitch_by_pitch['strikes'].astype(str)
    # Create BASES column
    full_pitch_by_pitch['BASES'] = full_pitch_by_pitch.apply(lambda row: f"{1 if pd.notna(row['on_1b']) else 0}-{1 if pd.notna(row['on_2b']) else 0}-{1 if pd.notna(row['on_3b']) else 0}", axis=1)

    # Create RS_ON_PLAY column
    full_pitch_by_pitch['RS_ON_PLAY'] = full_pitch_by_pitch['post_bat_score'] - full_pitch_by_pitch['bat_score']

    # create hit ID
    full_pitch_by_pitch["HIT_ID"] = np.where(
    full_pitch_by_pitch["type"] == "X",
    full_pitch_by_pitch["game_pk"].astype(str) +
    full_pitch_by_pitch["batter"].astype(str) +
    full_pitch_by_pitch["at_bat_number"].astype(str) +
    full_pitch_by_pitch["inning"].astype(str) +
    full_pitch_by_pitch["inning_topbot"].astype(str),
    np.nan
    )

    # create play ID
    full_pitch_by_pitch["PLAY_ID"] = np.where(
    full_pitch_by_pitch['events'].notna() & (full_pitch_by_pitch['events'] != ''),
    full_pitch_by_pitch["game_pk"].astype(str) +
    full_pitch_by_pitch["pitcher"].astype(str) +
    full_pitch_by_pitch["batter"].astype(str) +
    full_pitch_by_pitch["at_bat_number"].astype(str) +
    full_pitch_by_pitch["pitch_number"].astype(str) +
    full_pitch_by_pitch["inning"].astype(str) +
    full_pitch_by_pitch["inning_topbot"].astype(str),
    np.nan
    )


    full_pitch_by_pitch = full_pitch_by_pitch.sort_values(by=['game_pk', 'at_bat_number'])
    full_pitch_by_pitch['BASES_AFTER'] = full_pitch_by_pitch.groupby('game_pk')['BASES'].shift(-1)
    
    pitch_info = full_pitch_by_pitch[
    [
        "PITCH_ID",  "pitcher",  "batter","HIT_ID", "game_pk", "PLAY_ID", "COUNT", "BASES",
        "BASES_AFTER", "RS_ON_PLAY",
        "pitch_type", "description", "release_speed", "release_pos_x", "release_pos_z", "zone", "type",
        "pfx_x", "pfx_z", "plate_x", "plate_z", "vx0", "vy0", "vz0", "ax", "ay", "az", "sz_top", "sz_bot",
        "release_spin_rate", "release_extension", "release_pos_y", "pitch_name", "effective_speed",
        "spin_axis", "pitch_number", "PITCHER_TEAM", "HITTER_TEAM", 'stand', 'p_throws'
    ]
].rename(columns={
        "game_pk": "GAME_ID",
        "batter": "BATTER_ID",
        "pitcher": "PITCHER_ID",
    "pitch_type": "PITCH_TYPE",
    "description": "DESCRIPTION",
    "release_speed": "RELEASE_SPEED",
    "release_pos_x": "RELEASE_POS_X",
    "release_pos_y": "RELEASE_POS_Y",
    "release_pos_z": "RELEASE_POS_Z",
    "zone": "ZONE",
    "type": "TYPE",
    "pfx_x": "PFX_X",
    "pfx_z": "PFX_Z",
    "plate_x": "PLATE_X",
    "plate_z": "PLATE_Z",
    "vx0": "VELOCITY_PITCH_FPS_X",
    "vy0": "VELOCITY_PITCH_FPS_Y",
    "vz0": "VELOCITY_PITCH_FPS_Z",
    "ax": "ACCEL_PITCH_FPS_X",
    "ay": "ACCEL_PITCH_FPS_Y",
    "az": "ACCEL_PITCH_FPS_Z",
    "sz_top": "TOP_OF_ZONE",
    "sz_bot": "BOTTOM_OF_ZONE",
    "release_spin_rate": "RELEASE_SPIN_RATE",
    "release_extension": "RELEASE_EXTENSION",
    "pitch_name": "PITCH_NAME",
    "effective_speed": "EFFECTIVE_SPEED",
    "spin_axis": "SPIN_AXIS",
    "pitch_number": "PITCH_NUMBER_AB",
    "stand": "HITTER_STAND",
    "p_throws": "PITCHER_THROW"
})
    
    
    
    print(pitch_info.columns)
    print(pitch_info.head(10))
    print('This pitcher At bats')
    print(full_pitch_by_pitch[full_pitch_by_pitch["batter"] == 620454][['batter', 'pitcher', 'description']])
    print(len(full_pitch_by_pitch[full_pitch_by_pitch["batter"] == 620454]['des']))
    print((full_pitch_by_pitch[full_pitch_by_pitch["batter"] == 620454]['des']))
    print(len(full_pitch_by_pitch[full_pitch_by_pitch["batter"] == 620454]))
    return pitch_info
    
def transform_hit_data(full_pitch_by_pitch: pd.DataFrame, hit_df: pd.DataFrame ):

    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch['game_type'].isin(['S', 'E'])]


    full_pitch_by_pitch["HIT_ID"] = np.where(
    full_pitch_by_pitch["type"] == "X",
    full_pitch_by_pitch["game_pk"].astype(str) +
    full_pitch_by_pitch["batter"].astype(str) +
    full_pitch_by_pitch["at_bat_number"].astype(str) +
    full_pitch_by_pitch["inning"].astype(str) +
    full_pitch_by_pitch["inning_topbot"].astype(str),
    np.nan
    )

    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch["HIT_ID"].isin(hit_df['hit_id'])]

# Filter and rename columns
    hit_data = (full_pitch_by_pitch
        .dropna(subset=["HIT_ID"])
        .loc[:, ["HIT_ID", "hit_location", "bb_type", "hc_x", "hc_y", "hit_distance_sc", 
                "launch_speed", "launch_angle", "launch_speed_angle", 
                "estimated_ba_using_speedangle", "estimated_woba_using_speedangle"]]
        .rename(columns={
            "bb_type": "BB_TYPE",
            "hc_x": "HC_X",
            "hc_y": "HC_Y",
            "hit_distance_sc": "HIT_DISTANCE",
            "launch_speed": "LAUNCH_SPEED",
            "launch_angle": "LAUNCH_ANGLE",
            "launch_speed_angle": "LAUNCH_SPEED_ANGLE",
            "estimated_ba_using_speedangle": "ESTIMATED_BA_SPEED_ANGLE",
            "estimated_woba_using_speedangle": "ESTIMATED_WOBA_SPEED_ANGLE"
        })
    )

    return hit_data

def transform_play_data(full_pitch_by_pitch: pd.DataFrame, play_df: pd.DataFrame):

    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch['game_type'].isin(['S', 'E'])]

    

    full_pitch_by_pitch["PLAY_ID"] = (
    full_pitch_by_pitch["game_pk"].astype(str) +
    full_pitch_by_pitch["pitcher"].astype(str) +
    full_pitch_by_pitch["batter"].astype(str) +
    full_pitch_by_pitch["at_bat_number"].astype(str) +
    full_pitch_by_pitch["pitch_number"].astype(str) +
    full_pitch_by_pitch["inning"].astype(str) +
    full_pitch_by_pitch["inning_topbot"].astype(str)
    )

    full_pitch_by_pitch = full_pitch_by_pitch[~full_pitch_by_pitch["PLAY_ID"].isin(play_df['play_id'])]
    full_pitch_by_pitch = full_pitch_by_pitch.drop_duplicates(subset=['PLAY_ID'], keep='first')

    

    play_data = (full_pitch_by_pitch
    .dropna(subset=["events"])
    .query("events != ''")
    .loc[:, ["PLAY_ID", "events", "des", "on_3b", "on_2b", "on_1b",
             "outs_when_up", "inning", "inning_topbot", "fielder_2", "fielder_3", "fielder_4", 
             "fielder_5", "fielder_6", "fielder_7", "fielder_8", "fielder_9",
             "woba_value", "woba_denom", "babip_value", "iso_value", "at_bat_number", 
             "home_score", "away_score", "bat_score", "fld_score", "post_home_score", 
             "post_away_score", "post_bat_score", "if_fielding_alignment", "of_fielding_alignment", 
             "delta_home_win_exp", "delta_run_exp"]]
    .rename(columns={
        "if_fielding_alignment": "IF_ALIGNMENT",
        "of_fielding_alignment": "OF_ALIGNMENT"
    })

)
    return play_data
########################################################################################################################
# Loading Tables Functions
########################################################################################################################
def load_tables_many(df: pd.DataFrame, table_name):
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        cursor = conn.cursor()

        # Define the columns and values format from the dictionary
        columns = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['columns']
        values = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['values']


        #needed for pscyopg2 to insert null values
        df = df.replace({np.NaN: None})

        # Convert numpy.int64 to Python int for the dataframe values
        def convert_types(row):
            return tuple(int(value) if isinstance(value, np.int64) else value for value in row)

        # Convert the dataframe rows to a list of tuples with appropriate type handling
        data = [convert_types(x) for x in df.itertuples(index=False, name=None)]

        # SQL query with placeholders (%s)
        sql = f'INSERT INTO {table_name} {columns} VALUES {values};'

        print(sql)  # Debugging: print the query

        # Execute batch insert
        cursor.executemany(sql, data)

        # Commit and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        print('Success!!')
    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise  # raise error for further handling

def load_tables_many_on_conflict(df: pd.DataFrame, table_name):
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        cursor = conn.cursor()

        # Get column and query formatting info from your dictionary
        columns = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['columns']      # e.g., "(HITTER_ID, GAME_DATE, IBB, RBIS, RUNS, SB, WAR)"
        values = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['values']        # e.g., "(%s, %s, %s, %s, %s, %s, %s)"
        conflict = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['conflict']    # e.g., "(HITTER_ID, GAME_DATE)"
        conflict_updates = TABLE_TABLE_COLUMN_INSERT_DICT[table_name]['conflict_updates']  # e.g., ["IBB", "RBIS", "RUNS", "SB", "WAR"]

        # Replace NaNs with None for psycopg2 compatibility
        df = df.replace({np.NaN: None})

        # Convert numpy types to native Python types
        def convert_types(row):
            return tuple(int(v) if isinstance(v, np.integer) else v for v in row)

        data = [convert_types(row) for row in df.itertuples(index=False, name=None)]

        # Build the SQL insert statement
        sql = f'INSERT INTO {table_name} {columns} VALUES {values} ON CONFLICT {conflict} DO UPDATE SET '
        sql += ', '.join([f'{col} = EXCLUDED.{col}' for col in conflict_updates]) + ';'

        print(f"Prepared SQL:\n{sql}\n")

        # Execute each row individually, skipping rows that violate FK constraints
        skipped = 0
        for row in data:
            try:
                cursor.execute(sql, row)
            except psycopg2.errors.ForeignKeyViolation:
                print(f"Skipping row due to FK violation: {row}")
                conn.rollback()
                skipped += 1
                continue

        conn.commit()
        cursor.close()
        conn.close()

        print(f'Success! Inserted {len(data) - skipped} rows. Skipped {skipped} due to FK violations.')

    except psycopg2.Error as e:
        print("Error w/ PostgreSQL:", e)
        raise
########################################################################################################################

# The Dag Process that Runs in Airflow
with DAG(dag_id='baseball-savant-etl-workflow',schedule_interval="30 9 * * *", default_args=default_args, catchup=False) as dag:
    slack_success = SlackWebhookOperator(
        task_id='slack_success',
        slack_webhook_conn_id='slack_conn',
        message=":baseball: :baseball:  :baseball: :white_check_mark: DAG *{{ dag.dag_id }}* has completed successfully! :baseball: :baseball:  :baseball:  \n"
                ":identification_card: *Run ID*:  {{ run_id }}\n"
                ":hammer_and_wrench: *Run Type:* {{ dag_run.run_type }} \n"
                ":calendar: *Run Start Time:* {{ dag_run.start_date }} \n"
                f":calendar: *Run End Time:* {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S.%f%z')}\n"
                ":page_facing_up: *Task States:* \n"
                "{% for ti in dag_run.get_task_instances() %}"
                    "  - *Task:* {{ ti.task_id }} | *State:* {{ ti.state }} \n"
                "{% endfor %}",
        channel="#airflow-dag-status",
        username="Airflow-Dag-Updates",
        dag=dag,
    )

    slack_failure = SlackWebhookOperator(
        task_id='slack_failure',
        slack_webhook_conn_id='slack_conn',
        message=":baseball: :baseball: :baseball: :x: DAG *{{ dag.dag_id }}* has failed! Check the logs :baseball: :baseball: :baseball: \n"
                " :identification_card: *Run ID*: {{ run_id }}\n"
                ":hammer_and_wrench: *Run Type:* {{ dag_run.run_type }} \n"
                ":calendar: *Run Start Time:* {{ dag_run.start_date }} \n"
                f":calendar: *Run End Time:* {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S.%f%z')}\n"
                ":page_facing_up: *Task States:* \n"
                "{% for ti in dag_run.get_task_instances() %}"
                    "  - *Task:* {{ ti.task_id }} | *State:* {{ ti.state }} \n"
                "{% endfor %}",
        channel="#airflow-dag-status",
        username="Airflow-Dag-Updates",
        trigger_rule="one_failed",  # Triggers only if any previous task fails
        dag=dag,
    )

    with TaskGroup("load_all_baseball_data") as load_statcast_data_group:
        connection = PythonOperator(
            task_id='test_postgres_connection',
            python_callable=test_postgres_connection,
            dag=dag
        )
        

        execute_sql_file_for_creation = PythonOperator(
            task_id='create-sql-tables',
            python_callable=run_sql_file,
            op_args=[f'{os.getcwd()}/sql_files/schema.sql'],
            dag=dag
        )

        extract_woba_constants_task = PythonOperator(
            task_id='extract_woba_constants-task',
            python_callable=load_fangraphs_woba_constants,
            dag=dag
        )
        get_pybabseball_data = PythonOperator(
            task_id='load_statcast_data',
            python_callable=load_statcast_data,
            retries=3,
            retry_delay=timedelta(seconds=30),
            dag=dag
        )
        
        load_batting_stats_task = PythonOperator(
            task_id='load_batting_stats_non_null',
            python_callable=loading_other_batter_stats_non_null,
            dag=dag
        )
        
        
        load_pitching_stats_task = PythonOperator(
            task_id='load-pitching_stats_non_null',
            python_callable=loading_other_pitching_stats_non_null,
            dag=dag
        )
        
        connection >> execute_sql_file_for_creation >> extract_woba_constants_task  >>  get_pybabseball_data >> load_batting_stats_task  >> load_pitching_stats_task 

    with TaskGroup("Load-DB-Current-DW-Info") as get_current_dw_info:
        game_pks = PythonOperator(
            task_id='load_all_game_pk',
            python_callable=load_all_game_pk,
            dag=dag
        )
        hitters_pks = PythonOperator(
            task_id='load_all_hitter_pk',
            python_callable=load_all_hitter_pk,
            dag=dag
        )
        pitcher_pks = PythonOperator(
            task_id='load_all_pitcher_pk',
            python_callable=load_all_pitcher_pk,
            dag=dag
        )
        pitch_pks = PythonOperator(
            task_id='load_pitch_pks',
            python_callable=load_all_pitch_pk,
            dag=dag
        )

        hit_pks = PythonOperator(
            task_id='load_hit_pks',
            python_callable=load_all_hit_pk,
            dag=dag
        )

        play_pks = PythonOperator(
            task_id='load_play_pks',
            python_callable=load_all_play_pk,
            dag=dag
        )
        game_pks >> hitters_pks >> pitcher_pks >> hit_pks >> play_pks >> pitch_pks
    

    with TaskGroup("Transform-Loaded-Savant-Data") as transform_savant_data:
        transform_game_data_step = PythonOperator(
            task_id='transform_game_data',
            python_callable=transform_game_data,
            op_args=[get_pybabseball_data.output, game_pks.output],
            dag=dag
        )

        transform_hitter_data_step = PythonOperator(
            task_id='transform_hitter_data',
            python_callable=transform_hitter_data,
            op_args=[get_pybabseball_data.output,hitters_pks.output],
            dag=dag
        )

        transform_pitcher_data_step = PythonOperator(
            task_id='transform_pitcher_data',
            python_callable=transform_pitcher_data,
            op_args=[get_pybabseball_data.output, pitcher_pks.output],
            dag=dag
        )
        
        transform_pitcher_for_hitter_table_step = PythonOperator(
            task_id='transform_pitcher_data_for_hitter_table',
            python_callable=transform_pitcher_data_for_hitter_table,
            op_args=[get_pybabseball_data.output, pitcher_pks.output,hitters_pks.output, transform_hitter_data_step.output],
            dag=dag
        )
        
        transform_pitch_data_step = PythonOperator(
            task_id='transform_pitch_data',
            python_callable=transform_pitch_data,
            op_args=[get_pybabseball_data.output, pitch_pks.output ],
            dag=dag
        
        )

        transform_hit_data_step = PythonOperator(
            task_id='transform_hit_data',
            python_callable=transform_hit_data,
            op_args=[get_pybabseball_data.output, hit_pks.output]
        )
        
        transform_play_data_step = PythonOperator(
            task_id='transform_play_data',
            python_callable=transform_play_data,
            op_args=[get_pybabseball_data.output, play_pks.output],
            dag=dag
        )


        transform_game_data_step >> transform_hitter_data_step >> transform_pitcher_data_step >> transform_pitcher_for_hitter_table_step >>   transform_hit_data_step  >> transform_play_data_step >> transform_pitch_data_step
    
    with TaskGroup("Load-MLB-DW-Tables") as load_dw_tables:
        load_game_table = PythonOperator(
            task_id='load-game-table',
            python_callable=load_tables_many,
            op_args=[transform_game_data_step.output, 'GAME_INFO_DIM'],
            dag=dag
        )
        load_hitter_table = PythonOperator(
            task_id='load-hitter-table',
            python_callable=load_tables_many,
            op_args=[transform_hitter_data_step.output, 'HITTER_INFO_DIM'],
            dag=dag
        )    
        
        load_hitter_table_w_pitchers = PythonOperator(
            task_id='load-hitters-table-for-pitchers',
            python_callable=load_tables_many,
            op_args=[transform_pitcher_for_hitter_table_step.output, 'HITTER_INFO_DIM' ],
            dag=dag
        )
        load_pitcher_table = PythonOperator(
            task_id='load-pitcher-table',
            python_callable=load_tables_many,
            op_args=[transform_pitcher_data_step.output, 'PITCHER_INFO_DIM'],
            dag=dag
        )
        load_pitch_table = PythonOperator(
            task_id='load-pitch-table',
            python_callable=load_tables_many,
            op_args=[transform_pitch_data_step.output, 'PITCH_INFO_FACT'],
            dag=dag
        )
        load_hit_table = PythonOperator(
            task_id='load-hit-table',
            python_callable=load_tables_many,
            op_args=[transform_hit_data_step.output, 'HIT_INFO_DIM'],
            dag=dag
        )
        load_play_table = PythonOperator(
            task_id='load-play-table',
            python_callable=load_tables_many,
            op_args=[transform_play_data_step.output, 'PLAY_INFO_DIM'],
            dag=dag
        )
        
        load_game_table >> load_hitter_table >>  load_hitter_table_w_pitchers >> load_pitcher_table >> load_hit_table >> load_play_table >> load_pitch_table

    with TaskGroup("Load-Stats-Tables") as load_stats_stats:
        load_woba_constants_table_task = PythonOperator(
            task_id='load-woba-constants-table',
            python_callable=load_tables_many_on_conflict,
            op_args=[extract_woba_constants_task.output, 'WOBA_CONSTANTS' ]
        )
        
        load_non_null_pitchers_task = PythonOperator(
            task_id='load-pitcher-stats',
            python_callable=load_tables_many_on_conflict,
            op_args=[load_pitching_stats_task.output, 'PITCHER_EXTRA_STATS']
        )
        
        
        load_non_null_info_task = PythonOperator(
            task_id='load-batter-stats',
            python_callable=load_tables_many_on_conflict,
            op_args=[load_batting_stats_task.output,  'BATTER_EXTRA_STATS'],
            dag=dag
        )
        
        load_woba_constants_table_task >> load_non_null_info_task >> load_non_null_pitchers_task
    load_statcast_data_group >> get_current_dw_info >> transform_savant_data >> load_dw_tables >> load_stats_stats >> [slack_success, slack_failure]