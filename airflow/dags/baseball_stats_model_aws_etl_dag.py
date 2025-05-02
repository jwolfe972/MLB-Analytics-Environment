import time
from datetime import datetime
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
from io import StringIO

## imports specifically for fangraphs webscraper
import requests
import time
import statsapi
import pandas as pd
from bs4 import BeautifulSoup
import random
from statistics import mean
import os
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pytz

## imports specifically for MLFlow Model Genration/Logging
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LinearRegression, RidgeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier, ExtraTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.manifold import TSNE
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, accuracy_score, f1_score, recall_score, precision_score
from sklearn.preprocessing import MinMaxScaler
import mlflow
from mlflow.models import infer_signature
from sklearn.neural_network import MLPClassifier
from sklearn.metrics.pairwise import rbf_kernel
import seaborn as sns
import matplotlib.pyplot as plt
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import CreateRegisteredModelOperator
import boto3
from botocore.exceptions import ClientError
import json
import pickle
# VARIABLES
############################################################################################################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 3, 1, tz="America/Chicago"),
    'catchup': False
}

mlb_teams = {
    "Arizona Diamondbacks": {'abbr': 'ARI', 'team_num': 15},
    "Atlanta Braves": {'abbr': 'ATL', 'team_num': 16},
    "Baltimore Orioles": {'abbr': 'BAL', 'team_num': 2},
    "Boston Red Sox": {'abbr': 'BOS', 'team_num': 3},
    "Chicago White Sox": {'abbr': 'CHW', 'team_num': 4},
    "Chicago Cubs": {'abbr': 'CHC', 'team_num': 17},
    "Cincinnati Reds": {'abbr': 'CIN', 'team_num': 18},
    "Cleveland Guardians": {'abbr': 'CLE', 'team_num': 5},
    "Colorado Rockies": {'abbr': 'COL', 'team_num': 19},
    "Detroit Tigers": {'abbr': 'DET', 'team_num': 6},
    "Houston Astros": {'abbr': 'HOU', 'team_num': 21},
    "Kansas City Royals": {'abbr': 'KCR', 'team_num': 7},
    "Los Angeles Angels": {'abbr': 'LAA', 'team_num': 1},
    "Los Angeles Dodgers": {'abbr': 'LAD', 'team_num': 22},
    "Miami Marlins": {'abbr': 'MIA', 'team_num': 20},
    "Milwaukee Brewers": {'abbr': 'MIL', 'team_num': 23},
    "Minnesota Twins": {'abbr': 'MIN', 'team_num': 8},
    "New York Yankees": {'abbr': 'NYY', 'team_num': 9},
    "New York Mets": {'abbr': 'NYM', 'team_num': 25},
    "Oakland Athletics": {'abbr': 'OAK', 'team_num': 10},
    "Athletics": {'abbr': 'ATH', 'team_num': 10},
    "Philadelphia Phillies": {'abbr': 'PHI', 'team_num': 26},
    "Pittsburgh Pirates": {'abbr': 'PIT', 'team_num': 27},
    "San Diego Padres": {'abbr': 'SDP', 'team_num': 29},
    "San Francisco Giants": {'abbr': 'SFG', 'team_num': 30},
    "Seattle Mariners": {'abbr': 'SEA', 'team_num': 11},
    "St. Louis Cardinals": {'abbr': 'STL', 'team_num': 28},
    "Tampa Bay Rays": {'abbr': 'TBR', 'team_num': 12},
    "Texas Rangers": {'abbr': 'TEX', 'team_num': 13},
    "Toronto Blue Jays": {'abbr': 'TOR', 'team_num': 14},
    "Washington Nationals": {'abbr': 'WSN', 'team_num': 24},
    "Montreal Expos": {'abbr': 'MON', 'team_num': 24},
    "Cleveland Indians": {'abbr': 'CLE', 'team_num': 5},
    "Tampa Bay Devil Rays": {'abbr': 'TBD', 'team_num': 12},
    "Anaheim Angels": {'abbr': 'ANA', 'team_num': 1},
    "Florida Marlins": {'abbr': 'FLA', 'team_num': 20},
}

#Empty dictionary where statistics will be stored
stats = {
    'Date': [],
    'Offensive Team': [],
    'Defensive Team': [],
    'Total Games': [],
    'Total Runs': [],
    'RBIs': [],
    'AVG': [],
    'OBP': [],
    'SLG': [],
    'WRC+': [],
    'WAR': [],
    'K Percentage': [],
    'BB Percentage': [],
    'BSR': [],
    'Opposing K/9': [],
    'Opposing HR/9': [],
    'Opposing BB/9': [],
    'ERA': [],
    'Opposing War': [],
    'AVG/5 Players': [],
    'OBP/5 Players': [],
    'SLG/5 Players': [],
    'WAR/5 Players': [],
    'WRC+/5 Players': [],
    'K Percentage/5 Players': [],
    'BB Percentage/5 Players': [],
    'Opposing K/9/5 Players': [],
    'Opposing BB/9/5 Players': [],
    'ERA/5 Players': [],
    'Opposing WAR/5 Players': [],
    'AVG/Week': [],
    'OBP/Week': [],
    'SLG/Week': [],
    'WAR/Week': [],
    'WRC+/Week': [],
    'K Percentage/Week': [],
    'BB Percentage/Week': [],
    'Opposing K/9/Week': [],
    'Opposing BB/9/Week': [],
    'ERA/Week': [],
    'Opposing WAR/Week': [],
    'Runs Scored': [],
    'Win?': [],
}

#Copy of the original stats dictionary to reference
original_stats = stats.copy()



BUCKET = os.getenv("BUCKET")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REGION = os.getenv("REGION")
SECRET_NAME = os.getenv("SECRET_NAME")
DB_HOST=os.getenv("DB_HOST")
DB_NAME= os.getenv("DB_NAME")
DB_PORT= os.getenv("DB_PORT")


MLFLOW_CONN_ID = "mlflow_default"
EXPERIMENT_NAME = "MLB-GAME-PREDICTION"
REGISTERED_MODEL_NAME = "MLB-Prediction-Model"
############################################################################################################



def get_secret():

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=SECRET_NAME
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    dic = json.loads(secret)

    return dic




def convert_to_binary(row):
    if row['win'] ==  1:
        return True
    else:
        return False
    

def prepare_baseball_stats_df(df: pd.DataFrame):
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.rename(columns=str.lower, inplace=True)
    df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    df.rename(columns=lambda x: x.replace('/', '_'), inplace=True)
    df.rename(columns=lambda x: x.replace('+', '_plus'), inplace=True)
    df.rename(columns=lambda x: x.replace('?', ''), inplace=True)
    df['win'] = df['win'].astype(int)
    df['win'] = df.apply(convert_to_binary, axis=1)
    df = df.drop_duplicates(subset=['date', 'offensive_team', 'defensive_team', 'total_games'], keep='first')

    return df


def copy_df_to_postgres(file_name: str, table_name: str, stats_df: pd.DataFrame, secret):


    try:
        df = pd.read_csv(file_name)

    except pd.errors.EmptyDataError:
        print('CSV is empty no data to process!!')
        return None

    buffer = StringIO()

    df = prepare_baseball_stats_df(df)
    print(df.head(10))

    merge_cols = ['date', 'offensive_team', 'defensive_team', 'total_games']

    for col in merge_cols:
        df[col] = df[col].astype(str).str.strip().str.lower()
        stats_df[col] = stats_df[col].astype(str).str.strip().str.lower()


    df_filtered = df.merge(
        stats_df[merge_cols].drop_duplicates(),
        on=merge_cols,
        how='left',
        indicator=True
    )


    df_filtered = df_filtered[df_filtered['_merge'] == 'left_only'].drop(columns=['_merge'])

    print("\nFiltered DataFrame to insert (after anti-join):")
    print(df_filtered.head(10))
    print(f"\nTotal rows to insert: {len(df_filtered)}")

    if not df_filtered.empty:
        df_filtered.to_csv(buffer, index=False, header=False, na_rep='\\N')
        buffer.seek(0)
        
        columns = '('
        for col in df_filtered.columns:
            columns+= col + ','
        columns = columns[:-1]

        columns+= ')'
        print(columns)
        copy_sql = f"""
        COPY {table_name} {columns}
        FROM STDIN
        WITH CSV
        DELIMITER ','
        NULL '\\N';
        """
        
        print(df_filtered.head(10))

        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=secret['username'],
            password=secret['password'],
            host=DB_HOST,
            port=DB_PORT
        )


        cursor = conn.cursor()
        try:
            cursor.copy_expert(sql=copy_sql, file=buffer)
            conn.commit()
            print(f"Data from DataFrame copied to '{table_name}' successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error copying data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    else:
        print("No new data to insert. Skipping database insert.")


def test_postgres_connection(secret):
    print(os.getcwd())
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=secret['username'],
            password=secret['password'],
            host=DB_HOST,
            port=DB_PORT
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


def load_baseball_model_data(secret):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=secret['username'],
            password=secret['password'],
            host=DB_HOST,
            port=DB_PORT
        )

        data = pd.read_sql('SELECT DISTINCT Date, Offensive_Team, Defensive_Team, Total_Games FROM baseball_stats;', con=conn)


        print(data.head())
        print(data.describe())
        print(data.info())

        return data

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error



def scrape_f_graphs_team_data(year, df=None):

    full_df = pd.DataFrame()
    '''
    Builds a dataset that includes statistics for each team and each game for a given year. Web scrapes through FanGraphs.com to pull statistics from variouds pages
    Exports the completed statistics to a CSV file.
    
    Inputs:
    year -- Integer representing the year to pull statistics from

    Returns: None
    '''

    #Generates the regular season schedule for a given year
    schedule = [game for game in statsapi.schedule(start_date=f'{year}-01-01', end_date=f'{year}-12-31') if game['game_type'] == 'R']

    #Initialize opening day value and dates list to iterate through
    opening_day = datetime.strptime(pd.DataFrame(schedule)['game_date'][0], '%Y-%m-%d').date()
    dates = list(set(pd.DataFrame(schedule)['game_date']))
    dates.sort()
    
    for date in dates:
        #Resets stats dictionary before every iteration
        date = datetime.strptime(date, '%Y-%m-%d').date()

        #Used in debugging, can check which day causes function to crash
        print(date)

        all_dates = list(set(list(pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d'))))


        data_for_test = date.strftime('%Y-%m-%d')

        #Ensures the date is far enough past opening day to have enough statistics, and exludes statistics from today
        if date >= opening_day + timedelta(weeks=3) and date < datetime.today().date() and str(date) not in all_dates:
        #if (data_for_test) not in all_dates:
            print('not in')
            stats = {
                'Date': [],
                'Offensive Team': [],
                'Defensive Team': [],
                'Total Games': [],
                'Total Runs': [],
                'RBIs': [],
                'AVG': [],
                'OBP': [],
                'SLG': [],
                'WRC+': [],
                'WAR': [],
                'K Percentage': [],
                'BB Percentage': [],
                'BSR': [],
                'Opposing K/9': [],
                'Opposing HR/9': [],
                'Opposing BB/9': [],
                'ERA': [],
                'Opposing War': [],
                'AVG/5 Players': [],
                'OBP/5 Players': [],
                'SLG/5 Players': [],
                'WAR/5 Players': [],
                'WRC+/5 Players': [],
                'K Percentage/5 Players': [],
                'BB Percentage/5 Players': [],
                'Opposing K/9/5 Players': [],
                'Opposing BB/9/5 Players': [],
                'ERA/5 Players': [],
                'Opposing WAR/5 Players': [],
                'AVG/Week': [],
                'OBP/Week': [],
                'SLG/Week': [],
                'WAR/Week': [],
                'WRC+/Week': [],
                'K Percentage/Week': [],
                'BB Percentage/Week': [],
                'Opposing K/9/Week': [],
                'Opposing BB/9/Week': [],
                'ERA/Week': [],
                'Opposing WAR/Week': [],
                'Runs Scored': [],
                'Win?': [],
            }

            #Initializes the daily schedule of games
            daily_schedule = [game for game in statsapi.schedule(date=date) if game['game_type'] == 'R']

            #Hitting statistics for each team from opening day to date
            hitURL = f'https://www.fangraphs.com/leaders/major-league?startdate={opening_day}&enddate={date}&ind=0&qual=0&pageitems=2000000000&season1=&season=&type=8&pos=all&stats=bat&team=0,ts&month=1000'
            hit_page = requests.get(hitURL)
            hit_soup = BeautifulSoup(hit_page.content, "html.parser")
            hit_table = hit_soup.find('div', class_='table-scroll')
            hit_rows = hit_table.find('tbody').find_all('tr')

            #Pitching statistics for each team from opening day to date
            pitchURL = f'https://www.fangraphs.com/leaders/major-league?startdate={opening_day}&enddate={date}&ind=0&qual=0&pageitems=2000000000&season1=&season=&type=8&pos=all&stats=pit&team=0,ts&month=1000'
            pitch_page = requests.get(pitchURL)
            pitch_soup = BeautifulSoup(pitch_page.content, "html.parser")
            pitch_table = pitch_soup.find('div', class_='table-scroll')
            pitch_rows = pitch_table.find('tbody').find_all('tr')

            for game in daily_schedule:
                #Pitching List Structure:
                #[index, team, tg, w, l, sv, g, gs, ip, k/9, bb/9, hr/9, babip, lob%, era, fip, war]
                #['26', 'HOU', '14', '6', '8', '3', '50', '14', '122.0', '6.86', '3.47', '1.77', '.315', '71.7%', '5.61', '5.47', '0.3']

                #Hitting List Structure:
                #[index, team, tg, g, pa, hr, r, rbi, sb, bb%, k%, iso, babip, avg, obp, slg, woba, wrc+, bsr, off, def, war]
                #['27', 'HOU', '14', '163', '531', '19', '77', '73', '8', '10.9%', '18.5%', '.183', '.257', '.236', '.331', '.419', '.330', '87', '-1.6', '-11.6', '0.0', '0.6']
                if game['home_name'] in mlb_teams and game['away_name'] in mlb_teams and game['home_score'] != game['away_score']:
                    
                    #Initializes necessary variables
                    home_team, home_runs, home_num = mlb_teams[game['home_name']]['abbr'], game['home_score'], mlb_teams[game['home_name']]['team_num']
                    away_team, away_runs, away_num = mlb_teams[game['away_name']]['abbr'], game['away_score'],  mlb_teams[game['away_name']]['team_num']

                    #Locates hitting statistics for each team
                    for row in hit_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_hit = [cell.text.strip() for cell in row.find_all('td')]

                        if team_cell and team_cell.text.strip() == away_team:
                            away_hit = [cell.text.strip() for cell in row.find_all('td')]
                    
                    #Locates pitching statistics for each team
                    for row in pitch_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_pitch = [cell.text.strip() for cell in row.find_all('td')]

                        if team_cell and team_cell.text.strip() == away_team:
                            away_pitch = [cell.text.strip() for cell in row.find_all('td')]

                    #Add all data to the stats dictionary
                    stats['Date'].append(date)
                    stats['Date'].append(date)
                    stats['Offensive Team'].append(home_team)
                    stats['Offensive Team'].append(away_team)
                    stats['Defensive Team'].append(away_team)
                    stats['Defensive Team'].append(home_team)
                    stats['Total Games'].append(int(home_hit[2]))
                    stats['Total Games'].append(int(away_hit[2]))
                    stats['Total Runs'].append(int(home_hit[6]))
                    stats['Total Runs'].append(int(away_hit[6]))
                    stats['RBIs'].append(int(home_hit[7]))
                    stats['RBIs'].append(int(away_hit[7]))
                    stats['AVG'].append(float(home_hit[15]))
                    stats['AVG'].append(float(away_hit[15]))
                    stats['OBP'].append(float(home_hit[16]))
                    stats['OBP'].append(float(away_hit[16]))
                    stats['SLG'].append(float(home_hit[17]))
                    stats['SLG'].append(float(away_hit[17]))
                    stats['WRC+'].append(float(home_hit[20]))
                    stats['WRC+'].append(float(away_hit[20]))
                    stats['WAR'].append(float(home_hit[26]))
                    stats['WAR'].append(float(away_hit[26]))
                    stats['K Percentage'].append(float(home_hit[11][:len(home_hit[11])-1])/100)
                    stats['K Percentage'].append(float(away_hit[11][:len(away_hit[11])-1])/100)
                    stats['BB Percentage'].append(float(home_hit[10][:len(home_hit[10])-1])/100)
                    stats['BB Percentage'].append(float(away_hit[10][:len(away_hit[10])-1])/100)
                    stats['BSR'].append(float(home_hit[22]))
                    stats['BSR'].append(float(away_hit[22]))
                    stats['Opposing K/9'].append(float(away_pitch[10]))
                    stats['Opposing K/9'].append(float(home_pitch[10]))
                    stats['Opposing BB/9'].append(float(away_pitch[11]))
                    stats['Opposing BB/9'].append(float(home_pitch[11]))
                    stats['Opposing HR/9'].append(float(away_pitch[12]))
                    stats['Opposing HR/9'].append(float(home_pitch[12]))
                    stats['ERA'].append(float(away_pitch[20]))
                    stats['ERA'].append(float(home_pitch[20]))
                    stats['Opposing War'].append(float(away_pitch[25]))
                    stats['Opposing War'].append(float(home_pitch[25]))
                    stats['Runs Scored'].append(home_runs)
                    stats['Runs Scored'].append(away_runs)
                    stats['Win?'].append(home_runs > away_runs)
                    stats['Win?'].append(home_runs < away_runs)
                    
                    #Takes breaks in between iterations to ensure website doesn't get overloaded
                    #time.sleep(random.uniform(3, 5))

                    #Finds the date a week prior to date
                    game_7 = date - timedelta(days=7)

                    #Hitting statistics for the home team from game_7 to date
                    hit7 = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=bat&team={home_num}&season1=&season=&month=1000&sortcol=3&sortdir=default&pagenum=1'
                    hit7_page = requests.get(hit7)
                    hit7_soup = BeautifulSoup(hit7_page.content, "html.parser")
                    hit7_table = hit7_soup.find('div', class_='table-scroll')
                    hit7_rows = hit7_table.find('tbody').find_all('tr')
                    
                    #Pitching statistics for the away team from game_7 to date
                    pitch7 = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=pit&team={away_num}&season1=&season=&month=1000&sortcol=7&sortdir=default&pagenum=1'
                    pitch7_page = requests.get(pitch7)
                    pitch7_soup = BeautifulSoup(pitch7_page.content, "html.parser")
                    pitch7_table = pitch7_soup.find('div', class_='table-scroll')
                    pitch7_rows = pitch7_table.find('tbody').find_all('tr')
                    
                    #Initialize lists to hold hitting and pitching statistics for key players
                    hit_stats = []
                    pitch_stats = []
                    
                    #Finds each player that plays for the home team in the hitting statistics
                    for row in hit7_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_hit = [cell.text.strip() for cell in row.find_all('td')]
                            hit_stats.append(home_hit)
                    
                    #Finds each player that plays for the away team in the pitching statistics
                    for row in pitch7_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == away_team:
                            away_pitch = [cell.text.strip() for cell in row.find_all('td')]
                            pitch_stats.append(away_pitch)

                    #Shortens the hitting and pitching lists to the top 5 most active players
                    hit_stats = hit_stats[:5]
                    pitch_stats = pitch_stats[:5]

                    #Calculates averages for the top five players in each category and adds them to the stats dictionary
                    avg_5 = mean([float(num[15]) for num in hit_stats])
                    obp_5 = mean([float(num[16]) for num in hit_stats])
                    slg_5 = mean([float(num[17]) for num in hit_stats])
                    war_5 = mean([float(num[26]) for num in hit_stats])
                    wrc_5 = mean([float(num[20]) for num in hit_stats])
                    k_5 = mean([float(num[11][:len(num[11])-1])/100 for num in hit_stats])
                    bb_5 = mean([float(num[10][:len(num[10])-1])/100 for num in hit_stats])
                    ok_5 = mean([float(num[10]) for num in pitch_stats])
                    obb_5 = mean([float(num[11]) for num in pitch_stats])
                    era_5 = mean([float(num[20]) for num in pitch_stats])
                    owar_5 = mean([float(num[25]) for num in pitch_stats])                  
                    stats['AVG/5 Players'].append(avg_5)
                    stats['OBP/5 Players'].append(obp_5)
                    stats['SLG/5 Players'].append(slg_5)
                    stats['WAR/5 Players'].append(war_5)
                    stats['WRC+/5 Players'].append(wrc_5)
                    stats['K Percentage/5 Players'].append(k_5)
                    stats['BB Percentage/5 Players'].append(bb_5)
                    stats['Opposing K/9/5 Players'].append(ok_5)
                    stats['Opposing BB/9/5 Players'].append(obb_5)
                    stats['ERA/5 Players'].append(era_5)
                    stats['Opposing WAR/5 Players'].append(owar_5)

                    #Takes breaks in between iterations to ensure website doesn't get overloaded
                    #time.sleep(random.uniform(3, 5))
                    
                    #Hitting statistics for the away team from game_7 to date
                    hit7 = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=bat&team={away_num}&season1=&season=&month=1000&sortcol=3&sortdir=default&pagenum=1'
                    hit7_page = requests.get(hit7)
                    hit7_soup = BeautifulSoup(hit7_page.content, "html.parser")
                    hit7_table = hit7_soup.find('div', class_='table-scroll')
                    hit7_rows = hit7_table.find('tbody').find_all('tr')
                    
                    #Pitching statistics for the home team from game_7 to date
                    pitch7 = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=pit&team={home_num}&season1=&season=&month=1000&sortcol=7&sortdir=default&pagenum=1'
                    pitch7_page = requests.get(pitch7)
                    pitch7_soup = BeautifulSoup(pitch7_page.content, "html.parser")
                    pitch7_table = pitch7_soup.find('div', class_='table-scroll')
                    pitch7_rows = pitch7_table.find('tbody').find_all('tr')
                    
                    #Initialize lists to hold hitting and pitching statistics for key players
                    hit_stats = []
                    pitch_stats = []
                    
                    #Finds each player that plays for the away team in the hitting statistics
                    for row in hit7_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == away_team:
                            away_hit = [cell.text.strip() for cell in row.find_all('td')]
                            hit_stats.append(away_hit)
                    
                    #Finds each player that plays for the home team in the pitching statistics
                    for row in pitch7_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_pitch = [cell.text.strip() for cell in row.find_all('td')]
                            pitch_stats.append(home_pitch)

                    #Shortens the hitting and pitching lists to the top 5 most active players
                    hit_stats = hit_stats[:5]
                    pitch_stats = pitch_stats[:5]

                    #Calculates averages for the top five players in each category and adds them to the stats dictionary
                    avg_5 = mean([float(num[15]) for num in hit_stats])
                    obp_5 = mean([float(num[16]) for num in hit_stats])
                    slg_5 = mean([float(num[17]) for num in hit_stats])
                    war_5 = mean([float(num[26]) for num in hit_stats])
                    wrc_5 = mean([float(num[20]) for num in hit_stats])
                    k_5 = mean([float(num[11][:len(num[11])-1])/100 for num in hit_stats])
                    bb_5 = mean([float(num[10][:len(num[10])-1])/100 for num in hit_stats])
                    ok_5 = mean([float(num[10]) for num in pitch_stats])
                    obb_5 = mean([float(num[11]) for num in pitch_stats])
                    era_5 = mean([float(num[20]) for num in pitch_stats])
                    owar_5 = mean([float(num[25]) for num in pitch_stats]) 
                    stats['AVG/5 Players'].append(avg_5)
                    stats['OBP/5 Players'].append(obp_5)
                    stats['SLG/5 Players'].append(slg_5)
                    stats['WAR/5 Players'].append(war_5)
                    stats['WRC+/5 Players'].append(wrc_5)
                    stats['K Percentage/5 Players'].append(k_5)
                    stats['BB Percentage/5 Players'].append(bb_5)
                    stats['Opposing K/9/5 Players'].append(ok_5)
                    stats['Opposing BB/9/5 Players'].append(obb_5)
                    stats['ERA/5 Players'].append(era_5)
                    stats['Opposing WAR/5 Players'].append(owar_5)

                    #Takes breaks in between iterations to ensure website doesn't get overloaded
                    #time.sleep(random.uniform(3, 5))
                    
                    #Hitting statistics for each team from game_7 to date
                    team_hit = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=bat&season1=2000&season=2000&postseason=&month=1000&team=0%2Cts'
                    team_hit_page = requests.get(team_hit)
                    team_hit_soup = BeautifulSoup(team_hit_page.content, "html.parser")
                    team_hit_table = team_hit_soup.find('div', class_='table-scroll')
                    team_hit_rows = team_hit_table.find('tbody').find_all('tr')
                    
                    #Pitching statistics for each team from game_7 to date
                    team_pitch = f'https://www.fangraphs.com/leaders/major-league?startdate={game_7}&enddate={date}&ind=0&qual=0&pageitems=2000000000&type=8&pos=all&stats=pit&season1=2000&season=2000&postseason=&team=0,ts&month=1000'
                    team_pitch_page = requests.get(team_pitch)
                    team_pitch_soup = BeautifulSoup(team_pitch_page.content, "html.parser")
                    team_pitch_table = team_pitch_soup.find('div', class_='table-scroll')
                    team_pitch_rows = team_pitch_table.find('tbody').find_all('tr')
                    
                    #Locates hitting statistics for each team
                    for row in team_hit_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_hit = [cell.text.strip() for cell in row.find_all('td')]

                        if team_cell and team_cell.text.strip() == away_team:
                            away_hit = [cell.text.strip() for cell in row.find_all('td')]
                    
                    #Locates pitching statistics for each team
                    for row in team_pitch_rows:
                        team_cell = row.find('td', {'data-stat': 'Team'})
                        if team_cell and team_cell.text.strip() == home_team:
                            home_pitch = [cell.text.strip() for cell in row.find_all('td')]

                        if team_cell and team_cell.text.strip() == away_team:
                            away_pitch = [cell.text.strip() for cell in row.find_all('td')]

                    #Adds new data to the stats dictionary
                    stats['AVG/Week'].append(float(home_hit[15]))
                    stats['OBP/Week'].append(float(home_hit[16]))
                    stats['SLG/Week'].append(float(home_hit[17]))
                    stats['WAR/Week'].append(float(home_hit[26]))
                    stats['WRC+/Week'].append(float(home_hit[20]))
                    stats['K Percentage/Week'].append(float(home_hit[11][:len(home_hit[11])-1])/100)
                    stats['BB Percentage/Week'].append(float(home_hit[10][:len(home_hit[10])-1])/100)
                    stats['Opposing K/9/Week'].append(float(away_pitch[10]))
                    stats['Opposing BB/9/Week'].append(float(away_pitch[11]))
                    stats['ERA/Week'].append(float(away_pitch[20]))
                    stats['Opposing WAR/Week'].append(float(away_pitch[25]))
                    stats['AVG/Week'].append(float(away_hit[15]))
                    stats['OBP/Week'].append(float(away_hit[16]))
                    stats['SLG/Week'].append(float(away_hit[17]))
                    stats['WAR/Week'].append(float(away_hit[26]))
                    stats['WRC+/Week'].append(float(away_hit[20]))
                    stats['K Percentage/Week'].append(float(away_hit[11][:len(away_hit[11])-1])/100)
                    stats['BB Percentage/Week'].append(float(away_hit[10][:len(away_hit[10])-1])/100)
                    stats['Opposing K/9/Week'].append(float(home_pitch[10]))
                    stats['Opposing BB/9/Week'].append(float(home_pitch[11]))
                    stats['ERA/Week'].append(float(home_pitch[20]))
                    stats['Opposing WAR/Week'].append(float(home_pitch[25]))
            
            #Takes breaks in between iterations to ensure website doesn't get overloaded
            #time.sleep(random.uniform(3, 5))
            
            #Updates existing statistics CSV file with new statistics
            new_stats = pd.DataFrame(stats)

            print(new_stats)

            full_df = pd.concat([full_df, new_stats])


            
                #existing_stats = pd.read_csv('stats.csv')

            #  updated_stats = pd.concat([existing_stats, new_stats], ignore_index=True)
            #  updated_stats.to_csv('stats.csv', index=False)
    full_df.to_csv(f'{os.getcwd()}/data/temp.csv', index=False)
    return full_df


def run_sql_file(file_path: str, secret):
    """Executes an SQL file in PostgreSQL using psycopg2."""
    
    # Establish a connection
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=secret['username'],
        password=secret['password'],
        host=DB_HOST,
        port=DB_PORT
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


def remove_temp_file():
    os.remove(f'{os.getcwd()}/data/temp.csv')
    print(f'Temp File Removed!!')


def load_baseball_fg_team_game_data(year_threshold: int, secret):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=secret['username'],
            password=secret['password'],
            host=DB_HOST,
            port=DB_PORT
        )

        data = pd.read_sql(f'SELECT * FROM baseball_stats WHERE EXTRACT(YEAR FROM Date) >= {year_threshold}', con=conn)


        print(data.head())
        print(data.describe())
        print(data.info())

        return data

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error


def generate_game_model(data: pd.DataFrame):
    minmax_scaler = MinMaxScaler( feature_range=(0,1))

    # Load and preprocess data
    #data = pd.read_csv('stats.csv')
    #data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
    data['runs_game'] = data['total_runs']/data['total_games']
    data = data.drop_duplicates(keep='first')



    #data['L10'] = data['Win?'].rolling(window=10, min_periods=1).sum()

    #Differentiate between hitting statistics and pitching statistics
    hitting_stats = ['avg', 'obp', 'slg', 'wrc_plus', 'war', 'k_percentage', 'bb_percentage', 'bsr', 'avg_5_players', 'obp_5_players', 'SLG_5_Players', 'WAR_5_Players', 'WRC_plus_5_Players', 'K_Percentage_5_Players', 'BB_Percentage_5 Players', 'AVG_Week', 'OBP_Week', 'SLG_Week', 'WAR_Week', 'WRC+/Week', 'K Percentage/Week', 'BB Percentage/Week', 'runs_game']
    pitching_stats = ['Opposing K/9', 'Opposing HR/9', 'Opposing BB/9', 'ERA', 'Opposing War', 'Opposing K/9/5 Players', 'Opposing BB/9/5 Players', 'ERA/5 Players', 'Opposing WAR/5 Players', 'Opposing K/9/Week', 'Opposing BB/9/Week', 'ERA/Week', 'Opposing WAR/Week']

    fixed_hitter_stats = []
    fixed_pitcher_stats = []
    for i in hitting_stats:
        i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_hitter_stats.append(i)

    for i in pitching_stats:
        i = i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_pitcher_stats.append(i)


    #Separate between X and y datasets
    features = fixed_hitter_stats + fixed_hitter_stats


    X_normalized = minmax_scaler.fit_transform(data[features]) 
    Y = data['win']


    X_df = pd.DataFrame(X_normalized, columns=features)
    X_df['win'] = Y
    # train = data.sample(frac=0.8, random_state=42)
    # test = data.drop(train.index)

    X_train, X_test, Y_train, Y_test = train_test_split(data[features], Y, test_size=.2)


    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment('MLB Win Prediction Experiment')

    with mlflow.start_run():
        plt.figure(figsize=(30, 30))
        sns.heatmap(X_df.corr(), annot=True,cmap="crest")
        plt.savefig('corr_plot.png')
        mlflow.log_artifact('corr_plot.png')


        win_model = RidgeClassifier()
        win_model.fit(X_train, Y_train)
        predictions = win_model.predict(X_test)


        accuracy = accuracy_score(Y_test, predictions)
        f1 = f1_score(Y_test, predictions)
        recall = recall_score(Y_test, predictions)
        precision = precision_score(Y_test, predictions)
        mlflow.log_params(win_model.get_params())
        mlflow.log_metric('Accuracy', accuracy)
        mlflow.log_metric('F1', f1)
        mlflow.log_metric('Recall', recall)
        mlflow.log_metric('Precision', precision)

        mlflow.set_tag('Training Data Run', 'MLP Classifier')

        signature = infer_signature(X_train, win_model.predict(X_train))

        model_info = mlflow.sklearn.log_model(
            sk_model=win_model,
            artifact_path="win_model",
            signature=signature,
            input_example=X_train,
            registered_model_name="Ridge_Classifier-MLB-ML"
        )

        print(f'Model Generation Complete!! Model URI={model_info.model_uri}')

# https://www.astronomer.io/docs/learn/airflow-mlflow
def create_experiment(experiment_name):
    """Create a new MLFlow experiment with a specified name.
    Save artifacts in the default MLflow location."""

    ts = datetime.now().strftime("%Y%m%d%H%M%s")

    mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
    new_experiment_information = mlflow_hook.run(
        endpoint="api/2.0/mlflow/experiments/create",
        request_params={
            "name": f"{ts}_{experiment_name}",
        },
    ).json()

    # Extract and return the experiment ID
    return new_experiment_information.get("experiment_id")


def perform_model_training(data: pd.DataFrame, experiment_id: str):
    
    os.environ['AWS_ACCESS_KEY_ID'] = ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET_KEY
    
    minmax_scaler = MinMaxScaler( feature_range=(0,1))
    data['runs_game'] = data['total_runs']/data['total_games']
    data = data.drop_duplicates(keep='first')

    mlflow.sklearn.autolog()
    #Differentiate between hitting statistics and pitching statistics
    hitting_stats = ['avg', 'obp', 'slg', 'wrc_plus', 'war', 'k_percentage', 'bb_percentage', 'bsr', 'avg_5_players', 'obp_5_players', 'SLG_5_Players', 'WAR_5_Players', 'WRC_plus_5_Players', 'K_Percentage_5_Players', 'BB_Percentage_5 Players', 'AVG_Week', 'OBP_Week', 'SLG_Week', 'WAR_Week', 'WRC+/Week', 'K Percentage/Week', 'BB Percentage/Week', 'runs_game']
    pitching_stats = ['Opposing K/9', 'Opposing HR/9', 'Opposing BB/9', 'ERA', 'Opposing War', 'Opposing K/9/5 Players', 'Opposing BB/9/5 Players', 'ERA/5 Players', 'Opposing WAR/5 Players', 'Opposing K/9/Week', 'Opposing BB/9/Week', 'ERA/Week', 'Opposing WAR/Week']

    fixed_hitter_stats = []
    fixed_pitcher_stats = []
    for i in hitting_stats:
        i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_hitter_stats.append(i)

    for i in pitching_stats:
        i = i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_pitcher_stats.append(i)


    #Separate between X and y datasets
    features = fixed_hitter_stats + fixed_pitcher_stats


    X_normalized = minmax_scaler.fit_transform(data[features]) 
    Y = data['win']


    X_df = pd.DataFrame(X_normalized, columns=features)
    X_df['win'] = Y

    X_train, X_test, Y_train, Y_test = train_test_split(data[features], Y, test_size=.2)


    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment('MLB Win Prediction Experiment')

    with mlflow.start_run(experiment_id=experiment_id, run_name='airflow-model-generation') as run:
        plt.figure(figsize=(30, 30))
        sns.heatmap(X_df.corr(), annot=True,cmap="crest")
        plt.savefig('corr_plot.png')
        mlflow.log_artifact('corr_plot.png')


        win_model = RidgeClassifier()
        win_model.fit(X_train, Y_train)
        predictions = win_model.predict(X_test)


        accuracy = accuracy_score(Y_test, predictions)
        f1 = f1_score(Y_test, predictions)
        recall = recall_score(Y_test, predictions)
        precision = precision_score(Y_test, predictions)
        mlflow.log_params(win_model.get_params())
        mlflow.log_metric('Accuracy', accuracy)
        mlflow.log_metric('F1', f1)
        mlflow.log_metric('Recall', recall)
        mlflow.log_metric('Precision', precision)

        mlflow.set_tag('Training Data Run', 'MLP Classifier')

        signature = infer_signature(X_train, win_model.predict(X_train))

        model_info = mlflow.sklearn.log_model(
            sk_model=win_model,
            artifact_path="win_model",
            signature=signature,
            input_example=X_train,
            registered_model_name="Ridge_Classifier-MLB-ML"
        )
        
        model_uri = model_info.model_uri

        # create_model = CreateRegisteredModelOperator(
        #     task_id='create-model',
        #     name=f'{experiment_id}-{REGISTERED_MODEL_NAME}'
        # )
        
        return model_uri




def perform_model_training_run_model(data: pd.DataFrame, experiment_id: str):
    
    os.environ['AWS_ACCESS_KEY_ID'] = ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET_KEY
    
    minmax_scaler = MinMaxScaler( feature_range=(0,1))
    data['runs_game'] = data['total_runs']/data['total_games']
    data = data.drop_duplicates(keep='first')

    mlflow.sklearn.autolog()
    #Differentiate between hitting statistics and pitching statistics
    hitting_stats = ['avg', 'obp', 'slg', 'wrc_plus', 'war', 'k_percentage', 'bb_percentage', 'bsr', 'avg_5_players', 'obp_5_players', 'SLG_5_Players', 'WAR_5_Players', 'WRC_plus_5_Players', 'K_Percentage_5_Players', 'BB_Percentage_5 Players', 'AVG_Week', 'OBP_Week', 'SLG_Week', 'WAR_Week', 'WRC+/Week', 'K Percentage/Week', 'BB Percentage/Week', 'runs_game']
    pitching_stats = ['Opposing K/9', 'Opposing HR/9', 'Opposing BB/9', 'ERA', 'Opposing War', 'Opposing K/9/5 Players', 'Opposing BB/9/5 Players', 'ERA/5 Players', 'Opposing WAR/5 Players', 'Opposing K/9/Week', 'Opposing BB/9/Week', 'ERA/Week', 'Opposing WAR/Week']

    fixed_hitter_stats = []
    fixed_pitcher_stats = []
    for i in hitting_stats:
        i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_hitter_stats.append(i)

    for i in pitching_stats:
        i = i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_pitcher_stats.append(i)


    #Separate between X and y datasets
    features = fixed_hitter_stats + fixed_pitcher_stats


    X_normalized = minmax_scaler.fit_transform(data[features]) 
    Y = data['runs_scored']


    X_df = pd.DataFrame(X_normalized, columns=features)
    X_df['runs_scored'] = Y

    X_train, X_test, Y_train, Y_test = train_test_split(data[features], Y, test_size=.2)


    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment('MLB Win Prediction Experiment')

    with mlflow.start_run(experiment_id=experiment_id, run_name='airflow-model-generation') as run:
        plt.figure(figsize=(30, 30))
        sns.heatmap(X_df.corr(), annot=True,cmap="crest")
        plt.savefig('corr_plot.png')
        mlflow.log_artifact('corr_plot.png')

        #Initialize and train the model to predict team runs
        run_model = LinearRegression()
        run_model.fit(X_train, Y_train)
        predictions = run_model.predict(X_test)
        
        mae = mean_absolute_error(Y_test, predictions)
        mse = mean_squared_error(Y_test, predictions)
        r2 = r2_score(Y_test, predictions)
        print(mae)
        print(mse)
        print(r2)



        mlflow.log_params(run_model.get_params())
        mlflow.log_metric('MAE', mae)
        mlflow.log_metric('MSE', mse)
        mlflow.log_metric('R2', r2)

        mlflow.set_tag('Training Data Run', 'Linear Regression Model')

        signature = infer_signature(X_train, run_model.predict(X_train))

        model_info = mlflow.sklearn.log_model(
            sk_model=run_model,
            artifact_path="run_model",
            signature=signature,
            input_example=X_train,
            registered_model_name="LR-MLB-ML"
        )
        
        model_uri = model_info.model_uri

        # create_model = CreateRegisteredModelOperator(
        #     task_id='create-model',
        #     name=f'{experiment_id}-{REGISTERED_MODEL_NAME}'
        # )
        
        return model_uri



def calculate_moving_averages(stats, team):
    '''
    Generates exponential moving averages for each column in the dataset for a specific team

    Inputs:
    stats -- Statistics for the 2024 season
    team -- The desired team to generate rolling averages for

    Returns:
    team_offensive_stats -- A dataset including the exponential moving averages for the desired team on offense
    team_defensive_stats -- A dataset including the exponential moving averages for the desired team on defense
    '''
    
    print('Moving Averages')
    
    #Generates offensive and defensive datasets for the desired team
    team_offensive_stats = pd.DataFrame(stats[stats['offensive_team'] == team]).reset_index().drop(['index'], axis=1)
    team_defensive_stats = pd.DataFrame(stats[stats['defensive_team'] == team]).reset_index().drop(['index'], axis=1)
    
    #Calculates the moving average for the team_offensive_stats dataset
    for col in team_offensive_stats.columns:
        if col not in ['date', 'offensive_team', 'defensive_team']:
            team_offensive_stats[f'{col}_EMA'] = team_offensive_stats[col].ewm(span=28, adjust=False).mean()
    
    #Calculates the moving average for the team_defensive_stats dataset
    for col in team_defensive_stats.columns:
        if col not in ['date', 'offensive_team', 'defensive_team']:
            team_defensive_stats[f'{col}_EMA'] = team_defensive_stats[col].ewm(span=28, adjust=False).mean()
            

    return team_offensive_stats, team_defensive_stats

def predict_stats(team_stats, date):
    '''
    Predicts statistics based on the moving averages generated in calculate_moving)averages

    Inputs:
    team_stats -- The dataset with moving averages used to generate statistics from
    date -- The desired date to predict statistics for

    Returns:
    predicted_stats -- The predicted statistics for the given date
    '''
    print('Stat Predictions')

    #Finds the amount of days to generate predicted statistics for
    latest_recorded_date = team_stats.sort_values(['date'], ascending=False).iloc[0]['date']
    
    date = datetime.strptime(date, "%Y-%m-%d").date()
    num_days = (date-latest_recorded_date).days
    
    #Separates the features that will be used to predict
    all_features = [col for col in team_stats if col not in ['date', 'offensive_team', 'defensive_team']]
    predict_features = [feature for feature in all_features if 'SMA' not in feature and 'EMA' not in feature]
    X = team_stats[all_features]
    y = team_stats[predict_features]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    #RandomForestRegressor model used to predict statistics
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    #Predictions and metrics to grade accuracy of model
    '''predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions, multioutput='raw_values')
    mae = mean_absolute_error(y_test, predictions, multioutput='raw_values')'''

    #Finds the date range that will be used to label the dataset 
    future_dates = pd.date_range(start=team_stats['date'].iloc[-1] + pd.Timedelta(days=1), periods=num_days, freq='D')
    future_day_indices = range(len(team_stats), len(team_stats) + num_days)

    #Pulls the very last entry in the dataset to predict statistics from
    last_values = team_stats[all_features].iloc[-1]

    #Create future feature set with exact same order
    future_X = pd.DataFrame(index=future_day_indices)
    for col in all_features:
        future_X[col] = last_values[col]

    #Generates the future statistics for each day in the date range
    future_predictions = []
    for i in range(num_days):
        prediction = model.predict(future_X.iloc[[i]])
        future_predictions.append(prediction[0])
        last_values = pd.Series(prediction[0], index=predict_features)
        for col in all_features:
            if col in predict_features:
                future_X.loc[i+1:, col] = last_values[col]
    
    future_df = pd.DataFrame(future_predictions, columns=predict_features)
    future_df['date'] = future_dates
    predicted_stats = future_df.iloc[[-1]].reset_index(drop=True)
    
    return predicted_stats

def dataset_maker(home, away, date, stats):
    '''
    Creates two future datasets that will be used to predict outcomes for

    Inputs:
    home -- Home team of the game being predicted
    away -- Away team of the game being predicted
    date -- Date of the game
    stats -- Statistics for the 2024 season

    Returns:
    home_offense -- Data entry for the home team being on offense
    away_offense -- Data entry for the away team being on offense
    
    '''
    print('Dataset Maker')
    hitting_stats = ['avg', 'obp', 'slg', 'wrc_plus', 'war', 'k_percentage', 'bb_percentage', 'bsr', 'avg_5_players', 'obp_5_players', 'SLG_5_Players', 'WAR_5_Players', 'WRC_plus_5_Players', 'K_Percentage_5_Players', 'BB_Percentage_5 Players', 'AVG_Week', 'OBP_Week', 'SLG_Week', 'WAR_Week', 'WRC+/Week', 'K Percentage/Week', 'BB Percentage/Week', 'runs_game']
    pitching_stats = ['Opposing K/9', 'Opposing HR/9', 'Opposing BB/9', 'ERA', 'Opposing War', 'Opposing K/9/5 Players', 'Opposing BB/9/5 Players', 'ERA/5 Players', 'Opposing WAR/5 Players', 'Opposing K/9/Week', 'Opposing BB/9/Week', 'ERA/Week', 'Opposing WAR/Week']

    fixed_hitter_stats = []
    fixed_pitcher_stats = []
    for i in hitting_stats:
        i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_hitter_stats.append(i)

    for i in pitching_stats:
        i = i = str(i).lower()
        i = i.replace(' ', '_')
        i = i.replace('/', '_')
        i = i.replace('+', '_plus')
        i = i.replace('?', '')
        fixed_pitcher_stats.append(i)

        
    #Generates four datasets with their exponential moving averages included
    home_hitting, home_pitching = calculate_moving_averages(stats, home)
    away_hitting, away_pitching = calculate_moving_averages(stats, away)
    
    

    #Modifies the four datasets to have predicted statistics in them
    home_hitting = predict_stats(home_hitting, date)
    home_pitching = predict_stats(home_pitching, date)
    away_hitting = predict_stats(away_hitting, date)
    away_pitching = predict_stats(away_pitching, date)

    #Combines statistics for the home and away teams so that they can be used in the prediction model
    home_offense = pd.concat([home_hitting[fixed_hitter_stats], away_pitching[fixed_pitcher_stats]], axis=1)
    away_offense = pd.concat([away_hitting[fixed_hitter_stats], home_pitching[fixed_pitcher_stats]], axis=1)

    return home_offense, away_offense

def prediction(data, model_uri, model_uri_run):
    '''
    Predicts the outcomes of the games from a given schedule

    Inputs:
    date -- The date of the games being predicted
    schedule -- The schedule of games being predicted

    Returns: None
    '''
    
    game_ids = []
    predictions = []
    
        
    data['runs_game'] = data['total_runs']/data['total_games']
    data = data.drop_duplicates(keep='first')
    feature_cols =  ['avg' ,
                     'obp', 
                     'slg' ,
                     'wrc_plus', 
                     'war',
                     'k_percentage',
                     'bb_percentage', 
                     'bsr',
                     'avg_5_players',
                     'obp_5_players',
                     'slg_5_players',
                     'war_5_players',
                    'wrc_plus_5_players',
                    'k_percentage_5_players',
                    'bb_percentage_5_players',
                     'avg_week',
                     'obp_week',
                     'slg_week',
                     'war_week',
                     'wrc_plus_week',
                     'k_percentage_week',
                     'bb_percentage_week',
                     'runs_game',
                     'opposing_k_9',
                     'opposing_hr_9',
                     'opposing_bb_9',
                     'era',
                     'opposing_war',
                     'opposing_k_9_5_players',
                     'opposing_bb_9_5_players',
                     'era_5_players',
                     'opposing_war_5_players',
                     'opposing_k_9_week',
                     'opposing_bb_9_week',
                     'era_week',
                     'opposing_war_week']
    

    
    today = datetime.now().strftime("%Y-%m-%d")
    games_for_today =  statsapi.schedule(start_date=today, end_date=today)
    id_only = model_uri.split('/')[1]
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION # optional but good to set
    )
    bucket_name = 'mlflow-mlb-prediction'
    s3_path = f'3/{id_only}/artifacts/win_model/model.pkl'
    
    obj = s3.get_object(Bucket=bucket_name, Key=s3_path)
    model = obj['Body'].read()  # bytes
    
    id_only_run =  model_uri_run.split('/')[1]
    
    s3_path_run = f'3/{id_only_run}/artifacts/run_model/model.pkl'
    obj = s3.get_object(Bucket=bucket_name, Key=s3_path_run)
    model_run = obj['Body'].read()  # bytes

    # Load pickle from bytes
    win_model = pickle.loads(model)
    run_model = pickle.loads(model_run)
    cols_model_build = win_model.feature_names_in_
    print(cols_model_build)

    #Ensures there are games to predict
    if games_for_today:
        for game in games_for_today:
            
            game_ids.append(game['game_id'])
            #Finds and generates the dataset for the home and away teams in the game
            home_team, away_team = str(mlb_teams[game['home_name']]['abbr']).lower(), str(mlb_teams[game['away_name']]['abbr']).lower()
            print(away_team, '@', home_team)
            home_offense, away_offense = dataset_maker(home_team, away_team, today, data)
            

            # Predict
            home_win = win_model.predict(home_offense[feature_cols])[0]
            away_win = win_model.predict(away_offense[feature_cols])[0]
            home_runs = run_model.predict(home_offense[feature_cols])[0]
            away_runs = run_model.predict(away_offense[feature_cols])[0]
           # home_runs, away_runs = run_model.predict(home_offense)[0], run_model.predict(away_offense)[0]
            
            #Prints the predicted results of the game
            if home_win > away_win:
                print(  f"  {game['home_name']} Wins")
                predictions.append(home_team)
                
                
            elif home_win < away_win:
                print(  f"  {game['away_name']} Wins")
                predictions.append(away_team)
                
            else:
                if home_runs > away_runs:
                    print(  f"  {game['home_name']} Wins")
                    predictions.append(home_team)
                    
                elif home_runs < away_runs:
                    print(  f"  {game['away_name']} Wins")
                    predictions.append(away_team)
                    
                else:
                    print('Results inconclusive')  
                    predictions.append(np.nan)   
            
        

    else:
        print('No games scheduled for this day.')
    
    full_predictions_dict      = {
        'game_id': game_ids,
        'predictions': predictions
    }
    
    return pd.DataFrame(full_predictions_dict)





def load_predictions_postgres(df: pd.DataFrame, secret):
    
    
    table_name = 'game_predictions'
    columns = '(GAME_PK, TEAM_WINNER_ABBREV)'
    
    try:
        conn = psycopg2.connect(
        dbname=DB_NAME,
        user=secret['username'],
        password=secret['password'],
        host=DB_HOST,
        port=DB_PORT
    )
        
        copy_sql = f"""
        COPY {table_name} {columns}
        FROM STDIN
        WITH CSV
        DELIMITER ','
        NULL '\\N';
        """
        
        
        
        cursor = conn.cursor()
        try:
            
            buffer = StringIO()
            
            df.to_csv(buffer, index=False, header=False, na_rep='\\N')
            buffer.seek(0)
            cursor.copy_expert(sql=copy_sql, file=buffer)
            conn.commit()
            print(f"Data from DataFrame copied to '{table_name}' successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error copying data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        
    
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

    



with DAG(dag_id='load_mlb_game_prediction_aws', start_date=pendulum.datetime(2025,3,1, tz="America/Chicago"), schedule_interval=None, default_args=default_args ) as dag:
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
                "{% endfor %}"
                "\n",
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
                "{% endfor %}"
                "\n",
        channel="#airflow-dag-status",
        username="Airflow-Dag-Updates",
        trigger_rule="one_failed",  # Triggers only if any previous task fails
        dag=dag,
    )
    with TaskGroup("load_baseball_stat_data") as load_baseball_stat_data:
        get_db_auth = PythonOperator(
            task_id='get_db_auth',
            python_callable=get_secret,
            dag=dag
        )
        
        connection =  PythonOperator(
            task_id='test_postgres_connection',
            python_callable=test_postgres_connection,
            op_args=[get_db_auth.output],
            dag=dag
        )

        execute_sql_file_for_creation = PythonOperator(
            task_id='create-sql-tables',
            python_callable=run_sql_file,
            op_args=[f'{os.getcwd()}/sql_files/schema.sql', get_db_auth.output],
            dag=dag
        )

        load_baseball_model_data_task = PythonOperator(
            task_id='load-model-data',
            python_callable=load_baseball_model_data,
            op_args=[get_db_auth.output],
            dag=dag
        )
        initial_bulk_load_data = PythonOperator(
            task_id='bulk-load-data',
            python_callable=copy_df_to_postgres,
            op_args=[f'{os.getcwd()}/data/stats.csv', 'baseball_stats', load_baseball_model_data_task.output, get_db_auth.output ],
            dag=dag
        )

        load_baseball_model_data_task_again = PythonOperator(
        task_id='load-model-data-again',
        python_callable=load_baseball_model_data,
        op_args=[get_db_auth.output],
        dag=dag
        )

        connection >> execute_sql_file_for_creation >> load_baseball_model_data_task >> initial_bulk_load_data >> load_baseball_model_data_task_again

    

    with TaskGroup('load-fangraphs-data') as load_fangraphs_model_data:
        load_fangraph_data_task = PythonOperator(
            task_id='load-fangraphs-data',
            python_callable=scrape_f_graphs_team_data,
            dag=dag,
            retries=3,
            op_args=[2025, load_baseball_model_data_task_again.output ]
        )

        load_new_data_task = PythonOperator(

            task_id='load-new-fangraph-data',
            python_callable=copy_df_to_postgres,
            op_args=[f'{os.getcwd()}/data/temp.csv', 'baseball_stats', load_baseball_model_data_task_again.output, get_db_auth.output ],
            dag=dag
        )

        remove_temp_file_task = PythonOperator(
            task_id='remove-temp-file',
            python_callable=remove_temp_file,
            dag=dag
        )


        load_fangraph_data_task >> load_new_data_task >> remove_temp_file_task


    with TaskGroup('train_model_mlflow') as generate_game_prediciton_model:
        load_model_data = PythonOperator(
            task_id='load_model_data',
            python_callable=load_baseball_fg_team_game_data,
            op_args=[2000, get_db_auth.output],
            dag=dag
        )

        create_experiment_task = PythonOperator(
            task_id='create-experiment-for-model',
            python_callable=create_experiment,
            dag=dag,
            op_args=[EXPERIMENT_NAME]
        )

        perform_model_training_task = PythonOperator(
            task_id='train-model-and-store',
            python_callable=perform_model_training,
            dag=dag,
            op_args=[load_model_data.output, create_experiment_task.output]
        )
        
        perform_model_training_task_run_model = PythonOperator(
            task_id='train-model-and-store-run-model',
            python_callable=perform_model_training_run_model,
            dag=dag,
            op_args=[load_model_data.output, create_experiment_task.output]
        )
        
        
        
        make_game_predictions_task = PythonOperator(
            task_id='make_game_predictions-task',
            python_callable=prediction,
            op_args=[load_model_data.output, perform_model_training_task.output , perform_model_training_task_run_model.output ],
            dag=dag
        )
        
        load_predictions_to_postgres_task = PythonOperator(
            task_id='load_predictions_to_postgres',
            python_callable=load_predictions_postgres,
            op_args=[make_game_predictions_task.output, get_db_auth.output],
            dag=dag
            
        )
        
        
       

        # gen_game_pred_model_task = PythonOperator(
        #     task_id='generate-ml-model',
        #     python_callable=generate_game_model,
        #     op_args=[load_model_data.output]
        # )

        load_model_data >> create_experiment_task >> perform_model_training_task >> perform_model_training_task_run_model >>  make_game_predictions_task >>  load_predictions_to_postgres_task
        

    
    


    load_baseball_stat_data >> load_fangraphs_model_data >> generate_game_prediciton_model >> [slack_success, slack_failure]