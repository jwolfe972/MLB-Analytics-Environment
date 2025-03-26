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


MLFLOW_CONN_ID = "mlflow_default"
EXPERIMENT_NAME = "MLB-GAME-PREDICTION"
REGISTERED_MODEL_NAME = "MLB-Prediction-Model"
############################################################################################################
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


def copy_df_to_postgres(file_name: str, table_name: str, stats_df: pd.DataFrame):


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

        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )

        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table_name, sep=',', null='\\N', columns=df_filtered.columns)
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


def test_postgres_connection():
    print(os.getcwd())
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


def load_baseball_model_data():
    try:
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
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


def remove_temp_file():
    os.remove(f'{os.getcwd()}/data/temp.csv')
    print(f'Temp File Removed!!')


def load_baseball_fg_team_game_data(year_threshold: int):
    try:
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="postgres",
            port="5432"
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
    features = fixed_hitter_stats + fixed_hitter_stats


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

        # model_info = mlflow.sklearn.log_model(
        #     sk_model=win_model,
        #     artifact_path="win_model",
        #     signature=signature,
        #     input_example=X_train,
        #     registered_model_name="Ridge_Classifier-MLB-ML"
        # )

        create_model = CreateRegisteredModelOperator(
            task_id='create-model',
            name=f'{experiment_id}-{REGISTERED_MODEL_NAME}'
        )

        
def load_ml_model(model_uri: str):
    pass


def make_game_predicitions(model):
    pass

with DAG(dag_id='load_mlb_game_prediction', start_date=pendulum.datetime(2025,3,1, tz="America/Chicago"), schedule_interval=None, default_args=default_args ) as dag:
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
        connection =  PythonOperator(
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

        load_baseball_model_data_task = PythonOperator(
            task_id='load-model-data',
            python_callable=load_baseball_model_data,
            dag=dag
        )
        initial_bulk_load_data = PythonOperator(
            task_id='bulk-load-data',
            python_callable=copy_df_to_postgres,
            op_args=[f'{os.getcwd()}/data/stats.csv', 'baseball_stats', load_baseball_model_data_task.output ],
            dag=dag
        )

        load_baseball_model_data_task_again = PythonOperator(
        task_id='load-model-data-again',
        python_callable=load_baseball_model_data,
        dag=dag
        )

        connection >> execute_sql_file_for_creation >> load_baseball_model_data_task >> initial_bulk_load_data >> load_baseball_model_data_task_again

    

    with TaskGroup('load-fangraphs-data') as load_fangraphs_model_data:
        load_fangraph_data_task = PythonOperator(
            task_id='load-fangraphs-data',
            python_callable=scrape_f_graphs_team_data,
            dag=dag,
            retries=3,
            op_args=[2024, load_baseball_model_data_task_again.output ]
        )

        load_new_data_task = PythonOperator(

            task_id='load-new-fangraph-data',
            python_callable=copy_df_to_postgres,
            op_args=[f'{os.getcwd()}/data/temp.csv', 'baseball_stats', load_baseball_model_data_task_again.output ],
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
            op_args=[2010],
            dag=dag
        )

        # produce_game_model = PythonOperator(
        #     task_id='produce-game-model',
        #     python_callable=generate_game_model,
        #     op_args=[load_model_data.output],
        #     dag=dag
        # )

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

        load_ml_model_task = PythonOperator(
            task_id='load-ml-model-task',
            python_callable=load_ml_model,
            op_args=['dummy'],
            dag=dag
        )

        make_game_predictions_task = PythonOperator(
            task_id='make_game_predictions-task',
            python_callable=make_game_predicitions,
            op_args=['dummy'],
            dag=dag
        )

        # gen_game_pred_model_task = PythonOperator(
        #     task_id='generate-ml-model',
        #     python_callable=generate_game_model,
        #     op_args=[load_model_data.output]
        # )

        load_model_data >> create_experiment_task >> perform_model_training_task >> load_ml_model_task >> make_game_predictions_task
        

    
    


    load_baseball_stat_data >> load_fangraphs_model_data >> generate_game_prediciton_model >> [slack_success, slack_failure]