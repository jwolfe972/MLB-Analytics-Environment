import psycopg2
import pandas as pd
import numpy as np
from datetime import date, datetime
import statsapi as mlb_stats
###############################################################
outs_map = {
    "double": 0,
    "double_play": 2,
    "field_out": 1,
    "fielders_choice": 0,
    "fielders_choice_out": 1,
    "force_out": 1,
    "grounded_into_double_play": 2,
    "hit_by_pitch": 0,
    "home_run": 0,
    "sac_bunt": 1,
    "sac_bunt_double_play": 2,
    "sac_fly": 1,
    "sac_fly_double_play": 2,
    "single": 0,
    "strikeout": 1,
    "strikeout_double_play": 2,
    "triple": 0,
    "triple_play": 3,
    "walk": 0,
    "catcher_interf": 0,
    "field_error": 0
}

######################################


def load_schedule(start_d='2024-03-29', end_d='2024-09-30'):
    games = mlb_stats.schedule(start_date=start_d, end_date=end_d)

    game_li = []
    for game in games:
        game_li.append(game)

    df = pd.DataFrame(game_li)
    
    df['game_date'] = pd.to_datetime(df['game_date'])
    df = df[df['game_type'] == 'R']
   # df = df[df['game_date'].dt.date == date_filter ]

    # for index, row in df.iterrows():
    #     card(
    #         title=f'{row["home_name"]} vs {row["away_name"]}',
    #         text=f'{row["home_score"]}-{row["away_score"]}'
    #     )

    df['home_team_won'] = df['home_score'] > df['away_score']
    

    return df[['game_id', 'game_date', 'status', 'home_name', 'away_name', 'home_score', 'away_score', 'venue_name', 'home_team_won' ]]


def parse_box_score_json(json_input):
    away_team = json_input['teamInfo']['away']["abbreviation"]
    home_team = json_input['teamInfo']['home']["abbreviation"]
    batting_lineup_home = []
    batting_lineup_away = []
    for record in json_input['homeBatters']:
        # looking for starting batting lineup
        if str(record['battingOrder']).endswith('0'):
            batting_lineup_home.append({'team': home_team,
                'id': str(record['personId']),
                                   'lineup_num':str(record['battingOrder'])[0],
                                    'name': record['name'],
                                     'position': record['position'] })
    for record in json_input['awayBatters']:
        # looking for starting batting lineup
        if str(record['battingOrder']).endswith('0'):
            batting_lineup_away.append({'team': away_team,
                'id': str(record['personId']),
                                   'lineup_num':str(record['battingOrder'])[0],
                                    'name': record['name'],
                                     'position': record['position'] })   
    return pd.DataFrame(batting_lineup_home), pd.DataFrame(batting_lineup_away)



def predict_out_mlb_games():

    total_correct = 0
    game_ids = []
    home_team_wins = []
    actual_home_team_win = []
    date_list = pd.date_range(start='2024-06-01', end='2024-06-07')
    date_list = [date.date() for date in date_list]
    i = 1
    for date in date_list:
        day_schedule = load_schedule(str(date), str(date))
        for index, row in day_schedule.iterrows():
            print(f"Game # {i} - Date {date} {row['home_name']} vs {row['away_name']}")
            box_score = mlb_stats.boxscore_data(row['game_id'], timecode=None)
            df_boxscore_home, df_boxscore_away = parse_box_score_json(box_score)
            home_ids = df_boxscore_home['id'].to_list()
            away_ids = df_boxscore_away['id'].to_list()
            home_team_won = produce_winner(home_ids, away_ids, date)
            print(f'{row['home_name']} won is {home_team_won}')
            game_ids.append(row['game_id'],)
            home_team_wins.append(home_team_won)
            actual_home_team_win.append(row['home_team_won'])
            if home_team_won == row['home_team_won']:
                total_correct +=1
            print(f'Live Accuracy = {total_correct / i}')
            i+=1
    

    df = pd.DataFrame({'game_ids': game_ids, 'home_team_won': home_team_wins, 'actual_result': actual_home_team_win})
    df.to_csv('mlb_game_predictions_2024.csv', index=False)

            
def test_postgres_connection():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="localhost",
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

def get_fact_table_info():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        
        # Create a cursor and test the connection
        
        df = pd.read_sql("SELECT BATTER_ID, COUNT, RS_ON_PLAY, BASES_AFTER, BASES, PLAY_ID, GAME_ID FROM FactPitchByPitchInfo WHERE BASES_AFTER IS NOT NULL;", conn)
        
        
        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

def get_play_table_info():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        
        # Create a cursor and test the connection
        
        df = pd.read_sql("SELECT PLAY_ID, OUTS_WHEN_UP , EVENTS FROM PLAY_INFO_DIM WHERE EVENTS != 'truncated_pa';", conn)
        
        
        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

def get_hitter_table_info():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        
        # Create a cursor and test the connection
        
        df = pd.read_sql("SELECT DISTINCT HITTER_ID, HITTER_NAME FROM HITTER_INFO_DIM;", conn)
        
        
        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

def get_game_table_info():
    try:
        # Define connection parameters
        conn = psycopg2.connect(
            dbname="MLB_DATA",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        
        # Create a cursor and test the connection
        
        df = pd.read_sql("SELECT GAME_PK, GAME_YEAR, GAME_DATE FROM GAME_INFO_DIM;", conn)
        
        
        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        raise  # raise error for error

def get_random_event_for_batter_2023_2024(batter, bases, outs, game_date: date):
    # Filter the DataFrame based on the input parameters
    joined_filtered = joined_data[
        (joined_data["batter_id"] == batter) &
        (joined_data["bases"] == bases) &
        (joined_data["outs_when_up"] == outs) &
        (joined_data['game_date'] < game_date)
    ]
    
    if joined_filtered.empty:
        return None  # Return None if no records are found
    
    # Select a random row
    random_event = joined_filtered.sample(n=1).iloc[0]
    
    # Create the output dictionary
    output = {
        "outs": outs_map.get(random_event["events"]),  # Look up the outs using the events
        "batter": batter,
        "new_bases": random_event["bases_after"],
        "runs_scored": int(random_event["rs_on_play"]),
        "event": random_event["events"]
    }

    return output

def produce_run_scored(batter_ids: list, game_date: date):
    i = 1
    outs = 0
    bases = '0-0-0'
    runs = 0
    current_batter_index = 0
    runs_list = []
    for i in range(25):
        i = 1
        outs = 0
        bases = '0-0-0'
        runs = 0
        current_batter_index = 0
        while i <= 9:
            
            event = get_random_event_for_batter_2023_2024(batter_ids[current_batter_index], bases, outs, game_date)
            if event is None:
                event = {
            "outs": 1,  # Look up the outs using the events
            "batter": batter_ids[current_batter_index],
            "new_bases": bases,
            "runs_scored": 0,
            "event": 'field_out'
            }
            #print(f'Inning # {i} Batter {batter_ids[current_batter_index]} Play {event['event']} Bases={event['new_bases']} RS={event['runs_scored']} Num Outs={outs}')
            outs += event['outs']
            runs += event['runs_scored']
            bases = event['new_bases']
            if current_batter_index + 1 > 8:
                current_batter_index = 0
            else:
                current_batter_index = current_batter_index+1
            
            if outs >= 3:
                outs = 0
                i+=1
                bases = '0-0-0'
        runs_list.append(runs)
    return  np.array(runs_list)
        

def produce_winner(home_batter_ids: list, away_batter_ids: list, game_date: date):
    home = produce_run_scored(home_batter_ids, game_date)
    away = produce_run_scored(away_batter_ids, game_date)
    total_wins = np.sum(home > away)
    total_losses = np.sum(away > home)
    return total_wins > total_losses
            

def convert_string_to_dt(date):
    return datetime.strptime(date, '%Y-%m-%d').date()



if __name__ == "__main__":

    

    #print(load_schedule(convert_string_to_dt('2024-05-01')))

    test_postgres_connection()
    fact_pitch = get_fact_table_info()
    play_info = get_play_table_info()
    hitter_info = get_hitter_table_info()
    game_info = get_game_table_info()

    joined_data = (fact_pitch
    .merge(play_info, on="play_id", how="inner")
    .merge(game_info, left_on="game_id", right_on="game_pk", how="inner")
    )

    predict_out_mlb_games()




    #TODO: Adjust this HMM to change probabilities slightly for these factors:
        # Home vs Away Splits
        # Starting Pitcher (then relivers)
        # Park Factors for Hitting
        # Find a way to make probablities favor the most recent over distant past
    # Also TODO: run this for every game of the 2024 season end of April to September 30 and get the accuracy of this model
    # Hope for 75% Accuracy


    # home_won= produce_winner(home_batter_ids=[514888,608324,670541,673237,665161,572138,676694,605170,663550],
    #                away_batter_ids=[641943,668804,665833,642133,663698,663647,678225,669707,518735])
    
    # print(home_won)

    # i = 1
    # outs = 0
    # bases = '0-0-0'
    # runs = 0
    # batter_id = 592450
    # batter_ids = [592450,660271,592450,660271,592450,660271,592450,660271,592450]
    # current_batter_index = 0
    # while i <= 9:
        
    #     event = get_random_event_for_batter_2023_2024(batter_ids[current_batter_index], bases, outs)
    #     if event is None:
    #         event = {
    #     "outs": 1,  # Look up the outs using the events
    #     "batter": batter_ids[current_batter_index],
    #     "new_bases": bases,
    #     "runs_scored": 0,
    #     "event": 'field_out'
    #      }
    #     print(f'Inning # {i} Batter {batter_ids[current_batter_index]} Play {event['event']} Bases={event['new_bases']} RS={event['runs_scored']} Num Outs={outs}')
    #     outs += event['outs']
    #     runs += event['runs_scored']
    #     bases = event['new_bases']
    #     if current_batter_index + 1 > 8:
    #         current_batter_index = 0
    #     else:
    #         current_batter_index = current_batter_index+1
        
    #     if outs >= 3:
    #         outs = 0
    #         i+=1
    #         bases = '0-0-0'
    
    # print(f'Total Runs {runs}')


