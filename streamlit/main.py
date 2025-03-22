import streamlit as st
import numpy as np
import pandas as pd
import pybaseball
import statsapi as mlb_stats
import datetime
from streamlit_card import card
import os

import base64



TEAM_LOGOS_PATH = os.environ['MLB_TEAM_LOGOS']

all_teams = {'Chicago Cubs': 'CHC.png', 
             'New York Yankees': 'NYY.png', 
             'Toronto Blue Jays': 'TOR.png', 
             'Texas Rangers': 'TEX.png',
             'Washington Nationals': 'WSH.png',
            'Kansas City Royals': 'KC.png',
            'Houston Astros': 'HOU.png',
            'Cincinnati Reds': 'CIN.png',
            'San Diego Padres': 'SD.png',
            'Chicago White Sox': 'CWS.png',
            'Miami Marlins': 'MIA.png',
            'St. Louis Cardinals': 'STL.png',
            'Los Angeles Dodgers': 'LAD.png',
            'Arizona Diamondbacks': 'AZ.png',
            'Seattle Mariners': 'SEA.png',
            'Tampa Bay Rays': 'TB.png',
            'Milwaukee Brewers': 'MIL.png',
            'Baltimore Orioles': 'BAL.png',
            'Philadelphia Phillies': 'PHI.png',
            'Athletics': 'OAK.png',
            'Oakland Athletics': 'OAK.png',
            'Minnesota Twins': 'MIN.png',
            'Detroit Tigers': 'DET.png',
            'Boston Red Sox': 'BOS.png',
            'New York Mets': 'NYM.png',
            'Colorado Rockies': 'COL.png',
            'Pittsburgh Pirates': 'PIT.png',
            'San Francisco Giants': 'SF.png',
            'Atlanta Braves': 'ATL.png',
            'Los Angeles Angels': 'LAA.png',
            'Cleveland Guardians': 'CLE.png'}





@st.cache_data
def load_schedule(date_filter):
    games = mlb_stats.schedule(start_date=date_filter, end_date=date_filter)

    game_li = []
    for game in games:
        game_li.append(game)

    df = pd.DataFrame(game_li)
    
    df['game_date'] = pd.to_datetime(df['game_date'])
    df = df[df['game_type'] == 'R']
    df = df[df['game_date'].dt.date == date_filter ]

    # for index, row in df.iterrows():
    #     card(
    #         title=f'{row["home_name"]} vs {row["away_name"]}',
    #         text=f'{row["home_score"]}-{row["away_score"]}'
    #     )




    

    return df[['game_id', 'game_date', 'status', 'home_name', 'away_name', 'home_score', 'away_score', 'venue_name' ]]



# def load_card_information(df):
    
#     for index, row in df.iterrows():
#         image_path = f'{TEAM_LOGOS_PATH}/{all_teams.get(row["home_name"])}'
#         print(image_path)

#         with open(image_path, "rb") as f:
#             data = f.read()
#             encoded = base64.b64encode(data)
#         data = "data:image/png;base64," + encoded.decode("utf-8")

        
        # card(
        #     title=f'{row["home_name"]} vs {row["away_name"]}',
        #     text=f'{row["home_score"]}-{row["away_score"]}',
        #     image=data

        # )

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







def load_card_information(df):
    num_cols = 2  # Number of columns in the grid
    rows = df.shape[0]  # Number of rows in the dataframe

    for i in range(0, rows, num_cols):
        cols = st.columns(num_cols)  # Create columns for the grid
        
        for j in range(num_cols):
            if i + j < rows:  # Ensure we don't exceed dataframe rows
                row = df.iloc[i + j]
                image_path = f'{TEAM_LOGOS_PATH}/{all_teams.get(row["home_name"])}'

                with open(image_path, "rb") as f:
                    data = f.read()
                    encoded = base64.b64encode(data)
                data = "data:image/png;base64," + encoded.decode("utf-8")

                # Place the card in the corresponding column
                with cols[j]:
                     with st.container():  # Ensures full display within column
                        card(
                    title=f'{row["home_name"]} vs {row["away_name"]}',
                    text=f'{row["home_score"]}-{row["away_score"]} {row["status"]}\nPredicted Winner: redacted',
                    image=data

                        )
                        box_score = mlb_stats.boxscore_data(row['game_id'], timecode=None)
                        df_boxscore_home, df_boxscore_away = parse_box_score_json(box_score)
                        st.write(df_boxscore_home)
                        st.write(df_boxscore_away)



def update_win_model():
    pass



st.title('MLB Game Prediction Model')
selected_date = st.date_input("Select a date", value=datetime.datetime.today())






chart_data = pd.DataFrame(np.random.randn(20), columns=["a"])

st.subheader('Prediction Accuracy over time')
st.line_chart(chart_data)

st.subheader(f'Todays Date For Game Predictions: {datetime.datetime.today().strftime("%m/%d/%Y")}')



# st.button(label='Load Model', on_click=update_win_model)

data = load_schedule(selected_date)

df = mlb_stats.boxscore_data('744991', timecode=None)
#print(df)
# st.write(df)


st.subheader('Games Scheduled Today')
load_card_information(data)


#st.write(data)
# for _, game in data.iterrows():
#     home_team = game['home_name']
#     away_team = game['away_name']
#     home_score = game['home_score']
#     away_score = game['away_score']
#     inning = game['current_inning']
    
#     # Create the layout with columns for home/away teams and a centered score/inning card
#     col1, col2, col3 = st.columns([1, 2, 1])

#     with col1:
#         # Home Team Card
#         st.markdown(f"""
#             <div style="padding: 10px; background-color: grey; border-radius: 10px; text-align: center;">
#                 <strong>{home_team}</strong>
#             </div>
#         """, unsafe_allow_html=True)

#     with col2:
#         # Score and Inning Card
#         st.markdown(f"""
#             <div style="padding: 10px; background-color: grey; border-radius: 10px; text-align: center;">
#                 <strong>Score:</strong> {away_score} - {home_score} <br>
#                 <strong>Inning:</strong> {inning}
#             </div>
#         """, unsafe_allow_html=True)

#     with col3:
#         # Away Team Card
#         st.markdown(f"""
#             <div style="padding: 10px; background-color: grey; border-radius: 10px; text-align: center;">
#                 <strong>{away_team}</strong>
#             </div>
#         """, unsafe_allow_html=True)