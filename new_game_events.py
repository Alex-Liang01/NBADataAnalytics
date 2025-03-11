from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', None)
from time import sleep
import psycopg2
import os
from sqlalchemy import create_engine

from sqlalchemy.exc import SQLAlchemyError
import requests
from nba_api.stats.endpoints import ShotChartDetail

POSTGRES_USER = os.getenv("POSTGRES_USER", "default_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "default_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_NAME = os.getenv("POSTGRES_NAME", "default_db")

#Identifies missing game events by selecting games from main game table that do not have corresponding entries in the game_events_s table.
def read_data():
    
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = """
            WITH missing_games AS 
            (SELECT DISTINCT g.game_id FROM nba24_25_s.nba_games_s g 
             WHERE NOT EXISTS (
                SELECT 1 FROM nba24_25_s.game_events_s e WHERE e.game_id = g.game_id))
            
            SELECT m.game_id, b.player_id, p.team_id FROM missing_games m 
            LEFT JOIN nba24_25_s.boxscores_s b ON m.game_id = b.game_id 
            LEFT JOIN nba24_25_s.active_nba_players_s p ON b.player_id = p.player_id LIMIT 300
        """
        try:
            data = pd.read_sql(query, engine)
            return data
        except Exception as e:
            print(f"An error occurred while reading SQL: {e}")
        print("Data inserted")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Fetching shotchart data
def fetch_shotchart_data(player_id, game_id, team_id):
    try:
        shotchart = ShotChartDetail(
            player_id=player_id, 
            game_id_nullable=game_id, 
            team_id=team_id, 
            context_measure_simple='FGA'
        )
        fga = shotchart.shot_chart_detail.get_dict()  # Convert to dictionary
        print(f"Fetched data for Player {player_id}, Game {game_id}.")
        return fga
    except requests.exceptions.Timeout:
        print(f"Timeout for Player {player_id}, Game {game_id}. Skipping...")
    except requests.exceptions.RequestException as e:
        print(f"Request error for Player {player_id}, Game {game_id}: {e}")
    except Exception as e:
        print(f"Unexpected error for Player {player_id}, Game {game_id}: {e}")
    return None
# Getting Shot chart data
def get_shotchart_data(data):
    fga_final = []
    for _, row in data.iterrows():
        fga = fetch_shotchart_data(player_id=row['player_id'], game_id=row['game_id'], team_id=row['team_id'])
        if fga and "data" in fga and "headers" in fga:
            FGA = pd.DataFrame(fga["data"], columns=fga["headers"])
            fga_final.append(FGA)
        else:
            print(f"Skipping Player {row['team_id']}, Game {row['game_id']} due to missing data.")
        sleep(1) 
    if fga_final:
        return pd.concat(fga_final, ignore_index=True)
    else:
        print("No data collected.")
        return pd.DataFrame()

# Getting only active players
def active_players(data, players):
    if data:
        data = data[data['player_id'].isin(players['player_id'])]
        return data

# Removing existing shot chart locations
def exisitingLoc(data, shot):
    if data:
        data = data[~data[['loc_x', 'loc_y']].apply(tuple, axis=1).isin(shot[['loc_x', 'loc_y']].apply(tuple, axis=1))]
        return data

# Active players in the database
def selectPlayers():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = "SELECT p.player_id FROM nba24_25_s.active_nba_players_s p;"
        try:
            data = pd.read_sql(query, engine)
            return data
        except Exception as e:
            print(f"An error occurred while reading SQL: {e}")
        print("Data inserted")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Inserting into databsae
def intitalInsertion(data, tablename):

    if data is not None and not data.empty:
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
                f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
            )
            data.to_sql(tablename, engine, schema="nba24_25_s", if_exists="append", index=False)
            print("Data inserted into table:", tablename)
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f'No new data for table {tablename}')

# Normalized data
def normalization(data):

    if data is not None and not data.empty:
        # Normalize Game Events
        Game_events = data[['GAME_ID','GAME_EVENT_ID','PLAYER_ID','PERIOD','MINUTES_REMAINING',
                            'SECONDS_REMAINING','LOC_X','LOC_Y','SHOT_ATTEMPTED_FLAG','SHOT_MADE_FLAG']]
        Game_events.columns = ['game_id','event_id','player_id','current_period','minutes_remaining',
                                 'seconds_remaining','loc_x','loc_y','shot_attempted','shot_made']
        Game_events = Game_events.copy()
        Game_events['game_id'] = Game_events['game_id'].astype(str)
        Game_events['player_id'] = Game_events['player_id'].astype(str)
        Game_events['event_id'] = Game_events['event_id'].astype(str)
        print("Game Events")
        print(Game_events.head(5))
        
        # Normalize Shot Descriptions
        Shot_descriptions = data[['LOC_X','LOC_Y','SHOT_DISTANCE','SHOT_TYPE','SHOT_ZONE_RANGE',
                                  'SHOT_ZONE_AREA','SHOT_ZONE_BASIC']].drop_duplicates()
        Shot_descriptions.columns = ['loc_x','loc_y','shot_distance','shot_type',
                                     'shot_zone_range','shot_zone_area','shot_zone_basic']
        print("Shot Descriptions")
        print(Shot_descriptions.head(5))
        return Game_events, Shot_descriptions
    else:
        return None, None

# Selecting existing shot chart descriptions
def selectShot_descriptions():

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = "SELECT s.loc_x, s.loc_y FROM nba24_25_s.shot_descriptions_s s;"
        try:
            data = pd.read_sql(query, engine)
            return data
        except Exception as e:
            print(f"An error occurred while reading SQL: {e}")
        print("Data inserted")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Processing the data sequentially
def process_game_events():
    data = read_data()
    if not data.empty:
        data = get_shotchart_data(data)
        players = selectPlayers()
        sd = selectShot_descriptions()
        Game_events, Shot_descriptions = normalization(data)
        Game_events = active_players(Game_events, players)
        Shot_descriptions = exisitingLoc(Shot_descriptions, sd)
        intitalInsertion(Game_events, "game_events_s")
        intitalInsertion(Shot_descriptions, "shot_descriptions_s")
    else:
        print("No new data")
# Main Airflow DAG to automate new data
with DAG("nba_new_game_events_dag",
         schedule_interval="*/10 * * * *",
         default_args={"start_date": datetime(2024, 3, 10), "catchup": False},
         catchup=False) as dag:

    process_game_events_task = PythonOperator(
        task_id="process_game_events",
        python_callable=process_game_events
    )
