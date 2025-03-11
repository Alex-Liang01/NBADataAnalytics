from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
pd.set_option('display.max_columns', None)
from time import sleep
import psycopg2
from sqlalchemy import create_engine

from sqlalchemy.exc import SQLAlchemyError
from nba_api.stats.endpoints import PlayByPlayV2
import os
# PostgreSQL credentials
POSTGRES_USER = os.getenv("POSTGRES_USER", "default_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "default_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_NAME = os.getenv("POSTGRES_NAME", "default_db")

# Reading game ids to identify which games don't have play by play data yet
def read_data():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = """
            SELECT DISTINCT g.game_id, g.home_team_id, g.visiting_team_id
            FROM nba24_25_s.nba_games_s g
            WHERE NOT EXISTS (
                SELECT 1 FROM nba24_25_s.play_by_play_score_s s WHERE s.game_id = g.game_id
            );
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

# Fetching and processing play by play data
def playbyplay(games):

    if games is not None and not games.empty:
        data_list = [] 
        for i, row in enumerate(games.itertuples(index=False), start=1):
            print(i)
            print(row)            
            game_id = row.game_id
            # Fetch play-by-play data for the game
            playbyplay_data = PlayByPlayV2(game_id)
            data = playbyplay_data.get_data_frames()[0]
    
            # Filter and process data: remove rows with NaN in SCORE and duplicate scores
            data = data.dropna(subset=['SCORE'])
            data = data.drop_duplicates(subset=['SCORE'])
            
            # Split the SCORE column into AWAY and HOME points
            away_home = data['SCORE'].str.split('-', expand=True)
            away_home.columns = ['AWAY', 'HOME']
            
            # Keep only the relevant columns and combine with the split score columns
            data = data[['GAME_ID', 'PERIOD', 'PCTIMESTRING']]
            data = pd.concat([data, away_home], axis=1)
            
            # Create a DataFrame from the current game row and merge to retain game info
            row_df = pd.DataFrame([row._asdict()])  
            data = data.merge(row_df, how='left', left_on='GAME_ID', right_on='game_id')
    
            # Append the processed DataFrame for this game to the list
            data_list.append(data)
            sleep(1)  # Pause between API calls
    
        playbyplayscore = pd.concat(data_list)
        print("Play By Play Score")
        # Select and rename columns for star schema
        playbyplayscore = playbyplayscore[['GAME_ID','PERIOD','PCTIMESTRING','HOME','AWAY']]
        playbyplayscore.columns = ['game_id','current_period','time_left','htm_pts','vtm_pts']
        playbyplayscore = playbyplayscore.drop_duplicates()
        print(playbyplayscore.head(5))
        return playbyplayscore
    else:
        return None

# Inserting into the db
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

#Main pipeline
def process_play_by_play_score():
    games = read_data()
    playbyplayscore = playbyplay(games)
    intitalInsertion(playbyplayscore, "play_by_play_score_s")

# Airflow DAG
with DAG("nba_new_play_by_play_score_dag",
         schedule_interval="*/10 * * * *",
         default_args={"start_date": datetime(2024, 3, 10), "catchup": False},
         catchup=False) as dag:

    process_play_by_play_score_task = PythonOperator(
        task_id="process_play_by_play_score",
        python_callable=process_play_by_play_score
    )
