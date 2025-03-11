from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from nba_api.live.nba.endpoints import boxscore
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import requests
import os 

POSTGRES_USER = os.getenv("POSTGRES_USER", "default_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "default_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_NAME = os.getenv("POSTGRES_NAME", "default_db")

# Function to select games (and player IDs) where game_players_s has not been populated yet.
def selectGamesPlayers():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = """
            SELECT g.game_id, b.player_id
            FROM nba24_25_s.nba_games_s g 
            LEFT JOIN nba24_25_s.boxscores_s b ON g.game_id = b.game_id 
            WHERE NOT EXISTS (
                SELECT 1 FROM nba24_25_s.game_players_s p WHERE p.game_id = g.game_id
            ) AND g.game_id IS NOT NULL 
            AND b.player_id IS NOT NULL
            AND g.game_id != '0062400001';
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

# Function to insert data into PostgreSQL using the provided table name.
def intitalInsertion(data, tablename):
    if data is not None and not data.empty:
        try:
            # Using the same credentials as above for consistency.
            engine = create_engine(
                f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
                f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
            )
            data.to_sql(tablename, engine, schema="nba24_25_s", if_exists="append", index=False)
            print("Data inserted into " + tablename)
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f'No new data for table {tablename}')

# Main function to process new player games.
def process_new_player_games():
    data = selectGamesPlayers()
    intitalInsertion(data, "game_players_s")

# Define the DAG
with DAG("nba_new_player_games_dag",
         schedule_interval="*/10 * * * *",
         default_args={"start_date": datetime(2024, 3, 10), "catchup": False},
         catchup=False) as dag:

    process_new_player_games_task = PythonOperator(
        task_id="process_new_player_games",
        python_callable=process_new_player_games
    )
