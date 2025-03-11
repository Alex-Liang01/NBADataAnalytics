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

# Function to select games that don't yet have boxscore data inserted
def selectGames():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = """
            SELECT g.game_id 
            FROM nba24_25_s.nba_games_s g
            WHERE NOT EXISTS (
                SELECT 1 FROM nba24_25_s.boxscores_s b WHERE b.game_id = g.game_id
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

# Function to retrieve player boxscores and prepare injury reports
def player_boxscores(game_ids):
    if game_ids.size != 0:
        boxscores_final = []
        for id in game_ids:
            try:
                box = boxscore.BoxScore(id)
                data = box.game.get_dict()
            except requests.exceptions.Timeout:
                print("Time out Error")
                data = []
            
            # Home Team Data Wrangling
            home_players_boxscores = pd.json_normalize(data['homeTeam']['players'])
            if 'statistics' in home_players_boxscores.columns:
                data_stats = pd.json_normalize(home_players_boxscores['statistics'])
                home_players_boxscores = home_players_boxscores.drop(columns=['statistics']).join(data_stats)
            home_players_boxscores['HomeOrAway'] = 'Home'
            home_players_boxscores.columns = home_players_boxscores.columns.str.replace('statistics.', '')
            
            # Away Team Data Wrangling
            away_players_boxscores = pd.json_normalize(data['awayTeam']['players'])
            if 'statistics' in away_players_boxscores.columns:
                data_stats = pd.json_normalize(away_players_boxscores['statistics'])
                away_players_boxscores = away_players_boxscores.drop(columns=['statistics']).join(data_stats)
            away_players_boxscores['HomeOrAway'] = 'Away'
            away_players_boxscores.columns = away_players_boxscores.columns.str.replace('statistics.', '')
            
            # Combining individual game boxscores
            boxscores = pd.concat([home_players_boxscores, away_players_boxscores], ignore_index=True)
            boxscores['GAME_ID'] = id
            boxscores_final.append(boxscores)
        
        # Combining all boxscores
        boxscores_final = pd.concat(boxscores_final, ignore_index=True)
        boxscores_final = boxscores_final.drop(columns=['order','jerseyNum','oncourt','nameI','firstName','familyName','minutesCalculated','plus'])
        
        # Create Injury Report table
        injury_report = boxscores_final[["GAME_ID", "personId", "notPlayingReason", "notPlayingDescription"]]
        injury_report.columns = ['game_id', 'player_id', 'not_playing_reason', 'not_playing_description']
        injury_report = injury_report.copy()
        injury_report['game_id'] = injury_report['game_id'].astype(str)
        injury_report['player_id'] = injury_report['player_id'].astype(str)
        print("Injury Report")
        print(injury_report.head(5))
        print(boxscores_final.columns)
        
        # Create Boxscore table
        boxscores = boxscores_final[[
            "GAME_ID", "personId", "starter", "played", "assists", "blocks", "blocksReceived", 
            "fieldGoalsMade", "fieldGoalsAttempted", "fieldGoalsPercentage", "foulsOffensive", 
            "foulsDrawn", "foulsPersonal", "foulsTechnical", "freeThrowsMade", "freeThrowsAttempted",
            "freeThrowsPercentage", "minutes", "plusMinusPoints", "pointsFastBreak", 
            "pointsInThePaint", "pointsSecondChance", "points", "reboundsOffensive", 
            "reboundsDefensive", "reboundsTotal", "steals", "threePointersMade", 
            "threePointersAttempted", "threePointersPercentage", "turnovers", "twoPointersMade", 
            "twoPointersAttempted", "twoPointersPercentage", "HomeOrAway"
        ]]
        boxscores.columns = [
            'game_id', 'player_id', 'starter', 'played', 'assists', 'blocks', 'blocks_received', 
            'fgm', 'fga', 'fg_pct', 'fouls_offensive', 'fouls_drawn', 'fouls_personal', 
            'fouls_technical', 'ftm', 'fta', 'ft_pct', 'minutes_played', 'plus_minus', 
            'points_fast_break', 'points_in_paint', 'points_second_chance', 'pts', 'oreb', 
            'dreb', 'reb', 'stl', 'fg3m', 'fg3a', 'fg3_pct', 'turnovers', 'fg2m', 'fg2a', 
            'fg2_pct', 'homeoraway'
        ]
        boxscores = boxscores.copy()
        boxscores['game_id'] = boxscores['game_id'].astype(str)
        boxscores['player_id'] = boxscores['player_id'].astype(str)
        print("Boxscore")
        print(boxscores.head(5))
        
        return boxscores, injury_report
    else:
        return None, None

# Function to select active players from the database
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

# Function to filter data for active players only
def active_players(data, players):
    if data is not None and not data.empty:
        data = data[data['player_id'].isin(players['player_id'])]
        return data

# Function to insert data into PostgreSQL
def intitalInsertion(data, tablename):
    if data is not None and not data.empty:
        try:
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

# Main function to process boxscores and insert data into the database
def process_boxscores():
    game_ids = selectGames()
    if game_ids is not None:
        game_ids = game_ids['game_id'].unique()
        print("Game IDs:", game_ids)
        boxscores, injury_report = player_boxscores(game_ids)
        players = selectPlayers()
        boxscores = active_players(boxscores, players)
        injury_report = active_players(injury_report, players)
        intitalInsertion(boxscores, "boxscores_s")
        intitalInsertion(injury_report, "injury_report_s")
    else:
        print("No game IDs found.")

# Define the DAG
with DAG("nba_boxscores_dag",
         schedule_interval="*/10 * * * *",
         default_args={"start_date": datetime(2024, 3, 10), "catchup": False},
         catchup=False) as dag:

    process_boxscores_task = PythonOperator(
        task_id="process_boxscores",
        python_callable=process_boxscores
    )