from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os 

POSTGRES_USER = os.getenv("POSTGRES_USER", "default_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "default_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_NAME = os.getenv("POSTGRES_NAME", "default_db")

# Identifying newest date in the DB 
def dateOfMostRecentGameInDB():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = "SELECT MAX(g.game_date) as Date FROM nba24_25_S.nba_games_S g;"
        try:
            data = pd.read_sql(query, engine)
            if not data.empty and data.iloc[0, 0] is not None:
                return data.iloc[0, 0]
        except Exception as e:
            print(f"An error occurred: {e}")
        print("Data inserted")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Identifying all the games from the previous newest date until today
# Wrangles data into suitable for star schema
def games(beginning_date):
    try:
        result = leaguegamefinder.LeagueGameFinder()
        all_games = result.get_data_frames()[0]
    except requests.exceptions.Timeout:
        print("Time out Error")
        all_games = []
    
    # Set the date range from the beginning_date until today
    today_date = date.today()
    
    # Convert GAME_DATE to date format
    all_games['GAME_DATE'] = pd.to_datetime(all_games['GAME_DATE']).dt.date
    teams = ['CLE','BOS','NYK','MIL','IND','DET','ORL','MIA','ATL','CHI','PHI',
             'BKN','TOR','CHA','WAS','OKC','HOU','MEM','DEN','LAL','LAC','MIN',
             'PHX','DAL','SAC','GSW','SAS','POR','NOP','UTA']
    new_games = all_games.loc[(all_games['GAME_DATE'] >= beginning_date) & (all_games['GAME_DATE'] <= today_date)]
    if not new_games.empty:
        new_games = new_games[new_games['TEAM_ABBREVIATION'].isin(teams)]
        new_games = new_games.sort_values(by=['GAME_DATE','GAME_ID'], ascending=False)
        
        # Prepare NBA_GAME_TEAM_STATS table data
        NBA_game_team_stats = new_games[["GAME_ID","TEAM_ID","WL","MIN","FGM","FGA",
                                         "FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA",
                                         "FT_PCT","OREB","DREB","REB","AST","STL","BLK",
                                         "PF","PTS","PLUS_MINUS"]]
        NBA_game_team_stats.columns = ['game_id','team_id','wl','min','fgm','fga','fg_pct',
                                       'fg3m','fg3a','fg3_pct','ftm','fta','ft_pct','oreb',
                                       'dreb','reb','ast','stl','blk','pf','pts','plus_minus']
        print("NBA_game_team_stats")
        print(NBA_game_team_stats.head(5))
        
        # Prepare NBA_Games table data
        new_games["TEAM_ID"] = new_games["TEAM_ID"].astype(str)
        new_games["HOME_TEAM_ID"] = new_games.apply(lambda x: x["TEAM_ID"] if "vs." in x["MATCHUP"] else None, axis=1)
        new_games["AWAY_TEAM_ID"] = new_games.apply(lambda x: x["TEAM_ID"] if "@" in x["MATCHUP"] else None, axis=1)
        nba_games = new_games.groupby("GAME_ID").agg({
            "GAME_DATE": "first",
            "HOME_TEAM_ID": "first",
            "AWAY_TEAM_ID": "first"
        }).reset_index()
        nba_games.columns = ['game_id','game_date','home_team_id','visiting_team_id']
        nba_games = nba_games.astype({"game_id": "string", "home_team_id": "string", "visiting_team_id": "string"})
        nba_games['game_date'] = pd.to_datetime(nba_games['game_date'])
        print("nba_games")
        print(nba_games.head(5))
        return nba_games, NBA_game_team_stats
    else:
        return None, None

# Inserting into games table
def Insertion(data, tablename):
    if data is not None and not data.empty:
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
                f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
            )
            data.to_sql(tablename, engine, schema="nba24_25_s", if_exists="append", index=False)
            print(f"Data inserted into table {tablename}")
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f"No new data for table {tablename}")

def process_games():
    date_of_most_recent_game = dateOfMostRecentGameInDB()
  
    beginning_date = date_of_most_recent_game + timedelta(days=1)
    if beginning_date!=date.today():
        nba_games, NBA_game_team_stats = games(beginning_date)
        Insertion(nba_games, "nba_games_s")
        Insertion(NBA_game_team_stats, "nba_games_team_stats_s")
    else:
        print("REWRWE")
# Airflow DAG to automate retrival of new games
with DAG("nba_newgames_dag",
         schedule_interval="*/10 * * * *",
         default_args={"start_date": datetime(2024, 3, 10), "catchup": False},
         catchup=False) as dag:

    process_games_task = PythonOperator(
        task_id="process_games",
        python_callable=process_games
    )

