from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import pandas as pd
from time import sleep
import requests
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from nba_api.live.nba.endpoints import boxscore
from nba_api.stats.endpoints import leaguegamefinder, PlayByPlayV2, ShotChartDetail
import os
POSTGRES_USER = os.getenv("POSTGRES_USER", "default_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "default_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_NAME = os.getenv("POSTGRES_NAME", "default_db")

#Inserting into the DB
def insert_data(data, tablename):
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

# Getting only active players
def select_active_players():
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
            print(f"Error reading active players: {e}")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Identifying most recent date in the db
def date_of_most_recent_game_in_db():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = "SELECT MAX(g.game_date) as Date FROM nba24_25_s.nba_games_s g;"
        try:
            data = pd.read_sql(query, engine)
            if not data.empty and data.iloc[0, 0] is not None:
                return data.iloc[0, 0]
        except Exception as e:
            print(f"Error reading most recent game date: {e}")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Getting Relevant Games
def games(beginning_date):
    try:
        result = leaguegamefinder.LeagueGameFinder()
        all_games = result.get_data_frames()[0]
    except requests.exceptions.Timeout:
        print("Timeout error during leaguegamefinder call")
        all_games = []
    
    today_date = date.today()
    all_games['GAME_DATE'] = pd.to_datetime(all_games['GAME_DATE']).dt.date
    teams = ['CLE','BOS','NYK','MIL','IND','DET','ORL','MIA','ATL','CHI','PHI',
             'BKN','TOR','CHA','WAS','OKC','HOU','MEM','DEN','LAL','LAC','MIN',
             'PHX','DAL','SAC','GSW','SAS','POR','NOP','UTA']
    new_games = all_games.loc[(all_games['GAME_DATE'] >= beginning_date) & (all_games['GAME_DATE'] <= today_date)]
    if not new_games.empty:
        new_games = new_games[new_games['TEAM_ABBREVIATION'].isin(teams)]
        new_games = new_games.sort_values(by=['GAME_DATE','GAME_ID'], ascending=False)
        
        # Prepare NBA_GAME_TEAM_STATS data
        NBA_game_team_stats = new_games[["GAME_ID","TEAM_ID","WL","MIN","FGM","FGA",
                                         "FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA",
                                         "FT_PCT","OREB","DREB","REB","AST","STL","BLK",
                                         "PF","PTS","PLUS_MINUS"]]
        NBA_game_team_stats.columns = ['game_id','team_id','wl','min','fgm','fga','fg_pct',
                                       'fg3m','fg3a','fg3_pct','ftm','fta','ft_pct','oreb',
                                       'dreb','reb','ast','stl','blk','pf','pts','plus_minus']
        
        # Prepare NBA_Games data
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
        print("nba_games sample:")
        print(nba_games.head(5))
        return nba_games, NBA_game_team_stats
    else:
        return None, None

#Processing The games
def process_games():
    most_recent_date = date_of_most_recent_game_in_db()
    if most_recent_date is None:
        print("No recent game date found.")
        return
    beginning_date = most_recent_date + timedelta(days=1)
    # Only proceed if there are games after the most recent date (i.e. beginning_date < today)
    if beginning_date < date.today():
        nba_games, team_stats = games(beginning_date)
        insert_data(nba_games, "nba_games_s")
        insert_data(team_stats, "nba_games_team_stats_s")
    else:
        print("No new games to process.")

#Selectting game ids of missing boxscores
def select_boxscore_games():
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
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error in select_boxscore_games: {e}")

#Getting boxscores data and wrangling
def player_boxscores(game_ids):
    if game_ids.size != 0:
        boxscores_final = []
        for game_id in game_ids:
            try:
                box = boxscore.BoxScore(game_id)
                data = box.game.get_dict()
            except requests.exceptions.Timeout:
                print("Timeout error for game:", game_id)
                data = {}
            # Home team processing
            home = pd.json_normalize(data.get('homeTeam', {}).get('players', []))
            if 'statistics' in home.columns:
                stats = pd.json_normalize(home['statistics'])
                home = home.drop(columns=['statistics']).join(stats)
            home['HomeOrAway'] = 'Home'
            home.columns = home.columns.str.replace('statistics.', '')
            # Away team processing
            away = pd.json_normalize(data.get('awayTeam', {}).get('players', []))
            if 'statistics' in away.columns:
                stats = pd.json_normalize(away['statistics'])
                away = away.drop(columns=['statistics']).join(stats)
            away['HomeOrAway'] = 'Away'
            away.columns = away.columns.str.replace('statistics.', '')
            game_boxscore = pd.concat([home, away], ignore_index=True)
            game_boxscore['GAME_ID'] = game_id
            boxscores_final.append(game_boxscore)
        all_boxscores = pd.concat(boxscores_final, ignore_index=True)
        all_boxscores = all_boxscores.drop(columns=['order','jerseyNum','oncourt','nameI','firstName','familyName','minutesCalculated','plus'])
        # Prepare injury report
        injury_report = all_boxscores[["GAME_ID", "personId", "notPlayingReason", "notPlayingDescription"]].copy()
        injury_report.columns = ['game_id', 'player_id', 'not_playing_reason', 'not_playing_description']
        injury_report['game_id'] = injury_report['game_id'].astype(str)
        injury_report['player_id'] = injury_report['player_id'].astype(str)
        # Prepare boxscore table
        boxscores = all_boxscores[[
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
        boxscores['game_id'] = boxscores['game_id'].astype(str)
        boxscores['player_id'] = boxscores['player_id'].astype(str)
        print("Boxscores sample:")
        print(boxscores.head(5))
        return boxscores, injury_report
    else:
        return None, None

# Inserting into boxscores
def process_boxscores():
    games_df = select_boxscore_games()
    if games_df is not None:
        game_ids = games_df['game_id'].unique()
        print("Processing boxscores for game IDs:", game_ids)
        boxscores, injury_report = player_boxscores(game_ids)
        active_players_df = select_active_players()
        if active_players_df is not None:
            # Filter for active players
            boxscores = boxscores[boxscores['player_id'].isin(active_players_df['player_id'])]
            injury_report = injury_report[injury_report['player_id'].isin(active_players_df['player_id'])]
        insert_data(boxscores, "boxscores_s")
        insert_data(injury_report, "injury_report_s")
    else:
        print("No boxscore games found.")

# Selecting missing player id, game ids bridge
def select_games_players():
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
            ) AND g.game_id IS NOT NULL AND b.player_id IS NOT NULL AND g.game_id <> '0062400001';
        """
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error in select_games_players: {e}")

# Inserting new game data
def process_new_player_games():
    data = select_games_players()
    insert_data(data, "game_players_s")

# Identifying play by play for missing (new) games
def read_playbyplay_games():
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
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error in read_playbyplay_games: {e}")

# Fetching and wrangling play by play data
def playbyplay_data(games):
    if games is not None and not games.empty:
        data_list = []
        for i, row in enumerate(games.itertuples(index=False), start=1):
            print(f"Processing play-by-play for game {row.game_id}")
            game_id = row.game_id
            try:
                pbp = PlayByPlayV2(game_id)
                data = pbp.get_data_frames()[0]
            except Exception as e:
                print(f"Error fetching play-by-play for game {game_id}: {e}")
                continue
            data = data.dropna(subset=['SCORE']).drop_duplicates(subset=['SCORE'])
            # Split SCORE into AWAY and HOME columns
            away_home = data['SCORE'].str.split('-', expand=True)
            away_home.columns = ['AWAY', 'HOME']
            data = data[['GAME_ID', 'PERIOD', 'PCTIMESTRING']]
            data = pd.concat([data, away_home], axis=1)
            row_df = pd.DataFrame([row._asdict()])
            data = data.merge(row_df, how='left', left_on='GAME_ID', right_on='game_id')
            data_list.append(data)
            sleep(1)
        playbyplayscore = pd.concat(data_list)
        playbyplayscore = playbyplayscore[['GAME_ID','PERIOD','PCTIMESTRING','HOME','AWAY']]
        playbyplayscore.columns = ['game_id','current_period','time_left','htm_pts','vtm_pts']
        playbyplayscore = playbyplayscore.drop_duplicates()
        print("Play-by-play sample:")
        print(playbyplayscore.head(5))
        return playbyplayscore
    else:
        return None

# Inserting new play by play data into db
def process_play_by_play_score():
    games = read_playbyplay_games()
    playbyplayscore = playbyplay_data(games)
    insert_data(playbyplayscore, "play_by_play_score_s")

# Identifying game ids with missing (new) game events
def read_game_events_data():
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
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error in read_game_events_data: {e}")

# Fetching and wrangling shotchart data
def fetch_shotchart_data(player_id, game_id, team_id):
    try:
        shotchart = ShotChartDetail(
            player_id=player_id,
            game_id_nullable=game_id,
            team_id=team_id,
            context_measure_simple='FGA'
        )
        fga = shotchart.shot_chart_detail.get_dict()
        print(f"Fetched shotchart for Player {player_id}, Game {game_id}")
        return fga
    except requests.exceptions.Timeout:
        print(f"Timeout for Player {player_id}, Game {game_id}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for Player {player_id}, Game {game_id}: {e}")
    except Exception as e:
        print(f"Unexpected error for Player {player_id}, Game {game_id}: {e}")
    return None

def get_shotchart_data(data):
    fga_final = []
    for _, row in data.iterrows():
        fga = fetch_shotchart_data(player_id=row['player_id'], game_id=row['game_id'], team_id=row['team_id'])
        if fga and "data" in fga and "headers" in fga:
            df = pd.DataFrame(fga["data"], columns=fga["headers"])
            fga_final.append(df)
        else:
            print(f"Skipping Player {row['player_id']}, Game {row['game_id']} due to missing data.")
        sleep(1)
    if fga_final:
        return pd.concat(fga_final, ignore_index=True)
    else:
        print("No shotchart data collected.")
        return pd.DataFrame()

# Only allowing existing locations
def exisitingLoc(data, shot):
    data = data[~data[['loc_x', 'loc_y']].apply(tuple, axis=1).isin(shot[['loc_x', 'loc_y']].apply(tuple, axis=1))]
    return data

# Normalizing game events
def normalization(data):
    if data is not None and not data.empty:

        # Normalize Game Events
        Game_events = data[['GAME_ID','GAME_EVENT_ID','PLAYER_ID','PERIOD','MINUTES_REMAINING',
                            'SECONDS_REMAINING','LOC_X','LOC_Y','SHOT_ATTEMPTED_FLAG','SHOT_MADE_FLAG']].copy()
        Game_events.columns = ['game_id','event_id','player_id','current_period','minutes_remaining',
                                 'seconds_remaining','loc_x','loc_y','shot_attempted','shot_made']
        Game_events['game_id'] = Game_events['game_id'].astype(str)
        Game_events['player_id'] = Game_events['player_id'].astype(str)
        Game_events['event_id'] = Game_events['event_id'].astype(str)
        print("Game Events sample:")
        print(Game_events.head(5))

        # Normalize Shot Descriptions
        Shot_descriptions = data[['LOC_X','LOC_Y','SHOT_DISTANCE','SHOT_TYPE','SHOT_ZONE_RANGE',
                                  'SHOT_ZONE_AREA','SHOT_ZONE_BASIC']].drop_duplicates().copy()
        Shot_descriptions.columns = ['loc_x','loc_y','shot_distance','shot_type','shot_zone_range','shot_zone_area','shot_zone_basic']
        print("Shot Descriptions sample:")
        print(Shot_descriptions.head(5))
        return Game_events, Shot_descriptions
    else:
        return None, None

# Identifying existing shot_descriptions
def select_shot_descriptions():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        )
        query = "SELECT s.loc_x, s.loc_y FROM nba24_25_s.shot_descriptions_s s;"
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error in select_shot_descriptions: {e}")

# Processing the game events (shot charts)
def process_game_events():

    data = read_game_events_data()
    if not data.empty:
        shot_data = get_shotchart_data(data)
        active_players_df = select_active_players()
        existing_shot_desc = select_shot_descriptions()
        Game_events, Shot_descriptions = normalization(shot_data)
        if active_players_df is not None and Game_events is not None:
            Game_events = Game_events[Game_events['player_id'].isin(active_players_df['player_id'])]
        if Shot_descriptions is not None and existing_shot_desc is not None:
            Shot_descriptions = exisitingLoc(Shot_descriptions, existing_shot_desc)
        insert_data(Game_events, "game_events_s")
        insert_data(Shot_descriptions, "shot_descriptions_s")
    else:
        print("No new Games for finding new game events")

# Airflow
default_args = {
    "start_date": datetime(2024, 3, 10),
    "catchup": False
}
# Setting up DAG for the entire pipeline
with DAG(
    "nba_full_etl_dag",
    schedule_interval="0 22 * * *",  
    default_args=default_args,
    catchup=False
) as dag:

    newgames_task = PythonOperator(
        task_id="process_games",
        python_callable=process_games
    )

    boxscores_task = PythonOperator(
        task_id="process_boxscores",
        python_callable=process_boxscores
    )

    new_player_games_task = PythonOperator(
        task_id="process_new_player_games",
        python_callable=process_new_player_games
    )

    play_by_play_task = PythonOperator(
        task_id="process_play_by_play_score",
        python_callable=process_play_by_play_score
    )

    game_events_task = PythonOperator(
        task_id="process_game_events",
        python_callable=process_game_events
    )
    # Ordering
    newgames_task >> boxscores_task >> new_player_games_task >> play_by_play_task >> game_events_task
