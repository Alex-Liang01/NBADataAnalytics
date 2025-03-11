# NBADataAnalytics

## Introduction
The objective of this project was to investigate and create a dashboard to visualize player perfomance and important metrics in a NBA game. To address this objective, the NBA API was used to fetch data on game details such as boxscores, injury reports, shot chart details, and more. The fetched data was then extracted, transformed and loaded into a database in SQL that is optimal for performance using the star schema. Through the star schema, a Tableau dashboard was created to visualize player performance on the court for each game in the 2024-2025 season.  

## Data Cleaning and Wrangling

The NBA player data was fetched from multiple endpoints found in the NBA API. 

The endpoints that were used were as follows:

Playerindex: Player data  
Teams: Team data  
Leaguegamefinder: NBA Games in 2024-25  
Boxscore: Boxscore data  
ShotChartDetail: Shot chart details  
PlayByPlayV2: Play by play details  

The data from each of the endpoints was then wrangled into BCNF, allowing for efficient storage of the data through the prevention of redundancy while preventing any update,deletion, and insertion anomalies.

The resulting Entity Relation Diagram is as follows:  

![image](https://github.com/user-attachments/assets/5a7d16f3-d101-427b-ba9b-926046c195b7)


The details of each table in the schema is as follows:

ACTIVE_NBA_PLAYERS_S: Active Players in the NBA  
NBATEAMS_S: Teams in the NBA  
NBA_GAMES_S: Games in the NBA Season 2024 - 2025  
NBA_GAMES_TEAM_STATS_S: Performance of home and visiting team in a game  
BOXSCORES_S: Boxscores of each game  
INJURY_REPORT_S: Injury report of each game  
PLAY_BY_PLAY_SCORE_S: Tracks score by time for each game  
GAME_EVENTS_S:  Shot attempts in each game  
SHOT_DESCRIPTION_S: Description table of shot for each location on the court  
GAME_PLAYERS: Game/player Bridge table for boxscores to game_events

## Automation

Apache Airflow was then used to automate fetching of new data on the NBA so that new data can be added to the analysis easily.
The DAG is as follows:

![image](https://github.com/user-attachments/assets/039c6218-984c-4395-9feb-144b2ad98113)

1. New games and team stats are fetched and processed. (Fetches data for NBA_GAMES_S and NBA_GAMES_TEAM_STATS_S)
2. New boxscores for each new game are fetched and processed (FETCHES DATA FOR BOXSCORES_S and INJURY_REPORT_S)
3. New player and games combinations for the new games are fetched and processed (Fetches data for GAME_PLAYERS)
4. New play by play scores for the new games are fetched and processed (Fetches data for PLAY_BY_PLAY_SCORE_S)
5. New game events for the new games are fetched and processed. (Fetches data for GAME_EVENTS_S and SHOT_DESCRIPTIONS_S)

As there are no new data for ACTIVE_NBA_PLAYERS_S and NBA_TEAMS_S except for at the end of the season or when a player is traded, these are not included in the DAG.

## Tableau Visualization  
Using Tableau, dashbaords were created to visualize player performance on the court for each game in the 2024-2025 season wit filters so that different games can be observed.

The Dashboard is avaliable at 
https://public.tableau.com/app/profile/alex6894/viz/NBADashboard_17406906076290/ShotChartDetails

Below are some static snippets of the Dashboards for the 2024-11-30 PHX VS GSW Game.

Lead Tracker of the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/54045753-1921-4287-af35-7209dd57862b)

Shot Chart Details of Kevin Durant and Stephen Curry in the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/20d31adc-76c4-4c46-a51d-9b7d47be099b)


Shot Chart Densities of Kevin Durant and Stephen Curry in the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/7b788168-eb6a-48d2-acfd-5f3b7276a352)

FG Percentages of the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/67ebd9ce-a7d7-4bde-8738-b43458be2526)

Boxscores of the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/07948a8d-47c0-4e98-9211-f26d84621584)

Player Points, Assists, Rebound and Blocks of the 2024-11-30 PHX VS GSW Game  
![image](https://github.com/user-attachments/assets/f5bdc719-7df3-44e7-acf9-fda7b061bda5)  


![image](https://github.com/user-attachments/assets/063e90e6-5b65-41c5-8e89-f1f0107c1aba)

