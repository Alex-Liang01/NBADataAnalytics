# NBADataAnalytics

## Introduction
The objective of this project was to investigate and create a dashboard to visualize player perfomance and important metrics in a NBA game. To address this objective, the NBA API was used to fetch data on game details such as boxscores, injury reports, shot chart details, and more. The fetched data was then extracted, transformed and loaded into a database in SQL that is normalized using Boyce Codd Normal Form (BCNF). Through the normalized database, a Tableau dashboard was created to visualize player performance on the court for each game in the 2024-2025 season.  

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

<img width="1032" alt="image" src="https://github.com/user-attachments/assets/d6ec2170-a21c-4cc8-920f-24bb8826143c" />

The details of each table in the schema is as follows:

ACTIVE_NBA_PLAYERS: Active Players in the NBA  
NBATEAMS: Teams in the NBA  
NBA_TEAM_CITY: City each NBA team is from  
NBA_TEAM_STATES: State each NBA team is from  
NBA_GAMES: Games in the NBA Season 2024 - 25  
NBA_GAMES_HOME_VISITING_TEAM: Identifies home and visiting team for each game  
NBA_GAMES_TEAM_STATS: Performance of home and visiting team in a game  
BOXSCORES: Boxscores of each game  
INJURY_REPORT: Injury report of each game  
PLAY_BY_PLAY_SCORE: Tracks score by time for each game  
GAME_EVENTS:  Shot attempts in each game  
SHOT_DISTANCE: Description table of shot distance for each location on the court  
SHOT_TYPE: Description table of shot type for each location on the court  
SHOT_ZONE_RANGE: Description table of shot zone range for each location on the court  
SHOT_ZONE_AREA: Description table of shot zone area for each location on the court  
SHOT_ZONE_BASIC: Description table of shot zone basic area for each location on the court  


