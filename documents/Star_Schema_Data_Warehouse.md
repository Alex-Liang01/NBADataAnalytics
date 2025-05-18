# Star Schema Data Warehouse

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
