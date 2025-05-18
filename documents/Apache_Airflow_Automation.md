# Apache Airflow Automation

Apache Airflow was used in Docker to automate fetching of new data on the NBA so that new data can be added to the analysis easily.
The DAG is as follows:

![image](https://github.com/user-attachments/assets/039c6218-984c-4395-9feb-144b2ad98113)

1. New games and team stats are fetched and processed. (Fetches data for NBA_GAMES_S and NBA_GAMES_TEAM_STATS_S)
2. New boxscores for each new game are fetched and processed (FETCHES DATA FOR BOXSCORES_S and INJURY_REPORT_S)
3. New player and games combinations for the new games are fetched and processed (Fetches data for GAME_PLAYERS)
4. New play by play scores for the new games are fetched and processed (Fetches data for PLAY_BY_PLAY_SCORE_S)
5. New game events for the new games are fetched and processed. (Fetches data for GAME_EVENTS_S and SHOT_DESCRIPTIONS_S)

As there are no new data for ACTIVE_NBA_PLAYERS_S and NBA_TEAMS_S except for at the end of the season or when a player is traded, these are not included in the DAG.

