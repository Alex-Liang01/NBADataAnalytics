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
