# NBADataAnalytics

## Introduction
The objective of this project was to investigate and create a dashboard to visualize player perfomance and important metrics in a NBA game. To address this objective, the NBA API was used to fetch data on game details such as boxscores, injury reports, shot chart details, and more. The fetched data was then extracted, transformed and loaded into a database in SQL that is optimal for performance using the star schema. Through the star schema, a Tableau dashboard was created to visualize player performance on the court for each game in the 2024-2025 season. To accelerate the process of getting insights from this database, a SQL agent was created using LangGraph with streamlit as a front end in order to process natural language queries to get insights on the data. 

## Project Sections

- [LangGraph_SQL_Agent](documents/LangGraph_SQL_Agent.md)
- [Star Schema Data Warehouse](documents/Star_Schema_Data_Warehouse.md)
- [Tableau Dashboard](documents/Tableau_Dashboard.md)
- [Apache_Airflow_Automation](documents/Apache_Airflow_Automation.md)

## Tableau Dashboard and SQL Agent Streamlit App Links
Using Tableau, dashboards were created to visualize player performance on the court for each game in the 2024-2025 season with filters so that different games can be observed.

The Dashboard is avaliable at:   
https://public.tableau.com/app/profile/alex6894/viz/NBADashboard_17406906076290/ShotChartDetails

The Streamlit app for the SQL Agent using LLM is available at: 

## Entity Relation Diagram of the Star Schema Data Warehouse

![image](https://github.com/user-attachments/assets/5a7d16f3-d101-427b-ba9b-926046c195b7)

## SQL Agent Example

---

### Query with a plot
The users query is **"Visualize LeBron James's shooting averages from different shot zone areas on the court and different shot zone ranges."**  

![LeBron_averages](https://github.com/user-attachments/assets/340e67c0-fc50-461d-a0fb-d11a9792f48f)

As observed in the gif, the correct SQL query was generated that resulted in a Pandas dataframe and matplotlib plot with the Python code that was used to generate the dataframe and plot.   

