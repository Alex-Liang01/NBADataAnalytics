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

## SQL Agent 

To accelerate the process of getting insights from this database, a SQL agent was created using LangGraph with streamlit as a front end in order to process natural language queries to get insights on the data. Below are some example of some uses cases of the SQL_Agent

## SQL Agent Example Uses

---

The users query is **"Visualize LeBron James's shooting averages from different shot zone areas on the court and different shot zone ranges."**  

![LeBron_averages](https://github.com/user-attachments/assets/340e67c0-fc50-461d-a0fb-d11a9792f48f)

As observed in the gif, the correct SQL query was generated that resulted in a Pandas dataframe and matplotlib plot with the Python code that was used to generate the dataframe and plot.   

---
The users query is **"Give me a visualization of the top 20 players who scored the most points summed up. I want their full names and the amount of points they scored with a proper scale for their points."**  

![image](https://github.com/user-attachments/assets/affca137-c0d6-4e5f-9a4f-bac84ecbc64c)

As observed, it gave the correct SQL query, properly combining the player's first and last name to group the boxscores by player full names to find the players that scored the most points. It properly created a scale for the plot with the points as requested with it ranging from 0 to the max of Shai Gilgeous-Alexander's 2526 points. Though this plot doesn't say much as some player's played more games than other players resulting in more points.

---
The users query is  **""Give me a visualization of the top 20 players who averaged the most amount of points. Give me their full names and their average points with a proper scale.""*

![image](https://github.com/user-attachments/assets/57268a2c-c1a1-4ffe-bbff-01059cf072ef)

As observed, the SQL agent correctly gets the average points per game of the players. The results are quite similar to the most points showing that most players at the all star level are quite consistent.

## LangGraph Directed Graph
---

This is the LangGraph directed graph that represents the steps that the SQL Agent takes.

<img width="1000" alt="image" src="https://github.com/user-attachments/assets/9e4df4f7-fac1-4720-8b6b-a8f142739150" />  

---
  
**1.** It first gets the schema of the available tables in the NBA star schema data warehouse that it is connected to **(read_database_metadata)**.  

**2.** Then it checks if the user query is related to the database and generates a sql query to satisfy the user's question **(init_sql_generator && not_related_to_db)**  

**3.** If the user query is not related to the database, it ends with a print **(Printer)**  

**4.** If the user query is related to the database, it checks the generated SQL query for errors **(sql_query_checker)**  

**5.** It then executes the SQL statement generated which may end in an error **(sql_query_executor)**    

**6.** If there is an error it loops back to fix the query and tries to execute it again **(sql_query_fixer -> sql_query_checker -> sql_query_executor)**   

**7.** If there is no error, it then creates Python statements using llms to initialize the results of the SQL data in a Pandas dataframe **(df_code_creator -> dataframe_init)**  

**8.** If there is an error in the initilization of the Pandas dataframe, it loops to fix the error **(datframe_init -> dataframe_init_fixer -> dataframe_init)**  

**9.** Then it checks if a plot is requested **(plot_requested)**.  

**10.** If no plot is requested, it prints the results of the Pandas dataframe **(printer)**   

**11.** If a plot is requested, it generates and executes Python code using a llm for plots using matplotlib **(plot_code_generator -> plot_code_executor)**  

**12.** If there is an error in creating the Python code for plotting, it takes the error, using a llm in a loop to fix the error **(plot_code_executor -> plot_error_fixer -> plot_code_executor)**  

**13.** It then prints all the results of the SQL query, the Pandas dataframe and the plot **(printer)**  

In any of the loops for fixing SQL query errors, initializing Pandas dataframes, and creating matplotlib plots, if the error of the initilization is the same as the previous error, the agent breaks out of the loop in order to prevent infinite looping.
