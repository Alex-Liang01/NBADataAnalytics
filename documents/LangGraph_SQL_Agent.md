# LangGraph SQL Agent

Using LangGraph, a SQL agent was created that has the capabilities to create and invoke SQL queries as well as create plots on the results using matplotlib in python. The SQL agent uses streamlit for the front end and uses an GPT-4o from Open AI as it's LLM.

The streamlit app for this SQL agent is available at:  

## LangGraph Directed Graph
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

## SQL Agent Results
These are some results on querying on the SQL agent.

---

### Query with a plot
The users query is **"Visualize LeBron James's shooting averages from different shot zone areas on the court and different shot zone ranges."**  

![LeBron_averages](https://github.com/user-attachments/assets/340e67c0-fc50-461d-a0fb-d11a9792f48f)

As observed in the gif, the correct SQL query was generated that resulted in a Pandas dataframe and matplotlib plot with the Python code that was used to generate the dataframe and plot.   

---

### Query without a plot  
The users query is "Who are the top 5 players that have scored the most points in the database summed up? I want their names and their points in a dataframe.  

<img width="1000" alt="image" src="https://github.com/user-attachments/assets/70e56f8b-a8b1-4975-a7b6-5aa523bb1070" />


As observed above, the correct SQL query was generated that resulted in a Pandas dataframe as the user did not request a visualization.  

---

### Unrelated Query  

The user's query is "What time is it?" which is not related to the database on the NBA.  

<img width="1000" alt="image" src="https://github.com/user-attachments/assets/a85d84b7-e45f-4ebe-8ced-7bb900be4f4c" />

As observed, when asking a query "What time is it?" that is unrelated to the NBA database provided, the agent responds that the user query is not related to the database.
