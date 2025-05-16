
# Standard python imports
import os
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
import matplotlib.pyplot as plt
import matplotlib.figure

# Typing imports
from typing import Annotated
from typing_extensions import TypedDict

# Langchain and LangGraph imports

from langchain_openai import ChatOpenAI
# from langchain_ollama import ChatOllama

from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, SystemMessage
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.graph import START, END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langgraph.errors import GraphRecursionError

#Streamlit import
import streamlit as st
# Config file for connecting to postgres database
import sqlconfig


# Loading keys for OpenAI/LangSmith
load_dotenv()


# Creating SQL alchemy engine
uri = f"postgresql+psycopg2://{sqlconfig.DB_USER}:{sqlconfig.DB_PASSWORD}@{sqlconfig.DB_HOST}:{sqlconfig.DB_PORT}/{sqlconfig.DB_NAME}"
engine =  create_engine(uri)
metadata = MetaData()
metadata.reflect(bind=engine, schema='nba24_25_s')

# Connecting db to LLM
db = SQLDatabase(engine=engine, metadata=metadata,schema='nba24_25_s',sample_rows_in_table_info=1)

# Loading llm
# llm=ChatOllama(model="llama3.1:8b", temperature=0)
llm=ChatOpenAI(model="gpt-4o", temperature=0) 

# State passed from node to node
class State(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]   # messages input/output
    table_names: str #Table names of database
    schema: str # Schema (Create statements of database)
    sql_query: str # Sql_query generated 
    sql_error_bool:bool # Does most recent sql_query contain an error?
    not_related: bool # Is the user query related to the db?
    sql_results:str # Results of sql_statement 
    python_df_code:str # Python code to create pandas df from sql_results
    python_df_object: pd.DataFrame # Dataframe object
    plot_requested_bool: str # Does the user ask for a plot?
    plot_python_code:str # Python plot code
    plot_object:plt.Figure # Plot object
    recursion: bool # Recursion error
    last_error: str # last error

# Input state that loads messages to main State dictonary above
class InputState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]



# Loading 4 main SQL tools
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

run_query_tool = tools[0]
run_query_node = ToolNode([run_query_tool], name="run_query")

get_schema_tool = tools[1]
get_schema_node = ToolNode([get_schema_tool], name="get_schema")

get_table_tool = tools[2]
get_table_node = ToolNode([get_table_tool], name="get_table")

query_check_tool = tools[3]
run_query_check_node = ToolNode([query_check_tool], name="run_query_check")



# Initial node reads in table names and schema
def read_database_metadata(state: State):

    #Tool metadata
    tool_call = {
        "name": "sql_db_list_tables",
        "args": {},
        "id": "1",
        "type": "tool_call",
    }

    # Calling tool for table names of db
    tool_message = get_table_tool.invoke(tool_call)
    table_list_str = tool_message.content
    
    #Storing string of table names to state
    state['table_names']=table_list_str
    
    # Setting up args for schema metadata
    info_args = {"table_names": table_list_str}
    
    #schema metadata
    tool_call = {
        "name":"sql_db_schema",
        "args":info_args,
        "id":"2",
        "type":"tool_call"
    }

    #Calling tool for schema of tables
    schema_message=get_schema_tool.invoke(tool_call)

    #Extracting all CREATE statements
    create_statements = re.findall(r"CREATE TABLE.*?\/", schema_message.content, re.DOTALL)

    #Storing all CREATE statements to state for next node
    state['schema']="\n".join(create_statements)

    state['schema']=schema_message
    return state

# Checks if prompt is related to db and generates SQL query
def init_sql_generator(state: State):
    # Main prompt for agent
    prompt=f""""
        Determine if user input "{state['messages'][0].content}" is related to a query on the database. 
        The details of the database is as follow: 
        Schema of the database: {state.get('schema')}
        If the user input is related to the database, create the singular SQL SELECT statement to answer the user's query with the following format.

        "SELECT colname from tablename;"

        If the user input is not related to the database. Do not write a SQL SELECT statement.     
    """

    messages=[
        # System message for behavior of agent
        SystemMessage(content=f""""You are an SQL expert designed to determine if a user input is related to a SQL database. Never use CTEs, use subqueries instead.
                      Never return DML statements (INSERT, UPDATE, DELETE, DROP etc.). Do not explain your thinking.
                      """),
        HumanMessage(content=f"{prompt}")
    ]

    # Calling agent
    response=llm.invoke(messages)
    content=response.content

    # Extracting SQL statement from response
    select_statements = re.search(r"SELECT.*?;", content, re.DOTALL)
    # Setting sql_query for next nodes
    if select_statements:
        sql_query=select_statements.group(0)
        state['sql_query']=sql_query  
    else:
        state['sql_query']=""
    return state

# Checks if sql_query is empty. # If empty, prints END signaling graph to end 
def not_related_to_db(state: State):
    if state.get('sql_query')=="":
        state['not_related']=True
    else:
        state['not_related']=False
    return state

# Ends graph if previous node resulted in END statement (User query not related to the DB)
def end_or_sql_query(state: State):
    if state.get('not_related')==True:
        return True
    else: 
        return False

# Checks if SQL query generated in sql_query is valid
def sql_query_checker(state: State):
    # Invoking tool for checking if sql_query is valid
    response=query_check_tool.invoke({"query": state.get('sql_query')})

    content=response
    # Extracting only the SQL SELECT statement
    select_statements = re.search(r"SELECT.*?;", content, re.DOTALL)

    # Setting sql_query for next nodes
    if select_statements:
        sql_query=select_statements.group(0)
        state['sql_query']=sql_query  
    else:
        state['sql_query']=""

    return state


# Executes SQL Query
def sql_query_executor(state: State):

    # Executes SQL Query using tool
    response=run_query_tool.invoke({"query": state.get('sql_query')})

    #Extracts error from tool
    error = re.search(r"(Error:\s*.*)", response, re.DOTALL)

    if error:
        # if error is the same as last error infinite loop (Goes to printer)
        if state.get('last_error'):
            if state.get('last_error')==error.group(1):
                state['recursion']=True
            else:
                state['last_error']=error.group(1)
                state['recursion']=False
        else:
            # Passes error to sql_query_fixer allowing for next agent to fix query based off error
            state['last_error']=error.group(1)
            state['recursion']=False
    else:
        # No error
        state['last_error']=None
        state['sql_results']=response
        state['recursion']=False
  
    return state

    
# Fixes SQL statement and loops back to sql_query_executor
def sql_query_fixer(state: State):
    # Prompt for fixing sql query
    prompt=f"""This sql query failed: {state.get('sql_query')}.  
               The error for the attempted sql query attempted is {state.get('last_error')}.   
               The schema of is: {state.get('schema')}.       
    """

    messages=[
        # System message for sql fixer agent
        SystemMessage(content=f""""You are an expert at SQL. You are tasked with fixing errors in SQL queries.
                       Only return the corrected SQL select statement.
                       Common errors is not joining tables correctly and trying to select from columns that are not in the table.
                       Other common errors is attempting to SELECT columns that are not aggregated or a part of the group by clause.
                       Other common errors is not using window functions and not using Common Table Expressions when it is more efficient.
                       Do not explain your thinking.                 
                       Do not print anything else."""),
        HumanMessage(content=f"{prompt}")
    ]

    # Invoking sql fixer agent
    response=llm.invoke(messages)
    content=response.content

    # Extracting SELECT statement from response
    select_statements = re.search(r"SELECT.*?;", content, re.DOTALL)

    # Storing SELECT statement back to sql_query for execution
    if select_statements:
        sql_query=select_statements.group(0)
        state['sql_query']=sql_query    

    return state

# Generates Python code for creating Pandas dataframe
def df_code_creator(state: State):
    prompt=f"""This is a sql query {state.get('sql_query')} with the following results stored in a variable called y {state.get('sql_results')}.  
               Give me the code to create the Pandas dataframe so that I can store the evaluated string.
               The format of the code should follow:
               pd.DataFrame(y, columns=['col1', 'col2',...])
               Do not wrap the Python code in ```Python ```        
    """

    messages=[
        SystemMessage(content=f""""You are an expert at understanding Pandas and SQL. 
                      You are tasked with creating Python code to create a Pandas dataframe given a SQL SELECT statement 
                      and the results of the SQL query as a list with tuples representing each row of the dataframe.
                      Make sure to give the columns of the pandas dataframe appropriate names.
                      Only return the Python code one liner to create the Pandas dataframe.
                      Do not explain your thinking.            
                      """),
        HumanMessage(content=f"{prompt}")
    ]
    
    # Invoking agent to create Python Pandas code and storing into state for next node
    response=llm.invoke(messages)
    content=response.content
    state['python_df_code']=content
    return state

# Creating dataframe object in Python and determining if a plot is needed.
def dataframe_init(state:State):

    try:
        # Executing list of sql_results as a variable for Pandas code
        y=eval(state.get('sql_results'))
    
        # Executing Python code to create Pandas dataframe and storing Pandas dataframe back to state 
        df=eval(state.get('python_df_code'))
        state['python_df_object']=df
 
        #No dataframe initialization error
        state['last_error']=None
        state['recursion']=False

        return state
    # Dataframe initilization error for creation of Pandas Dataframe.     
    except Exception as E:
        if (str(state.get('last_error'))==str(E)):
            state['recursion']=True
        else:
            state['last_error']=E
            state['recursion']=False

        return state
    
# Node for checking if plot is requested
def plot_requested(state: State):
     # Prompt for determining if a plot needs to be made
    prompt=f"""This is the query that the user gave to you {state['messages'][0].content}.  
            If the prompt is related to creating a visualization of any kind print a statement about the type of plot the user wants.  
            If the prompt is not related to creating a plot print "END".
    """

    messages=[
        SystemMessage(content=f""""You are an agent that is an expert at understanding if the end user wants a dataframe or if they want a plot.
                    Do not explain your thinking. Do not print anything else.                 
                    """),
        HumanMessage(content=f"{prompt}")
    ]
        
    response=llm.invoke(messages)
    content=response.content
    
    # Storing if a plot is requested to state for next nodes
    state['plot_requested_bool']=content
    return state

# Returns True if there is an dataframe initialization error
def is_error(state: State):
    if state.get('recursion'):
        return "END"
    elif state.get('last_error'):
        return True       
    else:
        return False 
    
def dataframe_init_fixer(state: State):
    prompt=f"""  
            This is the previous python code that was used to initialize a dataframe {state.get('python_df_code')}.
            It resulted in the following error {state.get('last_error')}. 
            Fix the python code based off of the error. 
            Do not wrap the python code in ```python ``` as it needs to be executable. 
    """

    messages=[
        SystemMessage(content=f""""You are an agent that is an expert at python. The user has python code related to a prompt that results in an error. 
                    Return the corrected correct python code. Assume that the user has already defined the variable y that contains the list of tuples from a SQL query. 
                    So the contents of the dataframe to not need to be redefined only the pandas statement to create the dataframe using the list of tuples stored in y.
                    Do not explain your thinking. Do not print anything else.                 
                    """),
        HumanMessage(content=f"{prompt}")
    ]

    response=llm.invoke(messages)
    state['python_df_code']=response.content

    return state

# Ending condition if plot is not needed
def plot_or_end(state: State):
    if state.get('plot_requested_bool')=="END":
        return False
    else:
        return True
    



# Node for generating plots    
def plot_code_generator(state:State):
    # Prompt for creating Python plot code

    prompt=f"""You are a Python plotting assistant. Your task is to generate a Matplotlib plot based on the user's request "{state['messages'][0].content} using the provided Pandas DataFrame {state.get('python_df_object')} named df.
    Instructions:
    Interpret the user query and determine the appropriate plot type.
    Use the Matplotlib library to generate the plot.
    Use appropriate axis labels and a descriptive title based on the user query.
    Only return the code needed to generate the plot and assign the figure to y. Do not redefine the variable df.
    Example Output Format:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    # plotting code here using df
    y = fig
    Additional Notes:
    Ensure the code can run independently assuming df is already defined.
    Handle common plotting needs like setting axis labels and legends.
    Avoid interactive or inline-specific commands (like %matplotlib inline or plt.show()).
    Make sure that the python code will be executable with eval.
    """
    messages=[
        SystemMessage(content=f""""You are an agent that is an expert at creating Python code for plots using matplotlib. 
                      Do not explain your thinking. Provide the necessary imports for plotting as well as the python code to create the plot. Make sure that the labels are readable.
                      Do not wrap the code in ```Python ```.                
                     """),
        HumanMessage(content=f"{prompt}")
    ]

    response=llm.invoke(messages)
    

    # Storing Python plot code for execution in other nodes
    state['plot_python_code']=response.content

    return state

# Node for executing Python plot code
def plot_code_executor(state:State):
    try:
        #Storing the Pandas dataframe in variable df
        df=state.get('python_df_object')

        #Executing the Python plot code on df
        plot_code=state.get('plot_python_code')
        namespace={'df': df}
        exec(plot_code,globals(),namespace)
        fig=namespace.get("y")

        state['plot_object']=fig
        #Setting last_plot_error to if no errors occured
        state['last_error']=None

    except Exception as e:
        # Storing Python plot error to state for next nodes
        if str(state.get('last_error'))==str(e):
            state['recursion']=True
        else:    
            state['last_error']=e    
            state['recursion']=False            

    return state

# Node for fixing plot errors
def plot_error_fixer(state:State):
    # Prompts for plot code fixer agent
    prompt=f"""This is the query that the user gave to you {state['messages'][0].content}.  
               This is the dataframe that the user wanted to plot {state.get('python_df_object')}
               This is the previous Python plot code that resulted in an error {state.get('plot_python_code')}. 
               This is the error of the Python plot code {state.get('last_plot_error')}
               Fix the Python code to correctly handle the error.
               Return the Python code to create the plot that the user wants with the error fixed. Do not plt.show() and store the plot figure to a variable called y.               
    """

    messages=[
        SystemMessage(content=f""""You are an Python agent that is an expert at plotting using matplotlib. 
                      Fix any errors that were provided to you. 
                      Do not explain your thinking. Only provide the necessary imports for plotting as well as the python code to create the plot. 
                      Do not wrap the code in ```Python ```. Assign the plot (e.g., a matplotlib figure object) to a variable called y
                      The code should be a complete, valid Python code that can be executed in isolation. Do not plt.show() and store the plot figure to a variable called y.               
                     """),
        HumanMessage(content=f"{prompt}")
    ]

    # Returning python plot code back    
    response=llm.invoke(messages)
    state['plot_python_code']=response.content
    return state

# Final print
def printer(state: State):
    if state.get('not_related')==True:
        final_results=AIMessage(content=f"""Your query is not related to the database.""")
    elif state.get('recursion'):
        final_results=AIMessage(content=f"""Your query is related to the database but an error occured. Recheck your query.""")
    else:
        final_results=AIMessage(content=f"""This is the resulting SQL query: \n\n {state.get('sql_query')}""")

    state['messages']=[final_results]
    return state


builder = StateGraph(State, input=InputState)

# adding all the nodes.
builder.add_node("read_database_metadata", read_database_metadata)          ## Reads metadata of database
builder.add_node("init_sql_generator",init_sql_generator)                   ## Checks if query is related to database and creates sql query
builder.add_node("not_related_to_db",not_related_to_db)                     ## Checks if a SQL query was generated
builder.add_node("sql_query_checker",sql_query_checker)                     ## Checks syntax of SQL query
builder.add_node("sql_query_executor",sql_query_executor)                   ## Executes SQL query
builder.add_node("sql_query_fixer",sql_query_fixer)                         ## Fixes SQL query if sql_query_executor resulted in an error
builder.add_node("df_code_creator",df_code_creator)                         ## Creates Python code for creating dataframe from SQL results
builder.add_node("dataframe_init",dataframe_init)                           ## Initializes dataframe
builder.add_node("dataframe_init_fixer",dataframe_init_fixer)               ## Fixes dataframe init errors
builder.add_node("plot_code_generator",plot_code_generator)                 ## Generates python plot code
builder.add_node("plot_code_executor",plot_code_executor)                   ## Executes python plot code
builder.add_node("plot_error_fixer",plot_error_fixer)                       ## Fixes plot if plot_code_executor resulted in an error
builder.add_node("plot_requested",plot_requested)                           ## Checks if a plot was requested
builder.add_node("printer",printer)                                         ## Prints Results

# Adding all edges
builder.add_edge(START, "read_database_metadata")                           ## Start - > read_database_metadata
builder.add_edge("read_database_metadata","init_sql_generator")             ## read_database_metadata -> init_sql_generator
builder.add_edge("init_sql_generator", "not_related_to_db")                 ## init_sql_generator -> not_related_to_db

builder.add_conditional_edges("not_related_to_db",end_or_sql_query,{        ## Conditional split 
    True: "printer",                                                        ## not_related_to_db ----> printer     
    False: "sql_query_checker"                                              ## |
})                                                                          ## - - - -> sql_query_checker    
 
builder.add_edge("sql_query_checker","sql_query_executor")                  ## sql_query_checker -> sql_query_executor

builder.add_conditional_edges("sql_query_executor",is_error,{               ## sql_query_executor ----> sql_query_fixer
    True: "sql_query_fixer",                                                ## |-----> df_code_creator
    False: "df_code_creator",                                               ## | ------> printer
    "END": "printer"                                     
})                                                                        

builder.add_edge("sql_query_fixer","sql_query_checker")                     ## sql_query_fixer -> sql_query_checker
builder.add_edge("df_code_creator","dataframe_init")                        ## df_code_creator -> dataframe_init


builder.add_conditional_edges("dataframe_init",is_error, {                  ## dataframe_init ------> dataframe_init_fixer
        True: "dataframe_init_fixer",                                       ## | -----> plot_requested
        False: "plot_requested",                                            ## |
        "END": "printer"                                                    ## -----> printer
})                                                                         

builder.add_edge("dataframe_init_fixer","dataframe_init")                   ## dataframe_init_fixer -> dataframe_init


builder.add_conditional_edges("plot_requested",plot_or_end,{                ## Conditional split
    True: "plot_code_generator",                                            ## plot_requested -----> plot_code_generator
    False: "printer"                                                        ## |
})                                                                          ## -------> printer

builder.add_edge("plot_code_generator", "plot_code_executor")               ## plot_code_generator -> plot_code_executor

builder.add_conditional_edges("plot_code_executor",is_error,{               ## Condtional split
    True: "plot_error_fixer",                                               ## plot_code_executor -----> plot_error_fixer
    False: "printer",                                                       ## | 
    "END": "printer"                                                        ## --------> printer
})                                                                        

builder.add_edge("plot_error_fixer","plot_code_executor")                   ## plot_error_fixer ---> plot_code_executor

builder.add_edge("printer",END)                                             ## printer ---> END

react_graph = builder.compile()




st.title("LangGraph NBA SQL Agent")

st.markdown("""
    <style>
        [data-testid="stSidebar"] > div:first-child {
            display: flex;
            flex-direction: column;
            justify-content: center;
            height: 100vh;
        }
    </style>
""", unsafe_allow_html=True)
st.sidebar.header("Input")
text = st.sidebar.text_area("Ask me queries about the database.")

submit = st.sidebar.button("Submit")
if submit:
        try:
            messages=[HumanMessage(content=f"{text}")]
            response = react_graph.invoke({"messages": messages})
            st.write(f'This is your prompt \n"{text}"')

            # Getting and possibly printing generated SQL query code
            recursion=response.get("recursion")
            if recursion:
                st.write("The prompt is related to the database but there was an error.")
            else:
                # Getting and possibly printing generated SQL query code
                sql_query=response.get('sql_query')
                if sql_query:
                    st.write("SQL code generated to answer prompt")
                    st.code(sql_query,language="sql",wrap_lines=True)
                    
                    # Getting and possibly printing dataframe results from SQL query
                    df= response.get('python_df_object')
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        st.write("This is the resulting dataframe")
                        st.dataframe(df, use_container_width=True)

                    # Getting and possibly printing plot results and code to get the plot.
                    fig = response.get('plot_object')
                    if isinstance(fig, matplotlib.figure.Figure):
                        st.write("This is a plot of the dataframe using the matplotlib library in Python.")
                        st.pyplot(fig,use_container_width=True)
                        python_plot_code=response.get("plot_python_code")
                        python_df_code=response.get("python_df_code")
                        if python_df_code:
                            st.write("Python Code used to initialize the dataframe")
                            st.code(python_df_code,language="python",wrap_lines=True)
                        if python_plot_code:
                            st.write("Python Code used to Create the Plot")
                            st.code(python_plot_code,language="python",wrap_lines=True)

                # If the query is not related to the database    
                else:
                    st.write("Your prompt wasn't related to the database.")
                
               
        except RecursionError:
            print("Recursion Error")