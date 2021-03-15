import pandas as pd
import numpy as np
import numpy.ma as ma
import turbodbc, re, time, datetime, copy

# =============================================================================
# sql_utils.py
# =============================================================================
#
# DESCRIPTION: Collection of utility functions using the turbodbc library to
# read/write data from a SQL server.  Provides extra functionality to control
# write modes beyond truncate and append; also allows an "update" mode where
# the user can specify the primary key columns for the table and it will 
# automatically check whether those primary key values are present in the 
# database and will construct insert/update statements
# for the rows that need it.  Formats the inputs and outputs of the read and
# write operations as Pandas Dataframes for convenience.
#
# Create a login file or set up a DSN to specify the connection.   Exclude the
# login file from online repositories using gitignore to avoid having 
# credentials saved in code.  Or, set up a DSN using the windows ODBC manager.
#
# USAGE:
#
# import sql_utils as sql
#
# # Set up the connection
# connection = sql.connection_string(login_file_name)
#
# # Read a table from the database using the supplied connection file
# df = sql.read_query('SELECT TOP 100 * FROM TABLE_NAME', con_str = connection)
#
# # Write the contents of the table to SQL.
# sql.write_to_sql(df, 'TABLE_NAME', con_str = connection, \
#                ID_cols = ['id'], if_duplicated='update')
#
#
# TO-DO:
# Refactor to object-oriented design - was originally written as a quick fix,
# but would be cleaner as its own module.
# =============================================================================








# =============================================================================
# connection_string(login_file)
# =============================================================================
#
# Sets up an ODBC connetion string from a file containing the database login info.
# Looks for a text file with the following keys and values:
#
# server_name: required
# db_name: required
# username: required for SQL server authentication, blank for integrated
# password: required for SQL server authentication, blank for integrated
# driver: optional, default is 'ODBC Driver 17 for SQL Server'
# 
# 
# Inputs: 
# * login_file = path to .txt file containing the login credentials for the db
# 
# Returns:
# * con_str = odbc connection string to access the specified database
#
# =============================================================================

def get_connection_string(login_file):
    fin = open(login_file, 'r')
    temp = fin.readlines()
    login = dict()
    for line in temp:
        line = line.split(':')
        key = line[0].strip()
        value = ''
        for i in range(1, len(line)):
            value = value + line[i].strip()
            if i<len(line)-1:
                value = value + ':'
        login[key] = value
        
    # Use the default driver if one is not specified
    if 'driver' not in login:
        login['driver'] = 'ODBC Driver 17 for SQL Server'
    
    # SQL Authentication
    if 'username' in login.keys() and 'password' in login.keys():
        con_str = 'DRIVER={' + login['driver'] + '};' \
                + 'SERVER=' + login['server_name'] + ';' \
                + 'DATABASE='+ login['db_name'] +';'\
                + 'UID='+ login['username'] +';'\
                + 'PWD='+ login['password'] +';'
                    
    elif 'Authentication' in login.keys():
        con_str = 'DRIVER={' + login['driver'] + '};' \
                + 'SERVER=' + login['server_name'] + ';' \
                + 'DATABASE='+ login['db_name'] +';'\
                + 'Authentication='+ login['Authentication'] +';'

    # Windows Authentication
    else:
        con_str = 'DRIVER={' + login['driver'] + '};' \
                + 'SERVER=' + login['server_name'] + ';' \
                + 'DATABASE='+ login['db_name'] +';'\
                + 'Trusted_Connection=yes;'
    return con_str


# =============================================================================
# read_query(query, con_string)
# =============================================================================
#
# Retrieves data based on the supplied query.  Returns the data in the form of
# a Pandas DataFrame.
# 
# Inputs: 
# * query = The query to be executed.
# * con_str = the ODBC connection string for the database
# * dsn = the data source name to use for the connection.  If a connection
#         string and DSN are both provided, the DSN will take precedence.
# 
# Returns:
# * df = pandas dataframe containing the data returned by the query.
#
# =============================================================================

def read_query(query, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return
    con.autocommit=True
    cursor = con.cursor()
    cursor.execute(query)
    table = cursor.fetchallnumpy()
    con.close()
    
    # Load the numpy arrays into a dataframe
    columns = [k for k in table.keys()]
    df_constructor = {}
    for c in columns:
        df_constructor[c] = table[c]
    df = pd.DataFrame(df_constructor)
    
    # Convert empty strings to null values
    if len(df) > 0:
        text_cols = [c for c in df.columns if df.dtypes[c]=='object']
        for col in text_cols:
            df.loc[(df[col]=='') | df[col]==None, col] = np.nan
    
    return df

# =============================================================================
# create_table(sql_table_name, data, con_string):
# =============================================================================
#
# Retrieves data based on the supplied query.  Returns the data in the form of 
# a Pandas DataFrame.
# 
# Inputs: 
# * sql_table_name = The name of the table to be created in the database.
# * data = pandas dataframe containing the data table that will be written to 
#           the database
# * con_str = the ODBC connection string for the database
# * id_cols = the columns to set as unique identifiers
# 
# Returns:
# * None
#
# =============================================================================
def create_table(data, sql_table_name, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    ID_cols = kwargs.get('ID_cols', [])
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return
    con.autocommit=True
    cursor = con.cursor()
    
    query = 'CREATE TABLE ' + sql_table_name + '('
    for c in data.columns:
        col_type = str(data.dtypes[c])
        if 'int64' in col_type:
            query = query + '[' + c +  '] bigint, '
        elif 'int' in col_type:
            query =query + '[' + c +  '] int, '
        elif 'float' in col_type:
            query = query + '[' + c +  '] real, '
        elif 'datetime' in col_type:
            query =query + '[' + c +  '] datetime, '
        elif 'object' in col_type:
            if c in ID_cols:
                query = query + '[' + c +  '] nvarchar(256), '
            else:
                query = query + '[' + c +  '] nvarchar(256), '
        elif 'bool' in col_type:
            query = query + c + ' int, '
        else:
            print('error adding column "' + c + '."  Unrecognized type: ' + col_type)
        # If a column is one of the index columns, make it not nullable
        if c in ID_cols:
            query = query[0:-2] + ' NOT NULL, '
        
    query = query[0:-2] + ')'
    print(query)
    cursor.execute(query)
    con.close()
    return


# =============================================================================
# clear_table(sql_table_name, data, **kwargs):
# =============================================================================
#
# Clears out the conents of a table in the database.  Acts similarly to 
#   TRUNCATE TABLE table_name
# but uses a delete statement, which is allowed under R/W access accounts.
# 
# Inputs: 
# * sql_table_name = The name of the table to be cleared in the database.
# 
# kwargs:
# * con_str = the ODBC connection string for the database
# * condition = string containing the logic when only a subset of the data 
#               should be cleared.
#               For example, if you had a column called [status_date] in your
#               table and you wanted to clear all data before today's date, you
#               would pass:
#               condition='[status_date] < GETDATE()'
# 
# Returns:
# * None
#
# =================================================================================================
def clear_table(sql_table_name, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    condition = kwargs.get('condition', '')
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return
    con.autocommit=True
    cursor = con.cursor()
    if condition=='':
        cursor.execute('DELETE FROM ' + sql_table_name + ' WHERE 1=1')
    else:
        cursor.execute('DELETE FROM ' + sql_table_name + ' WHERE ' + condition)
    con.commit()
    con.close()
    return

# =================================================================================================
# check_sql_table(table, sql_table_name, kwargs)
# =================================================================================================
#
# Prior to writing data, checks that the table "sql_table_name" exists, and if it does not, creates
# the table.  If the sql table does already exist, adds any new columns from the table that may be
# missing.
# 
# Inputs: 
# * table = dataframe to be written to sql. It is recommended to pass table.head(1) to this
#           function for processing speed.
# * sql_table_name = name of the table in sql where the data will be written
# 
# kwargs:
# * DSN = dsn name for odbc connection
# * con_str = odbc connection string.  If a DSN is also provided, it will take precedence.
# * ID_cols = list containg the names of the columns that should be treated as indexes for the
#             table
# 
# Returns: None
#
# =================================================================================================
    
def check_sql_table(data, sql_table_name, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    ID_cols = kwargs.get('ID_cols', [])
    
    # First, determine if the table exists
    try:
        sql_cols = read_query('SELECT TOP 1 * FROM ' + sql_table_name, **kwargs)
    except:
        # Create the table if it does not appear in sql
        print('table: "' + sql_table_name + '" does not exist.  Creating the table.')
        create_table(data, sql_table_name, **kwargs)
        sql_cols = data
    
    # Create the connection for writing the table    
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return
    # Determine which columns are already present and which need to be added.  Cast the column
    # names in lower-case to ensure that a duplicate column is not missed due to differing
    # capitalization of the same spelling
    sql_cols = [c.lower() for c in sql_cols.columns]
    data_cols = [c.lower() for c in data.columns]
    new_cols = list(set(data_cols).difference(set(sql_cols)))
    
    con.autocommit=True
    cursor = con.cursor()
    
    if len(new_cols) > 0:
        for col in new_cols:
            print([c for c in data.columns if c.lower() in new_cols])
            # Get the original column name from the table (not the lower-case version)
            col_corrected = [c for c in data.columns if c.lower()==col][0]
            col_type = str(data.dtypes[col_corrected])
            
            # Create a query to add columns of the correct name and type
            print('Adding column: "' + col_corrected + '" to "' + sql_table_name)
            query = 'ALTER TABLE ' + sql_table_name + ' ADD '
            if 'int' in col_type:
                query = query + '[' + col_corrected + '] bigint'
            elif 'float' in col_type:
                query = query + '[' + col_corrected + '] real'
            elif 'datetime' in col_type:
                query = query + '[' + col_corrected + '] datetime'
            elif 'object' in col_type:
                query = query + '[' + col_corrected + '] varchar (256)'
            elif 'bool' in col_type:
                query = query + '[' + col_corrected + '] int'
            else:
                print('error adding column "' + col_corrected + '."  Unrecognized type: ' + col_type)
            print(query)
            cursor.execute(query)
    con.close()
    return


# =================================================================================================
# correct_types(table, sql_table_name, **kwargs)
# =================================================================================================
#
# Accesses the table schema in the database and changes the column types in the pandas DataFrame
# (where possible) to match the types in the database.
# 
# Inputs: 
# * table = dataframe to be written to sql
# * sql_table_name = name of the table in sql where the data will eventually be written
# 
# kwargs:
# * DSN = dsn name for odbc connection
# * con_str = odbc connection string.  If a DSN is also provided, it will take precedence.
# * ID_cols = list containg the names of the columns that should be treated as indexes for the
#             table
# 
# Returns: new_table = dataframe with the types corrected to match the ones in the databse
#
# =================================================================================================
    
def correct_types(table, sql_table_name, **kwargs):
    new_table =copy.copy(table)
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    ID_cols = kwargs.get('ID_cols', [])
    verbose = kwargs.get('verbose', False)
    type_query = 'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS ' \
                 + 'WHERE TABLE_NAME = \''
    if '.' in sql_table_name:
        type_query = type_query + sql_table_name.split('.')[-1] + '\''
    else:
        type_query = type_query + sql_table_name + '\''

    sql_cols = read_query(type_query, **kwargs)
    sql_cols = sql_cols.set_index('COLUMN_NAME').to_dict()['DATA_TYPE']
    
    type_transform = {'nvarchar': 'object', \
                      'varchar': 'object', \
                      'text': 'object', \
                      'real': 'float', \
                      'decimal': 'float', \
                      'float': 'float', \
                      'int': 'int64', \
                      'int32': 'int64',\
                      'bit': 'int', \
                      'bigint': 'int64',\
                      'datetime': 'datetime64[ns]',\
                      'date': 'datetime64[ns]',\
                      'time': 'datetime64[ns]',\
                      'datetime2': 'datetime64[ns]',\
                      'tinyint': 'int64',\
                      'smallint': 'int64', \
                      'money': 'float'}
    sql_cols_corr = {col.upper(): type_transform[sql_cols[col]] for col in sql_cols.keys()}
    
    table_types = new_table.dtypes.to_dict()
    for col in table_types.keys():
        if table_types[col] != sql_cols_corr[col.upper()]:
            if verbose:
                print('converting', col, 'from', table_types[col], 'to', sql_cols_corr[col.upper()])
            try:
                new_table[col] = new_table[col].astype(sql_cols_corr[col.upper()])
            except:
                # Pandas does not accept nulls in int columns, so try converting to float if there
                # is an error
                if 'int' in sql_cols_corr[col.upper()]:
                    new_table[col] = new_table[col].astype('float')
                    
                else:
                    print('Error converting type of column', col)
                    return pd.DataFrame()
    return new_table


# =============================================================================
# write_to_sql(table, sql_table_name, **kwargs)
# =============================================================================
#
# Utility function that allows for flexible writing to SQL.  Will automatically
# check if the table exists in the database, and if it does not, it will 
# attempt to create it.  If the table does exist, it will check to see if all 
# columns are present, and if there are any missing it will attempt to add 
# them.  It will also check to ensure that they dtypes of the Pandas Dataframe
# are compatible with the table in SQL prior to writing.
#
# The user can specify a number of rules for update/append behavior.  If
# the kwarg 'if_duplicated' = 'skip' or 'update', 'ID_cols' needs to be 
# provided so that the
# function can check for duplicate rows in the database.
# 
# Inputs: 
# * table = dataframe to be written to sql.
# * sql_table_name = name of the table in sql where the data will be written
# 
# kwargs:
# * DSN = dsn name for odbc connection
# * con_str = odbc connection string.  If a DSN is also provided, it will take 
#               precedence.
# * ID_cols = list containg the names of the columns that should be treated as
#                indexes for the table
# * if_duplicated = option to use if the entry already exists in the table. 
#                   options are "skip", "append", "update".  Default is "skip"
# * batch_size = the number of rows to include in each commit batch. 
#                Default is 5000.
# * verbose = flag to specify whether to get detailed output on timing and 
#             performance.  Default is False.
# 
# Returns: None
#
# =============================================================================
    

def write_to_sql(table, sql_table_name, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    ID_cols = kwargs.get('ID_cols', [])
    if_duplicated = kwargs.get('if_duplicated', 'skip')
    if if_duplicated not in ['update', 'append', 'skip', 'overwrite']:
        print('"', if_duplicated, '" is not a valid option for kwarg "if_duplicated".',\
              '\nDefaulting to "skip".  Options are: "update", "append", "skip"')
        if_duplicated = 'skip'
    batch_size = kwargs.get('batch_size', 5000)
    verbose = kwargs.get('verbose', False)
    
    # First, check to make sure that the table exists and that all of the necessary columns are 
    # present
    if verbose:
        t00 = time.clock()
        t0 = time.clock()
        print('Checking for consistency with the table in the database')
        
    check_sql_table(table.head(1), sql_table_name, **kwargs)
    table = correct_types(table, sql_table_name, **kwargs)

    if verbose:
        print('    Elapsed:', int((time.clock()-t0)*1000), 'ms')
        t0 = time.clock()
        print('Creating insert and update tables')
    
    # Set up the sql statements needed to insert and update
    insert_stmnt = insert_statement(table.columns, sql_table_name)
    if len(ID_cols)> 0:
        update_stmnt = update_statement(table.columns, ID_cols, sql_table_name)
    
    # Set up the tables for update vs insert operations according to the 'if_duplicated' option
    if if_duplicated in ('append', 'overwrite') or len(ID_cols)==0:
        data_insert = table
        data_update = pd.DataFrame()
    else:
        # Get the index columns from the table in the database
        query = 'SELECT [' + ID_cols[0] + ']'
        for i in range(1, len(ID_cols)):
            query = query + ', [' + ID_cols[i] + ']'
        query = query + 'FROM ' + sql_table_name
        ids_from_sql = read_query(query, **kwargs)
        
        # Create the list of indices to update (already in the database)
        if if_duplicated == 'update':
            data_update = pd.merge(table, ids_from_sql, how='inner', on=ID_cols)
            
        # If using the "skip" option, don't update any data
        else:
            data_update = pd.DataFrame()
            
        # Get the list of indices that is not present in the database - new data.
        data_insert = pd.merge(table, ids_from_sql, how='outer', on=ID_cols, indicator=True)\
                           .query('_merge == "left_only"')\
                           .drop(['_merge'], axis=1)
                           
#    data_insert = correct_types(data_insert, sql_table_name, **kwargs)
#    print(data_insert['API'].head(5))
#    data_update = correct_types(data_update, sql_table_name, **kwargs)
#    print(data_update['API'].head(5))
    
    if verbose:
        print('    Elapsed:', int((time.clock()-t0)*1000), 'ms')
        t0 = time.clock()
        print('Writing to the database')

    # Set up the connection to the database
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return

    if if_duplicated == 'overwrite':
        clear_table(sql_table_name, **kwargs)

    # Arrange the tables for output to sql
    if len(data_insert) > 0:
        batch_write_data(data_insert, insert_stmnt, con, batch_size, verbose)
        
    if len(data_update) > 0:
        batch_start = 0
        update_cols = [col for col in data_update.columns if col not in ID_cols]
        update_cols.extend(ID_cols)
        batch_write_data(data_update[update_cols], update_stmnt, con, batch_size, verbose)

    con.close()
    
    if verbose:
        print('Total Time Elapsed:', int(time.clock()-t00), 'seconds')
    
    return


# =================================================================================================
# batch_write_data(data, statement, con, batch_size, verbose=False)
# =================================================================================================
#
# Helper function to split a data table into batches and commit each separately.  Should only be
# called from write_to_sql
# 
# Inputs: 
# * data = dataframe to be written to sql. 
# * statement = the SQL statement to be executed with the turbodbc.executemany 
#                function.
#               e.g, INSERT INTO TABLE (col1, col2) VALUES (?,?)
# * con = open turbodbc connection
# * batch_size = the size of the batches to split the data in for each commit.
# 
# Returns: None
#
# =============================================================================
    
def batch_write_data(data, statement, con, batch_size, verbose=False):
    batch_start = 0
    cursor = con.cursor()
    while batch_start < len(data):
        t0 = time.clock()
        if batch_start + batch_size > len(data):
               batch_end = len(data)
        else:
           batch_end = batch_start + batch_size    
        data_batch = data.iloc[batch_start:batch_end]
        text_cols = [c for c in data_batch.columns if data_batch.dtypes[c]=='object']
        data_batch = [ma.array(data_batch[col].values, mask=data_batch[col].isnull().values) \
                      if col not in text_cols else \
                      ma.array(data_batch[col].fillna(value='').values, \
                               mask=data_batch[col].isnull().values)\
                      for col in data_batch.columns]
        cursor.executemanycolumns(statement, data_batch)
        con.commit()
        if verbose:
            if 'INSERT' in statement:
                print(int((time.clock()-t0)*1000), 'ms to insert', batch_end-batch_start, 'rows')
            elif 'UPDATE' in statement:
                print(int((time.clock()-t0)*1000), 'ms to update', batch_end-batch_start, 'rows')
        batch_start = batch_start + batch_size
    return
    
# =============================================================================
# insert_statement(cols, sql_table_name)
# =============================================================================
#
# Helper function to write_to_sql.  Builds the insert statement for a given 
# table and column set.
# 
# Inputs: 
# * cols = List of columns to use for the insert statement
# * sql_table_name = the name of the table to be written to in the database.
# 
# Returns: 
# * query = string containing the insert statement to use with 
#           turbodbc.executemanycolumns
#
# =============================================================================
    
def insert_statement(cols, sql_table_name):
    query ='INSERT INTO ' + sql_table_name + ' ([' + cols[0] + ']'
    for i in range(1, len(cols)):
        query = query + ', [' + cols[i] + ']'
    query = query + ') VALUES (?' + ',?'*(len(cols)-1) + ')'
    return query


# =============================================================================
# update_statement(cols, sql_table_name)
# =============================================================================
#
# Helper function to write_to_sql.  Builds the insert statement for a given 
# table and column set.
# 
# Inputs: 
# * cols = List of columns to use for the insert statement
# * sql_table_name = the name of the table to be written to in the database.
# 
# Returns: 
# * query = string containing the insert statement to use with 
#           turbodbc.executemanycolumns
#
# =============================================================================

def update_statement(cols, ID_cols, sql_table_name):
    data_cols = [c for c in cols if c not in ID_cols]
    query = 'UPDATE ' + sql_table_name + ' SET [' \
                + data_cols[0] + ']=?'
    for i in range(1, len(data_cols)):
        query = query + ', [' + data_cols[i] + ']=?'
    query = query + ' WHERE ([' + ID_cols[0] + ']=?)'
    for i in range(1, len(ID_cols)):
        query = query + ' AND ([' + ID_cols[i] + ']=?)'
    return query

def add_to_log_table(log_table_name, updated_table_name, **kwargs):
    DSN = kwargs.get('dsn', '')
    con_str = kwargs.get('con_str', '')
    if DSN != '':
        con = turbodbc.connect(dsn=DSN)
    elif con_str != '':
        con = turbodbc.connect(connection_string=con_str)
    else:
        print('Cannot connect.  Need to provide either a dsn or a connection string through the '+\
              '"dsn" or "con_str" kwargs.')
        return
    insert_stmnt = 'INSERT INTO ' + log_table_name + \
        ' ([table_name]) VALUES (\'' + updated_table_name + '\')'
    con.autocommit=True
    cursor = con.cursor()
    cursor.execute(insert_stmnt)
    con.close()
    
    return
