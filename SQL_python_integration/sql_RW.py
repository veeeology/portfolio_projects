import pandas as pd
import numpy as np
import numpy.ma as ma
import turbodbc, re, time, datetime, copy

# =============================================================================
# sql_RW.py
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

class sql_RW():
    def __init__(self, **kwargs):
        '''
    -----------------------------------------------------------------------
    sql_RW(**kwargs)
    -----------------------------------------------------------------------
    Initializes the sql_RW object with the supplied login information.  The
    three ways of providing the connection information are described in the
    KWARGS section below:
    
    INPUTS:
    * None
    
    KWARGS:
    * login_file (str): Path to a text file that contains all of the needed
        information to construct an ODBC connection string.  The file 
        contains key/value pairs with the following fields:
         server_name: required
         db_name: required
         username: required for SQL server authentication, blank for integrated
         password: required for SQL server authentication, blank for integrated
         driver: optional, default is 'ODBC Driver 17 for SQL Server'
    * con_str (str): an ODBC connection string
    * dsn (str): The name of an ODBC data source configured through the
        ODBC data sources utility in Windows.
        '''
        # Check to make sure that the user provided some sort of connection
        # information
        if len(kwargs) == 0:
            print('Need to provide some type of login information in order',\
                  'to initialize the sql_RW object.  Options are:')
            print('"login_file", "con_str", or "dsn"')
        
        # Get the connection information from the kwargs
        login_file = kwargs.get('login_file', '')
        con_str = kwargs.get('con_str', '')
        self.dsn = kwargs.get('dsn', '')
        
        # For each different method, use the appropriate function to configure
        # the connection information.
        if login_file != '':
            self.__con_str = self.__get_connection_string(login_file)
        else:
            self.__con_str = con_str
        return
    
   
    
    
    def read_query(self, query):
        '''
    -----------------------------------------------------------------------
    read_query(query):
    -----------------------------------------------------------------------
    Given the text of a SQL query, executes it and returns the results in a
    pandas dataframe
    
    INPUTS:
    * query (str): Text containing a SQL query
    
    RETURNS:
    * df (dataframe): Pandas dataframe containing the result of the query
        '''
        con, cursor = self.__open_connection()
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
    
    
    def write_to_sql(self, df, sql_table_name, **kwargs):
        '''
    -----------------------------------------------------------------------
    write_to_sql(df, sql_table_name, **kwargs)
    -----------------------------------------------------------------------
    Utility function that allows for flexible writing to SQL.  Will
    automatically check if the table exists in the database, and if it does
    not, it will attempt to create it.  If the table does exist, it will 
    check to see if all columns are present, and if there are any missing 
    it will attempt to add them.  It will also check to ensure that the
    dtypes of the Pandas Dataframe are compatible with the table in SQL 
    prior to writing.
    
    The user can specify a number of rules for update/append behavior.  If
    the kwarg 'write_mode' = 'skip' or 'update', 'ID_cols' needs to be 
    provided so that the function can check for duplicate rows in the 
    database.
    
    INPUTS:
    * df (dataframe): Pandas dataframe containing the data to be written to
        the database.
    * sql_table_name (str): the name of the destination table in SQL
    
    KWARGS:
    * schema (str): The schema for the sql_table_name in the database
    * ID_cols (list or iterable): List of columns that represent the 
        primary keys for the table.  Default = []
    * verbose (bool): Print output to the console.  Default = True.
    * write_mode (str): Determines the behavior when writing data to the
        database. Default = "append".  Options are:
            "append" - simply writes the additional rows to the databse
                without checking for duplicates.
            "overwrite" - deletes all existing rows and loads the entire
                contents of the current dataframe.  (truncate/load)
            "update" - checks the primary keys in the database table against
                those in the current dataframe.  Appends any new rows and
                updates rows with matching primary keys.  Requires the
                ID_cols kwarg.
            "skip" - checks the primary keys in the database table against
                those in the current dataframe.  Appends any new rows, but
                does not modify the rows with matching primary keys.
                Requires the ID_cols kwarg
    * batch_size = the number of rows to include in each commit batch. 
        Default is 5000.
    
    RETURNS:
    * None
        '''
        # Get the kwargs for the function.
        schema = kwargs.get('schema', '')
        ID_cols = kwargs.get('ID_cols', [])
        write_mode = kwargs.get('write_mode', 'append').lower()
        if write_mode not in ['update', 'append', 'skip', 'overwrite']:
            print('"', write_mode, '" is not a valid option for write_mode.',\
                  ' Defaulting to "append".')
            write_mode = 'append'
        batch_size = kwargs.get('batch_size', 5000)
        verbose = kwargs.get('verbose', True)
        
        # ---------------------------------------------------------------------
        # 1. Check that the table exists and that the dtypes are consistent
        # ---------------------------------------------------------------------
        
        # First, check to make sure that the table exists and that all of the 
        # necessary columns are present
        if verbose:
            t00 = time.clock()
            t0 = time.clock()
            print('Checking for consistency with the table in the database')
            
        self.__check_sql_table(df, sql_table_name, **kwargs)
        new_df = self.__correct_types(df, sql_table_name, **kwargs)
    
        # ---------------------------------------------------------------------
        # 2. Split data into the sections that need to be inserted or updated
        # ---------------------------------------------------------------------
        if verbose:
            print('    Elapsed:', int((time.clock()-t0)*1000), 'ms')
            t0 = time.clock()
            print('Checking for matching records in the database')
        
        # Construct the full table name
        if schema == '':
            full_table_name = '[' + sql_table_name + ']'
        else:
            full_table_name = '[' + schema + '].[' + sql_table_name + ']'
        
        # Set up the sql statements needed to insert and update
        insert_stmnt = self.__insert_statement(new_df.columns, full_table_name)
        if len(ID_cols)> 0:
            update_stmnt = self.__update_statement(new_df.columns, ID_cols, \
                                                   full_table_name)
        
        # Set up the tables for update vs insert operations according to the 'if_duplicated' option
        if write_mode in ('append', 'overwrite') or len(ID_cols)==0:
            data_insert = new_df
            data_update = pd.DataFrame()
        else:
            # Get the index columns from the table in the database
            query = 'SELECT [' + ID_cols[0] + ']'
            for i in range(1, len(ID_cols)):
                query = query + ', [' + ID_cols[i] + ']'
            query = query + ' FROM ' + full_table_name
            ids_from_sql = self.read_query(query)
            
            # Create the list of indices to update (already in the database)
            if write_mode == 'update':
                data_update = pd.merge(new_df, ids_from_sql, how='inner' \
                                       , on=ID_cols)
                
            # If using the "skip" option, don't update any data
            else:
                data_update = pd.DataFrame()
                
            # Get the list of indices that is not present in the database
            data_insert = pd.merge(new_df, ids_from_sql, how='outer'\
                                   , on=ID_cols, indicator=True)\
                                .query('_merge == "left_only"')\
                                .drop(['_merge'], axis=1)
                               
        # ---------------------------------------------------------------------
        # 3. Execute the insert, update, delete transactions as needed
        # ---------------------------------------------------------------------
        if verbose:
            print('    Elapsed:', int((time.clock()-t0)*1000), 'ms')
            t0 = time.clock()
            print('Writing to the database')
        
        if write_mode == 'overwrite':
            self.clear_table(sql_table_name, **kwargs)
    
        # Arrange the tables for output to sql
        if len(data_insert) > 0:
            self.__batch_write_data(data_insert, insert_stmnt, batch_size, verbose)
            
        if len(data_update) > 0:
            batch_start = 0
            update_cols = [col for col in data_update.columns if col not in ID_cols]
            # Do this to rearrange the column order so that the ID cols are
            # at the end of the list when they are processed.
            update_cols.extend(ID_cols)
            self.__batch_write_data(data_update[update_cols], update_stmnt, \
                                    batch_size, verbose)
        
        if verbose:
            print('Total Time Elapsed:', int(time.clock()-t00), 'seconds')
        
        return
    
    def create_table(self, df, table_name, **kwargs):
        '''
    ----------------------------------------------------------------------
    create_table(df, table_name, **kwargs)
    ----------------------------------------------------------------------
    Creates a table in the database given the schema of the supplied
    dataframe.  If the user account does not have sufficient privileges to
    create the table, prints the "CREATE TABLE" script to the screen and
    provides a 30-second timer so that the user can copy/paste it into SSMS
    and execute it if they are logged into an account with sufficient
    permissions.
    
    INPUTS:
    * sql_table_name (str): The name of the table to be created
    * df (dataframe): The current data to be written to the database
    
    KWARGS:
    * schema (str): The name of the database schema where the table should 
        be created.
    * ID_cols (list or iterable): The columns that should be used as
        primary keys when the table is created.
    
    RETURNS:
    * None
        '''
        # Get the keyword args
        ID_cols= kwargs.get('ID_cols', [])
        if type(ID_cols) == str:
            ID_cols = [ID_cols]
        schema = kwargs.get('schema', '')
        
        # Determine the correct size for the nvarchar columns - take the
        # maximum length of the data in the current column and add 20%
        obj_cols = [c for c in df.columns if df.dtypes[c]=='object']
        col_lengths = dict()
        for col in obj_cols:
            col_len = np.nanmax(df[col].astype(str).apply(len).values)
            col_len = int(col_len*1.2)
            if col_len > 4000:
                col_lengths[col] = 'MAX'
            else:
                col_lengths[col] = str(col_len)

        # Begin constructing the create table query
        if schema == '':
            query = 'CREATE TABLE [' + table_name + '](\n'
        else:
            query = 'CREATE TABLE [' + schema + '].[' + table_name + '](\n'
        for c in df.columns:
            col_type = str(df.dtypes[c])
            if 'int64' in col_type:
                query += '[' + c +  '] bigint, '
            elif 'int' in col_type:
                query += '[' + c +  '] int, '
            elif 'float' in col_type:
                query += '[' + c +  '] float, '
            elif 'datetime' in col_type:
                query += '[' + c +  '] datetime, '
            elif 'object' in col_type:
                query += '[' + c +  '] nvarchar(' + col_lengths[c] + '), '
            elif 'bool' in col_type:
                query += + c + ' int, '
            else:
                print('error adding column "' + c + '."  Unrecognized type: ' + col_type)
            # If a column is one of the index columns, make it not nullable
            if c in ID_cols:
                query = query[0:-2] + ' NOT NULL, '
            query = query + '\n'
        
        # If a primary key is specified, add it to the create table syntax
        if len(ID_cols) > 0:
            query = query + 'PRIMARY KEY('    
            for col in ID_cols:
                query += '[' + col + '], '
            query = query[:-2] + ') \n'
            
        query = query[:-2] + ')'
        con, cursor = self.__open_connection()
        cursor.execute(query)
        con.close()
        return


    def clear_table(self, sql_table_name, **kwargs):
        '''
    -----------------------------------------------------------------------
    clear_table(self, sql_table_name, **kwargs)
    -----------------------------------------------------------------------
    Clears the contents from the selected sql_table_name, similar to a 
    TRUNCATE operation.  If a condition string is provided, will clear only
    the rows matching the condition.
    
    INPUTS:
    * sql_table_name (str): The table to be cleared.
    
    KWARGS:
    * schema (str): The schema of the selected table.
    * condition (str): string containing the logic that would normally be
        included in a WHERE clause.  For example to clear all rows in a 
        table before today's date, you might pass 
            condition='[status_date] < GETDATE()'
    
    RETURNS:
    *None
        '''
        schema = kwargs.get('schema', '')
        condition = kwargs.get('condition', '')
        
        # Construct the full table name
        if schema == '':
            full_table_name = '[' + sql_table_name + ']'
        else:
            full_table_name = '[' + schema + '].[' + sql_table_name + ']'
        
        con, cursor = self.__open_connection()
        cursor = con.cursor()
        
        if condition=='':
            cursor.execute('DELETE FROM ' + full_table_name)
        else:
            cursor.execute('DELETE FROM ' + full_table_name + ' WHERE ' + condition)
        con.commit()
        con.close()
        return
    
    def __open_connection(self):
        '''
    -----------------------------------------------------------------------
    __open_connection()
    -----------------------------------------------------------------------
    Utility function that uses the object's saved connection settings to 
    open up a connection and cursor to use for executing SQL transactions.
    Relies on the turbodbc library 
    (https://turbodbc.readthedocs.io/en/latest/index.html)
    
    RETURNS:
    * con = a turbodbc connection object
    * cursor a turbodbc cursor based on the connection
        '''
        if self.dsn != '':
            con = turbodbc.connect(dsn=self.dsn)
        elif self.__con_str != '':
            con = turbodbc.connect(connection_string=self.__con_str)
        else:
            print('Cannot connect.  Need to provide valid connection information.')
            return (None, None)
        con.autocommit=True
        cursor = con.cursor()
        
        return (con, cursor)

    
    def __get_connection_string(self, login_file):
        '''
    -----------------------------------------------------------------------
    get_connection_file(login_file):
    -----------------------------------------------------------------------
    Helper method to construct an ODBC connection string based on the 
    information provided in a login file.  The file is a simple text file
    with key/value pairs.  The fields are described below:
        server_name: required
        db_name: required
        username: required for SQL server authentication, blank for integrated
        password: required for SQL server authentication, blank for integrated
        driver: optional, default is 'ODBC Driver 17 for SQL Server'
    INPUTS:
    * login_file (str): path to a text file containing the login information
        described above
    
    RETURNS:
    * con_str (str): the ODBC connection string containing the information 
        provided in the file.
        '''
        try:
            fin = open(login_file, 'r')
        except FileNotFoundError:
            print('login_file "' + login_file + '" not found.')
            return ''
        
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
    
    def __check_sql_table(self, df, sql_table_name, **kwargs):
        '''
        -----------------------------------------------------------------------
        check_sql_table(df, sql_table_name, **kwargs)
        -----------------------------------------------------------------------
        Prior to writing data to the database, checks that the destination
        table exists, and if it does not, calls the create_table method to 
        either create the table or generate the CREATE script for the user.
        Also checks that all columns needed are present in the destination
        table, and if they do not, either alters the table to add the needed
        columns, or generates the ALTER TABLE script for the user.
        
        INPUTS:
        * df (dataframe): Pandas dataframe containing the data to be written to
            the database.
        * sql_table_name (str): The name of the destination table in SQL
        
        KWARGS:
        * schema (str): The schema for the selected table in SQL
        * ID_cols (list or iterable)
        
        RETURNS:
        * None
        '''
        # Get the kwargs
        ID_cols = kwargs.get('ID_cols', [])
        schema = kwargs.get('schema', '')
        
        # Construct the full table name
        if schema == '':
            full_table_name = '[' + sql_table_name + ']'
        else:
            full_table_name = '[' + schema + '].[' + sql_table_name + ']'
        
        # Check to see if the table exists in the databse
        query = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE ' +\
                'TABLE_NAME=\'' + sql_table_name + '\''
        if schema != '':
            query += ' AND TABLE_SCHEMA = \'' + schema + '\''

        # Get the list of columns that are present in the table
        sql_cols = self.read_query(query)['COLUMN_NAME'].values
        
        if len(sql_cols) == 0:
            # Create the table if it does not appear in sql
            print('table: "' + sql_table_name + '" does not exist.  ' + \
                  'Creating the table.')
            self.create_table(df, sql_table_name, **kwargs)
            sql_cols = [c.lower() for c in df.columns]
        
        else:
            sql_cols = [c.lower() for c in sql_cols]
        
        # Determine which columns are already present and which need to be
        # added.  Cast the column names in lower-case to ensure that a 
        # duplicate column is not missed due to differing capitalization of the
        # same spelling
        df_cols = [c.lower() for c in df.columns]
        new_cols = list(set(df_cols).difference(set(sql_cols)))
        
        # Create the connection for writing the table    
        con, cursor = self.__open_connection()
        cursor = con.cursor()
        
        if len(new_cols) > 0:
            for col in new_cols:
                # Get the original column name from the table
                col_corrected = [c for c in df.columns if c.lower()==col][0]
                col_type = str(df.dtypes[col_corrected])
                
                # Create a query to add columns of the correct name and type
                print('Adding column: "' + col_corrected + '" to "' + sql_table_name)
                query = 'ALTER TABLE ' + full_table_name + ' ADD '
                if 'int' in col_type:
                    query = query + '[' + col_corrected + '] bigint'
                elif 'float' in col_type:
                    query = query + '[' + col_corrected + '] float'
                elif 'datetime' in col_type:
                    query = query + '[' + col_corrected + '] datetime'
                elif 'object' in col_type:
                    # Determine the correct size for the nvarchar columns - 
                    # take the maximum length of the data in the current column
                    # and add 20%
                    max_len = np.nanmax(df[col].astype(str).apply(len).values)
                    col_len = int(max_len*1.2)
                    if col_len > 4000:
                        col_len = 'MAX'
                    else:
                        col_len = str(col_len)
                    query = query + '[' + col_corrected + '] nvarchar (' \
                        + col_len + ')'
                elif 'bool' in col_type:
                    query = query + '[' + col_corrected + '] int'
                else:
                    print('error adding column "' + col_corrected + \
                          '."  Unrecognized type: ' + col_type)
                print(query)
                cursor.execute(query)
        con.close()
        return


    def __correct_types(self, df, sql_table_name, **kwargs):
        '''correct_types(df, sql_table_name, **kwargs):
        Changes the types in the table to be written to ensure compatibility 
        with the types of the destination table in SQL.
        
        INPUTS:
        * df: Pandas dataframe to be written to SQL
        * sql_name_name (str): The name of the destination table in SQL
        
        KWARGS:
        * schema (str): The schema for the sql_table_name in the database
        
        RETURNS:
        * df_corrected: Copy of the original pandas dataframe with the dtypes
            corrected to match the SQL data table.
        '''
        
        # Make a copy of the dataframe to avoid interfering with the original.
        new_table =copy.copy(df)
        
        schema = kwargs.get('schema', '')
        
        # Query the information_schema table to get the list of types in the
        # destimation table
        type_query = 'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS ' \
                      + 'WHERE TABLE_NAME = \'' + sql_table_name + '\''
        if schema != '':
            type_query += ' AND TABLE_SCHEMA=\'' + schema + '\''
    
        sql_cols = self.read_query(type_query)
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
        sql_cols_corr = {col.lower(): type_transform[sql_cols[col]] \
                         for col in sql_cols.keys()}
        
        table_types = new_table.dtypes.to_dict()
        for col in table_types.keys():
            if table_types[col] != sql_cols_corr[col.lower()]:
                if verbose:
                    print('converting', col, 'from', table_types[col], 'to', \
                          sql_cols_corr[col.lower()])
                try:
                    new_table[col] = new_table[col].astype(sql_cols_corr[col.lower()])
                except:
                    # Pandas does not accept nulls in int columns, so try 
                    # converting to float if there is an error
                    if 'int' in sql_cols_corr[col.lower()]:
                        new_table[col] = new_table[col].astype('float')
                        
                    else:
                        print('Error converting type of column', col)
                        return pd.DataFrame()
        return new_table



   


# # =================================================================================================
# # batch_write_data(data, statement, con, batch_size, verbose=False)
# # =================================================================================================
# #
# # Helper function to split a data table into batches and commit each separately.  Should only be
# # called from write_to_sql
# # 
# # Inputs: 
# # * data = dataframe to be written to sql. 
# # * statement = the SQL statement to be executed with the turbodbc.executemany 
# #                function.
# #               e.g, INSERT INTO TABLE (col1, col2) VALUES (?,?)
# # * con = open turbodbc connection
# # * batch_size = the size of the batches to split the data in for each commit.
# # 
# # Returns: None
# #
# # =============================================================================
    
    def __batch_write_data(self, df, statement, batch_size, verbose=True):
        '''
    -----------------------------------------------------------------------
    __batch_write_data(df, statement, batch_size, verbose=True)
    -----------------------------------------------------------------------
    Breaks down a dataset into manageable chunks to send to the turbodbc
    executemanycolumns command, and executes the transactions.
    
    INPUTS:
    * df (dataframe): Final dataset to be written to the database
    * statement (str): The generic update or insert statement generated by 
        __insert_statement or __update_statement.
    * batch_size (int): The number of rows to send to each transaction
    * verbose (bool): Print output to the console.  Default = True.
    
    RETURNS:
    * None.
        '''
        batch_start = 0
        con, cursor = self.__open_connection()
        while batch_start < len(df):
            t0 = time.clock()
            batch_end = min(batch_start + batch_size, len(df))
            df_batch = df.iloc[batch_start:batch_end]
            text_cols = [c for c in df_batch.columns \
                         if df_batch.dtypes[c]=='object']
                
            # Convert the data into a numpy masked array with the mask
            # indicating which are null values
            df_batch = [ma.array(df_batch[col].values, mask=df_batch[col].isnull().values) \
                          if col not in text_cols else \
                          ma.array(df_batch[col].fillna(value='').values, \
                                    mask=df_batch[col].isnull().values)\
                          for col in df_batch.columns]
                
            # Execute the transaction on the current batch
            cursor.executemanycolumns(statement, df_batch)
            
            if verbose:
                if 'INSERT' in statement:
                    print(int((time.clock()-t0)*1000), 'ms to insert',\
                          batch_end-batch_start, 'rows')
                elif 'UPDATE' in statement:
                    print(int((time.clock()-t0)*1000), 'ms to update', \
                          batch_end-batch_start, 'rows')
            batch_start = batch_start + batch_size
        return
    

    def __insert_statement(self, cols, table_name):
        ''' 
        -----------------------------------------------------------------------
        insert_statement(cols, table_name)
        -----------------------------------------------------------------------
        Creates the text for an insert statement given a set of column names
        and the specified table.
        
        INPUTS:
        * cols (list or iterable): list of column names to be used in the
            INSERT statement
        * table_name (str): The full name of the table in sql
            e.g, [schema].[tablename].
        
        RETURNS:
        * insert_text(str): String containing the INSERT statement.
        '''
        insert_text ='INSERT INTO ' + table_name + ' ([' + cols[0] + ']'
        for i in range(1, len(cols)):
            insert_text = insert_text + ', [' + cols[i] + ']'
        insert_text = insert_text + ') VALUES (?' + ',?'*(len(cols)-1) + ')'
        return insert_text


    def __update_statement(self, cols, ID_cols, table_name):
        ''' 
        -----------------------------------------------------------------------
        __update_statement(cols, ID_cols, table_name)
        -----------------------------------------------------------------------
        Creates the text for an update statement given a set of column names,
        the list of primary key columns and the specified table.
        
        INPUTS:
        * cols (list or iterable): list of column names to be used in the
            UPDATE statement
        * ID_cols (list or iterable): List of columns that represent the
            primary key for the table
        * table_name (str): The full name of the table in sql
            e.g, [schema].[tablename].
        
        RETURNS:
        * update_text(str): String containing the UPDATE statement.
        '''
        data_cols = [c for c in cols if c not in ID_cols]
        update_text = 'UPDATE ' + table_name + ' SET [' \
                    + data_cols[0] + ']=?'
        for i in range(1, len(data_cols)):
            update_text = update_text + ', [' + data_cols[i] + ']=?'
        update_text = update_text + ' WHERE ([' + ID_cols[0] + ']=?)'
        for i in range(1, len(ID_cols)):
            update_text = update_text + ' AND ([' + ID_cols[i] + ']=?)'
        return update_text
