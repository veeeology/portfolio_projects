import os, re
import pandas as pd
import numpy as np
import SQL_python_integration.sql_RW as sql

# Initialize the sql_RW object for SQL input/output
sql_writer = sql.sql_RW(login_file='../logins/sqldb_owner_login.txt')
dirs = {}
data_dir = 'Data/'
# Walk through the directory structure and get all of the folders
for r, d, f in os.walk(data_dir):
    root = r.replace('\\', '/')
    dirs[root] = d
    
# Set the max length for nvarchar columns.  It seems like due to a bug in
# the ODBC driver or turbodbc, we can't have nvarchar columns with length >1600
# https://github.com/blue-yonder/turbodbc/issues/143
# We will split the columns into chunks of 1600 characters for now until
# I figure out a workaround for this bug.
nvarmax = 1600
    
# Iterate through each folder and extract the data and upload to SQL
for d in dirs.keys():
    files = [f for f in os.listdir(d) if f.endswith('csv')]
    for f in files:
        print(d, f)
        
        # Construct the table name based on the file name
        tbl_name = f.replace('.csv', '').replace(' ', '_')
        
        # If the table name starts with a digit (e.g., 2020_data.csv) add the
        # "y" prefix to indicate that it is a year and avoid errors in SQL
        if re.match('\d.*', tbl_name) != None:
            tbl_name = 'y' + tbl_name
        print(tbl_name)
        
        # Read in the CSV file
        df = pd.read_csv(d + '/' + f, encoding='utf-8')
        obj_cols = [c for c in df.columns if df.dtypes[c]=='object']
        
        # Break long nvarchar columns into smaller pieces
        for col in obj_cols:
            maxlen = np.nanmax(np.nanmax(df[col].astype(str).apply(len).values))
            i = 1
            # Iterate through and generate any needed overflow columns
            while i*nvarmax <= maxlen:
                endpt = min((i+1)*nvarmax, maxlen)
                df[col + '_overflow' + str(i)] = \
                    df[col].apply(lambda x: np.nan if len(str(x))<= i*nvarmax \
                                  else x[i*nvarmax:endpt])
                i += 1
            # Truncate the original column
            if maxlen >= nvarmax:
                df[col] = df[col].apply(lambda x: x if len(str(x)) <= nvarmax \
                                        else x[:nvarmax])
        sql_writer.write_to_sql(df, tbl_name, schema = 'cdp', \
                                write_mode = 'overwrite')
