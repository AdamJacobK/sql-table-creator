import re
import pandas as pd
import numpy as np

def slugify(string):
    """
    Create a slug from a free form string.
    
    PARAMETERS
    ----------
    string : str
        String to create the slug from
    
    RETURNS
    -------
    string : str
        Slug.
    
    """  
    
    if string:    
        string = string.strip() 
        string = re.sub('\.', '', string)
        string = re.sub('\s+', '_', string)
        string = re.sub('[^\w.-]', '', string)
        return string.strip('_.- ').lower()


def create_table_sql_script(df, table_name=None, pkey=None, varchar_max=False, infer_nulls=True, overwrite=None) :
    """
    Takes a DataFrame and outputs a basic SQL script CREATE TABLE statement. This 
    statement can then be copied and run in SQL to generate the table.
    
    This function supports pandas 'int', 'uint', 'float', 'category', and 
    'object' data types.
    
    Supported SQL types are:
        - BIT
        - SMALLINT
        - INT
        - BIGINT
        - VARCHAR
        - TEXT
        - NUMERIC
    
    PARAMETERS
    ----------
    df : DataFrame
        DataFrame to be supplied to the function.
    table_name : str, default None
        Specified table name.
    pkey : str, default None
        Specify the column name for the primary key column. This column
        must not contain any missing values.
    varchar_max : bool, default False
        Whether to set all pandas 'object'/'category' columns to 
        the maximum size. This will set the SQL type to 'TEXT'.
    infer_nulls : bool, default True
        When True, the function scans all columns to identify those
        with null values. The column then adds the 'NOT NULL' constraint
        to those columns in the SQL script.
    overwrite : dict, default None
        Dictionary in the form {'column_name':'column_sql_data_type'}. 
        The `overwrite` parameter can be used to overwrite the inferred
        data types from this function.
    
    
    RETURNS
    -------
    statement : str
        The statement to run in SQL to create the table. The function can
        be wrapped in a `print()` function in order to display the output
        in a readable way.
    
    """
    class PrimaryKeyNullError(Exception):   
        def __init__(self, data):    
            self.data = data
        def __str__(self):
            return repr(self.data)
    
    # Ensure that each column has a unique name
    if len(df.columns.unique()) < len(df.columns) :
        raise Exception('The columns in the supplied DataFrame must have unique names.')
    
    if infer_nulls not in [True, False] :
        raise TypeError('The parameter `infer_nulls` must be set to either True or False.')
     
    
    # pandas column names, and pandas column types
    column_names = df.dtypes.to_dict().keys()
    column_types = df.dtypes.to_dict().values()
    
    sql_col_types, sql_col_names = [], []
    
    # Perform string formatting on each column name
    sql_col_names = [slugify(column) for column in column_names]
    
    # Create list of columns with no null values.
    not_null_columns = df.columns[df.notnull().all()].tolist()
    not_null_columns = [slugify(column) for column in not_null_columns]
    
    # Determine column SQL datatypes from the pandas datatypes
    for col_index, dtype in enumerate(column_types) :
        
        # String columns
        if dtype.name in ['object', 'category'] :
            col_length = df.iloc[:, col_index].str.len().max()
            if varchar_max is True :
                sql_col_types.append('TEXT')
            else :
                sql_col_types.append('VARCHAR({})'.format(int(col_length)))
        
        # Integer columns
        elif dtype.name in ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'] :
            if df.iloc[:, col_index].min() == 0 and df.iloc[:, col_index].max() == 1 :
                sql_col_types.append('BIT')
            elif dtype.name in ['int8', 'int16', 'uint8'] :
                sql_col_types.append('SMALLINT')
            elif dtype.name in ['int32', 'uint16'] :
                sql_col_types.append('INT')
            elif dtype.name in ['int64', 'uint32'] :
                sql_col_types.append('BIGINT')
            else :
                sql_col_types.append('NUMERIC')
        
        # Float columns
        elif dtype.name in ['float8', 'float16', 'float32', 'float64'] :
            sql_col_types.append('NUMERIC')
    
    
    # Primary Key
    if pkey is not None :
        if type(pkey) == str :
            if slugify(pkey) in sql_col_names :
                if slugify(pkey) in not_null_columns :
                    pkey_col_index = sql_col_names.index(slugify(pkey))
                    sql_col_types[pkey_col_index] = sql_col_types[pkey_col_index] + ' PRIMARY KEY'
                else :
                    raise PrimaryKeyNullError('The `pkey` column specified must not contain any null values.')
            else :
                raise ValueError('The column specified in `pkey` must exist in the DataFrame.')
        else:
            raise TypeError('The `pkey` parameter must be a string.')
    
    
    # Non-Null columns
    if infer_nulls == True :
        if pkey is not None :
            not_null_columns.remove(slugify(pkey))
        for column in not_null_columns :
            not_null_column_index = sql_col_names.index(slugify(column))
            sql_col_types[not_null_column_index] = sql_col_types[not_null_column_index] + ' NOT NULL'
    
    
    # Overwrite specific column SQL datatypes and add in any constraints
    if overwrite is not None :
        for column, sql_type in overwrite.items() :
            if slugify(column) in sql_col_names :
                sql_col_types[sql_col_names.index(slugify(column))] = sql_type
            else:
                raise ValueError('An invalid column name was specified in the `overwrite` parameter dictionary. Ensure that each column name is \n spelled correctly.')
    
    
    # Create SQL script
    if table_name :
        statement = 'CREATE TABLE {} ( \n{} {}'.format(table_name, sql_col_names[0], sql_col_types[0])
    else :
        statement = 'CREATE TABLE no_name ( \n{} {}'.format(sql_col_names[0], sql_col_types[0])
    
    for i in range(1, len(sql_col_names)) :
        statement = (statement + '\n, {} {}').format(sql_col_names[i], sql_col_types[i])
    statement = statement + '\n);'
    
    return statement
