import sqlite3 as sql
import pandas as pd
from pathlib import Path
import time
from datetime import datetime

class CSVFilesystemBackend:
    def __init__(self, mount_point: str):
        '''
        Interfaces a folder with CSVs in it with a SQLite database backend.
        '''
        self.mount_point = Path(mount_point)
        
        # Check if we need to initialize the database
        db_path = Path(f'{mount_point}/.backend')
        if not db_path.exists():
            db_path.mkdir()

        self.db = sql.connect(self.mount_point / '.backend/database.db')
        self.c_time = time.time() # Creation time of the mount
        self.m_cache = {} # Modification cache (object -> last modified time)

        cursor = self.db.cursor()

        # Check for LastModified Table
        cursor.execute('''
            SELECT name FROM sqlite_master
            WHERE type="table" AND name="LastModified"
        ''')
        if cursor.fetchone() is None:
            # Create the last modified table if it doesn't exist
            cursor.execute('''
                CREATE TABLE LastModified (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    FileName VARCHAR(255), 
                    TimeStamp TIMESTAMP
                )''')
        
        # Make sure the LastModified table is caught up with the underlying files
        for csv_file in self.mount_point.iterdir():
            if csv_file.is_file() and csv_file.name.endswith('.csv'):
                # Need to check its status in the LastModified table
                filename = csv_file.name
                last_modified = csv_file.stat().st_mtime
                self.m_cache[filename] = last_modified

                # Check if the status matches
                cursor.execute('''
                    SELECT TimeStamp FROM LastModified 
                    WHERE FileName = ?''', (filename,))
                result = cursor.fetchone()

                if result is None:
                    # Table has no data for this file, insert it
                    cursor.execute('''
                        INSERT INTO LastModified
                        (FileName, TimeStamp)
                        VALUES (?, ?)''', (filename, last_modified))
                    self.sync_csv_to_db(csv_file) # Newest data needs to be sync'd
                elif result[0] < last_modified:
                    # Table has outdated data for this file, update it
                    cursor.execute('''
                        UPDATE LastModified
                        SET TimeStamp = ?
                        WHERE FileName = ?''', (last_modified, filename))
                    self.sync_csv_to_db(csv_file) # Database has potentially stale data
        self.db.commit()

    def sync_csv_to_db(self, csv_path):
        '''
        Sync an updated CSV file back to the data base.
        '''
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
    
        for encoding in encodings:
            try:
                # Try to decode the file using different encoding standards (in case it's a different encoding)
                df = pd.read_csv(csv_path, encoding=encoding)

                for col in df.select_dtypes(include=['float']):
                    if (df[col].dropna() % 1 == 0).all():
                        df[col] = df[col].astype('Int64')

                table_name = Path(csv_path).stem
                df.to_sql(table_name, self.db, if_exists='replace', index=False)
                return
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue

    def query(self, sql: str):
        '''
        Run an SQL query against the database.
        '''
        try:
            df = pd.read_sql_query(sql, self.db)
            
            for col in df.columns:
                if df[col].dtype == object: # Only process object columns
                    # Replace empty strings with pandas NA
                    df[col] = df[col].replace('', pd.NA)

                # Now attempt to convert numeric-looking columns
                # (ignore errors so strings that aren't numbers stay as they are)
                try: 
                    df[col] = pd.to_numeric(df[col])
                except:
                    continue

            # After conversion, cast float columns that are actually integers
            for col in df.select_dtypes(include=['float']):
                if (df[col].dropna() % 1 == 0).all(): # All are whole numbers
                    df[col] = df[col].astype('Int64')
            
            return df
        except:
            return None
        
class Typist:
    def __init__(self, schema: dict[str, type]=None):
        self.schema = {col: {'type': schema[col], 'inferred': False} for col in schema.keys()} if schema is not None else {}

    @staticmethod
    def _infer_number(col: pd.Series) -> tuple[pd.Series, type]:
        '''
        Try to convert a column into all numeric values.
        '''
        col = pd.to_numeric(col)
        if (col.dropna() % 1 == 0).all():
            col = col.astype('Int64')
            return col, int
        else:
            col = col.astype('Float64')
            return col, float
        
    @staticmethod
    def _infer_bool(col: pd.Series) -> tuple[pd.Series, type]:
        '''
        Try to convert a column into all boolean values.
        '''
        unique_vals = col.dropna().unique()
        if len(unique_vals) == 2:
            lookup = {
                'true': True,
                'false': False,
                'yes': True,
                'no': False,
                '1': True,
                '0': False,
                't': True,
                'f': False,
                'y': True,
                'n': False,
            }
            first, second = str(unique_vals[0]).lower(), str(unique_vals[1]).lower()
            if first in lookup and second in lookup:
                col = col.apply(lambda x: lookup[str(x).lower()] if not pd.isna(x) else False).astype(bool)
                return col, bool
            else:
                raise ValueError()

    @staticmethod    
    def _infer_date(col: pd.Series) -> tuple[pd.Series, type]:
        '''
        Try to convert a column into all datetime values.
        '''
        try:
            col = pd.to_datetime(col, format='%m/%d/%Y')
            return col, type(datetime)
        except:
            pass        

        try:
            col = pd.to_datetime(col, format='%m-%d-%Y')
            return col, type(datetime)
        except:
            pass

        try:
            col = pd.to_datetime(col, format='%Y-%m-%d')
            return col, type(datetime)
        except:
            pass       

        try:
            col = pd.to_datetime(col, format='%m/%d/%Y %H:%M:%S')
            return col, type(datetime)
        except:
            pass 

        try:
            col = pd.to_datetime(col, format='%m/%d/%Y %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass      

        try:
            col = pd.to_datetime(col, format='%m-%d-%Y %H:%M:%S')
            return col, type(datetime)
        except:
            pass          

        try:
            col = pd.to_datetime(col, format='%m-%d-%Y %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass    

        try:
            col = pd.to_datetime(col, format='%Y-%m-%d %H:%M:%S')
            return col, type(datetime)
        except:
            pass     

        try:
            col = pd.to_datetime(col, format='%Y-%m-%d %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass    

        try:
            col = pd.to_datetime(col, format='%Y-%m-%dT%H:%M:%S')
            return col, type(datetime)
        except:
            pass
        
        try:
            col = pd.to_datetime(col, format='%Y-%m-%dT%H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass

        raise ValueError()


    def infer_types(self, df: pd.DataFrame, columns: list[str]=None):
        inferred_types = {}
        
        columns = df.columns if columns is None else columns

        for col in columns:
            if col in self.schema:
                df[col] = df[col].astype(self.schema[col]['type'])
            else:
                self.schema[col] = {
                    'type': None,
                    'inferred': True,
                }

                if df[col].dtype == object: # Only process object columns
                    # Replace empty strings with pandas NA
                    df[col] = df[col].replace('', pd.NA)

                    # Attempt to convert to numeric-looking columns
                    try:
                        df[col], self.schema[col]['type'] = Typist._infer_number(df[col])
                        continue
                    except:
                        pass

                    # Attempt to convert to boolean-looking columns
                    try:
                        df[col], self.schema[col]['type'] = Typist._infer_bool(df[col])
                        continue
                    except:
                        pass

                    # Attempt to convert to datetime-looking columns
                    try:
                        df[col], self.schema[col]['type'] = Typist._infer_date(df[col])
                        continue
                    except:
                        pass

                    # Default to string
                    self.schema[col]['type'] = str

        return inferred_types


test = pd.DataFrame({
    'float_row': [10, 64, '', 58.14, 48.5, 46],
    'int_row': [1, 4, 6.0, 47, 58.0, ''],
    'bool_row': ['Y', 'N', 'Y', 'Y', 'N', ''],
    'date_row': ['04/05/2025', '12/12/2024', '', '11/14/1998', '04/20/2014', '05/23/1967'],
    'str_row': [1, 41.0, 'test', 45, 90.4, 5],
})

t = Typist(schema={'bool_row': str})
t.infer_types(test)
print(test)