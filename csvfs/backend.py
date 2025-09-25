import sqlite3 as sql
import pandas as pd
from pathlib import Path
import time
from datetime import datetime
import json

class Typist:
    def __init__(self, schema: dict[str, type]=None):
        self.schema = {col: {'type': schema[col], 'inferred': False} for col in schema.keys()} if schema is not None else {}

    @staticmethod
    def _infer_number(col: pd.Series) -> tuple[pd.Series, type]:
        '''
        Try to convert a column into all numeric values.
        '''
        col = pd.to_numeric(col.dropna())
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
        tmp = col.dropna()
        try:
            col = pd.to_datetime(tmp, format='%m/%d/%Y')
            return col, type(datetime)
        except:
            pass        

        try:
            col = pd.to_datetime(tmp, format='%m-%d-%Y')
            return col, type(datetime)
        except:
            pass

        try:
            col = pd.to_datetime(tmp, format='%Y-%m-%d')
            return col, type(datetime)
        except:
            pass       

        try:
            col = pd.to_datetime(tmp, format='%m/%d/%Y %H:%M:%S')
            return col, type(datetime)
        except:
            pass 

        try:
            col = pd.to_datetime(tmp, format='%m/%d/%Y %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass      

        try:
            col = pd.to_datetime(tmp, format='%m-%d-%Y %H:%M:%S')
            return col, type(datetime)
        except:
            pass          

        try:
            col = pd.to_datetime(tmp, format='%m-%d-%Y %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass    

        try:
            col = pd.to_datetime(tmp, format='%Y-%m-%d %H:%M:%S')
            return col, type(datetime)
        except:
            pass     

        try:
            col = pd.to_datetime(tmp, format='%Y-%m-%d %H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass    

        try:
            col = pd.to_datetime(tmp, format='%Y-%m-%dT%H:%M:%S')
            return col, type(datetime)
        except:
            pass
        
        try:
            col = pd.to_datetime(tmp, format='%Y-%m-%dT%H:%M:%S.%f')
            return col, type(datetime)
        except:
            pass

        raise ValueError()

    def __call__(self, df: pd.DataFrame, columns: list[str]=None):
        
        columns = df.columns if columns is None else columns

        for col in columns:
            if col in self.schema:
                df[col] = df[col].astype(self.schema[col]['type'])
            else:
                self.schema[col] = {
                    'type': None,
                    'inferred': True,
                }

                # Replace empty strings with pandas NA
                df[col] = df[col].replace('', pd.NA)

                # Attempt to convert to boolean-looking columns
                try:
                    df[col], self.schema[col]['type'] = Typist._infer_bool(df[col])
                    continue
                except:
                    pass

                # Attempt to convert to numeric-looking columns
                try:
                    df[col], self.schema[col]['type'] = Typist._infer_number(df[col])
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

        return df

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
        self.typists = {} # Typists for queries

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
        
        # Check for existing schemas
        if (self.mount_point / '.backend/schema.json').exists():
            self._load_typists()

        # Check for schema files
        for schema_file in self.mount_point.iterdir():
            if schema_file.is_file() and schema_file.name.endswith('.csv.schema'):
                # Need to check its status in the LastModified table
                filename = schema_file.name
                csv_file = schema_file.stem
                last_modified = schema_file.stat().st_mtime
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
                    self.m_cache[csv_file] = 0 # Indicates a refresh is needed
                elif result[0] < last_modified:
                    # Table has outdated data for this file, update it
                    cursor.execute('''
                        UPDATE LastModified
                        SET TimeStamp = ?
                        WHERE FileName = ?''', (last_modified, filename))
                    self.m_cache[csv_file] = 0 # Indicates a refresh is needed

                table_name = Path(csv_file).stem
                if self.m_cache[csv_file] == 0 or table_name not in self.typists:
                    # Parse out any schema information from the schema file
                    schema_text = schema_file.read_text()
                    schema_lookup = {
                        'INT': int,
                        'FLOAT': float,
                        'BOOL': bool,
                        'DATE': type(datetime),
                        'STR': str,
                    }
                    schema = {
                        column: schema_lookup[column_type]
                        for row in schema_text.splitlines() if row.strip()
                        for column, column_type in [row.split(':', 1)]
                    }

                    # Set the typists for each csv_file
                    self.typists[table_name] = Typist(schema=schema)

        # Make sure the LastModified table is caught up with the underlying files
        for csv_file in self.mount_point.iterdir():
            if csv_file.is_file() and csv_file.name.endswith('.csv'):
                # Need to check its status in the LastModified table
                filename = csv_file.name
                last_modified = csv_file.stat().st_mtime

                # If the modication is already set, pass it
                if filename not in self.m_cache:
                    self.m_cache[filename] = last_modified

                # If there is no typist, create a default typist
                if csv_file.stem not in self.typists:
                    self.typists[csv_file.stem] = Typist()

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
                    self.m_cache[filename] = 0 # Newest data needs to be sync'd
                elif result[0] < last_modified:
                    # Table has outdated data for this file, update it
                    cursor.execute('''
                        UPDATE LastModified
                        SET TimeStamp = ?
                        WHERE FileName = ?''', (last_modified, filename))
                    self.m_cache[filename] = 0 # Database has potentially stale data
                
                # A zero'd m_cache entry signals an update is needed 
                if self.m_cache[csv_file.name] == 0:
                    self.sync_csv_to_db(csv_file)

        # Save the typists and the modification information
        self._save_typists()
        self.db.commit()

    def _load_typists(self):
        '''
        Load typist information from the schema file.
        '''
        lookup = {
            'int': int,
            'float': float,
            'bool': bool,
            'date': type(datetime),
            'str': str,
        }

        # Parse the types from the schema file
        schema_file = self.mount_point / '.backend/schema.json'
        schemas = json.loads(schema_file.read_text())
        for table in schemas:
            schema = schemas[table]
            for column in schema:
                # Each column needs to point to a type instead of a str
                schema[column]['type'] = lookup[schema[column]['type']]

            # Update the typists
            self.typists[table] = Typist()
            self.typists[table].schema = schema

    def _save_typists(self):
        '''
        Save typist information to the schema file.
        '''
        lookup = {
            int: 'int',
            float: 'float',
            bool: 'bool',
            type(datetime): 'date',
            str: 'str',
        }

        # Update schemas to contain strings instead of types
        schemas = {}
        for table, typist in self.typists.items():
            schema = {}
            for column in typist.schema:
                schema[column] = {
                    'type': lookup[typist.schema[column]['type']],
                    'inferred': typist.schema[column]['inferred'],
                }
            schemas[table] = schema

        (self.mount_point / '.backend/schema.json').write_text(json.dumps(schemas, indent=2))

    def sync_csv_to_db(self, csv_path):
        '''
        Sync an updated CSV file back to the data base.
        '''
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
    
        for encoding in encodings:
            try:                
                # Try to decode the file using different encoding standards (in case it's a different encoding)
                df = pd.read_csv(csv_path, encoding=encoding)
                table_name = Path(csv_path).stem
                df = self.typists[table_name](df)

                # Custom dtype mapping for SQLite
                dtype_mapping = {}
                for col in df.columns:
                    if col in self.typists[table_name].schema:
                        schema_type = self.typists[table_name].schema[col]['type']
                        if schema_type == int and len(df[df[col].isna()]) > 0:
                            # Force integer columns to be stored as TEXT to preserve NaN as NULL
                            dtype_mapping[col] = 'TEXT'

                df.to_sql(table_name, self.db, if_exists='replace', index=False, dtype=dtype_mapping)
                return
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f'Error uploading {csv_path}: {e}')

    def query(self, sql: str):
        '''
        Run an SQL query against the database.
        '''
        try:
            return pd.read_sql_query(sql, self.db)
        except:
            return None