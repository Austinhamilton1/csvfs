import sqlite3 as sql
import pandas as pd
from pathlib import Path
import time

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