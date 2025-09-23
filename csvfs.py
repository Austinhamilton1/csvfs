import sqlite3 as sql
import pandas as pd
from pathlib import Path

from fuse import FUSE, Operations, FuseOSError
import os
import stat
import time
import errno

class CSVFilesystemBackend:
    def __init__(self, mount_point: str):
        '''
        Interfaces a folder with CSVs in it with a SQLite database backend.
        '''
        self.mount_point = Path(mount_point)
        
        # Check if we need to initialize the database
        db_path = Path(f'{mount_point}/backend')
        if not db_path.exists():
            db_path.mkdir()

        self.db = sql.connect(self.mount_point / 'backend/database.db')

        cursor = self.db.cursor()
        self.last_modified = {} # Filename -> last modified

        # Check for LastModified Table
        cursor.execute('''
            SELECT name FROM sqlite_master
            WHERE type="table" AND name="LastModified"
        ''')
        if cursor.fetchone() is None:
            # Create the last modified table if it doesn't exist
            cursor.execute('''
                CREATE TABLE LastModified (
                    Id INT PRIMARY KEY AUTOINCREMENT, 
                    FileName VARCHAR(255), 
                    TimeStamp TIMESTAMP
                )''')
        
        # Make sure the LastModified table is caught up with the underlying files
        for csv_file in self.mount_point.iterdir():
            if csv_file.is_file():
                # Need to check its status in the LastModified table
                filename = str(csv_file)
                last_modified = csv_file.stat().st_mtime
                self.last_modified[filename] = last_modified

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
            return pd.read_sql_query(sql, self.db)
        except:
            return None
    
class CSVFS(Operations):
    def __init__(self, root: str):
        self.root = os.path.realpath(root)
        self.csv = CSVFilesystemBackend(root)

        # Virtual directory structure
        self.virtual_dirs = {
            '/',
            '/data',
            '/sql',
            '/sql/queries',
            '/sql/results'
        }

        # Storage for virual files
        self.virtual_files = {} # path -> content
        self.query_results = {} # query_name -> dataframe
        print('CSVFS initialization complete')

    def _get_file_type(self, path: str):
        '''
        Determine what type of file/directory this path represents.
        '''
        if path in self.virtual_dirs:
            return 'directory'
        elif path.startswith('/data/') and path.endswith('.csv'):
            return 'csv_file'
        elif path.startswith('/sql/queries/') and path.endswith('.sql'):
            return 'query_file'
        elif path.startswith('/sql/results/') and path.endswith('.csv'):
            return 'result_file'
        else:
            return 'unknown'
        
    # REQUIRED FUSE OPERATIONS
    def getattr(self, path, fh=None):
        '''
        Get file attributes (like ls -l).
        '''
        file_type = self._get_file_type(path)

        if file_type == 'directory':
            # Directory attributes
            st = {
                'st_mode': stat.S_IFDIR | 0o755,
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': time.time(),
                'st_mtime': time.time(),
                'st_atime': time.time(),
            }
        elif file_type in ['csv_file', 'query_file', 'result_file']:
            # File attributes
            size = 0
            ctime = time.time()
            mtime = time.time()
            atime = time.time()
            query_name = Path(path).stem
            if path in self.virtual_files:
                size = len(self.virtual_files[path].encode('utf-8'))
            elif file_type == 'csv_file':
                # Get size from original CSV or data base
                table_name = Path(path).stem
                df = self.csv.query(f'SELECT * FROM `{table_name}`')
                if df is not None:
                    size = len(df.to_csv().encode('utf-8'))
                    for filename, fmtime in self.csv.last_modified.items():
                        if Path(filename).stem == table_name:
                            mtime = fmtime
                            break
            elif query_name in self.query_results:
                size = len(self.query_results[query_name].to_csv().encode('utf-8'))
            
            st = {
                'st_mode': stat.S_IFREG | 0o644,
                'st_nlink': 1,
                'st_size': size,
                'st_ctime': ctime,
                'st_mtime': mtime,
                'st_atime': atime,
            }
        else:
            raise FuseOSError(errno.ENOENT)
        
        return st
                
    def readdir(self, path, fh):
        '''
        List directory contents.
        '''
        entries = ['.', '..']

        if path == '/':
            entries.extend(['data', 'backend', 'sql'])
        elif path == '/data':
            # List all CSV tables from data base
            cursor = self.csv.db.execute('SELECT name FROM sqlite_master WHERE type="table" AND name != "LastModified"')
            for row in cursor:
                entries.append(f'{row[0]}.csv')
        elif path == '/sql':
            entries.extend(['queries', 'results'])
        elif path == '/sql/queries':
            # List stored queries
            for file_path in self.virtual_files:
                if file_path.startswith('/sql/queries/'):
                    entries.append(Path(file_path).name)
        elif path == '/sql/results':
            # List query results
            for result_name in self.query_results:
                entries.append(f'{result_name}.csv')
        
        return entries
    
    def read(self, path, size, offset, fh):
        '''
        Read file contents.
        '''
        file_type = self._get_file_type(path)
        content = ''

        if file_type == 'csv_file':
            # Read CSV data from database
            table_name = Path(path).stem
            df = self.csv.query(f'SELECT * FROM `{table_name}`')
            if df is not None:
                content = df.to_csv(index=False)
            else:
                content = f'Error reading table `{table_name}`'
        elif file_type == 'query_file':
            # Read SQL query content
            content = self.virtual_files.get(path, '')
        elif file_type == 'result_file':
            # Read query result
            result_name = Path(path).stem
            if result_name in self.query_results and self.query_results[result_name] is not None:
                content = self.query_results[result_name].to_csv(index=False)
            else:
                content = 'Query result not found'
        
        content_bytes = content.encode('utf-8')
        return content_bytes[offset:offset+size]
    
    def write(self, path, data, offset, fh):
        '''
        Write file contents.
        '''
        file_type = self._get_file_type(path)

        if file_type == 'query_file':
            # Initialize file content if it doesn't exist
            if path not in self.virtual_files:
                self.virtual_files[path] = ''

            # Convert bytes to string
            new_content = data.decode('utf-8')

            # Handle offset writing (simple implementation)
            current_content = self.virtual_files.get(path, '')
            if offset == 0:
                self.virtual_files[path] = new_content
            else:
                # Extend content if necessary
                if len(current_content) < offset:
                    current_content += '\0' * (offset - len(current_content))
                self.virtual_files[path] = current_content[:offset] + new_content + current_content[offset+len(new_content):]
            
            # If it's a complete SQL query, try to execute it
            if self.virtual_files[path].strip().endswith(';'):
                self._execute_query(path)

            return len(data)
        
        raise FuseOSError(errno.EACCES)
    
    def create(self, path, mode, fi=None):
        '''
        Create a new file.
        '''
        file_type = self._get_file_type(path)

        if file_type == 'query_file':
            self.virtual_files[path] = ''
            return 0
        
        raise FuseOSError(errno.EACCES)
    
    def truncate(self, path, length, fh=None):
        '''
        Truncate file to specified length.
        '''
        print(f'truncate called for: {path}')
        file_type = self._get_file_type(path)

        if file_type == 'query_file':
            if path in self.virtual_files:
                content = self.virtual_files[path]
                if len(content) > length:
                    self.virtual_files[path] = content[:length]
                else:
                    self.virtual_files[path] = content + '\0' * (length - len(content))

    def unlink(self, path):
        '''
        Delete a file.
        '''
        file_type = self._get_file_type(path)

        if file_type == 'query_file' and path in self.virtual_files:
            del self.virtual_files[path]

            # Also remove corresponding result
            query_name = Path(path).stem
            if query_name in self.query_results:
                del self.query_results[query_name]
        else:
            raise FuseOSError(errno.EACCES)
        
    # HELPER METHODS

    def _execute_query(self, query_path):
        '''
        Execute SQL query and store results.
        '''
        query_content = self.virtual_files.get(query_path, '')
        query_name = Path(query_path).stem

        # Execute the query
        df = self.csv.query(query_content)
        self.query_results[query_name] = df
    
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('source_dir', help='Directory containing CSV files')
    parser.add_argument('mount_point', help='Mount point for the filesystem')
    parser.add_argument('-f', '--foreground', action='store_true', help='Run in foreground')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
    
    args = parser.parse_args()
    
    # Check if mount point exists
    mount_path = Path(args.mount_point)
    if not mount_path.exists():
        print(f"Creating mount point: {mount_path}")
        mount_path.mkdir(parents=True, exist_ok=True)
    
    # Check if source directory exists
    source_path = Path(args.source_dir)
    if not source_path.exists():
        print(f"Error: Source directory {source_path} does not exist")
        return 1
    
    print(f"Mounting {source_path} -> {mount_path}")
    
    try:
        FUSE(CSVFS(args.source_dir), args.mount_point, 
             nothreads=True, foreground=args.foreground, debug=args.debug)
    except Exception as e:
        print(f"FUSE mount failed: {e}")
        return 1
    
if __name__ == '__main__':
    main()