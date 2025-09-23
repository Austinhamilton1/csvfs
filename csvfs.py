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
                    Id INTEGER PRIMARY KEY AUTOINCREMENT, 
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
        self.PAGE_SIZE = 1000
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
            if self._is_paginated_file(path):
                return 'paginated_csv_file'
            return 'csv_file'
        elif path.startswith('/data/paged_'):
            return 'paginated_directory'
        elif path.startswith('/sql/queries/') and path.endswith('.sql'):
            return 'query_file'
        elif path.startswith('/sql/results/') and path.endswith('.csv'):
            return 'result_file'
        else:
            return 'unknown'
        
    def _is_paginated_file(self, path: str):
        '''
        Check if this is a paginated file.
        '''
        filename = Path(path).stem
        
        import re
        return re.match(r'.+\.\d+-\d+', filename) is not None
    
    def _parse_pagination(self, path: str):
        '''
        Parse out the pagination information for a paginated file.
        '''
        filename = Path(path).stem

        import re
        m = re.match(r'(.+)\.(\d+)-(\d+)', filename)
        if m is None:
            return None
        return (m.group(1), int(m.group(2)), int(m.group(3)))
    
    def _get_paginated_directories(self):
        '''
        Get list of tables that should have paginated directories.
        '''
        paginated_dirs = []
        cursor = self.csv.db.execute('SELECT name FROM sqlite_master WHERE type="table" AND name != "LastModified" AND name != "sqlite_sequence"')
        for row in cursor:
            table_name = row[0]
            # Check if table has more than PAGE_SIZE rows
            count_df = self.csv.query(f'SELECT COUNT(*) as count FROM `{table_name}`')
            if count_df is not None and len(count_df) > 0:
                row_count = count_df.iloc[0]['count']
                if row_count > self.PAGE_SIZE:
                    paginated_dirs.append(f'paged_{table_name}')
        return paginated_dirs
        
    # REQUIRED FUSE OPERATIONS
    def getattr(self, path, fh=None):
        '''
        Get file attributes (like ls -l).
        '''
        file_type = self._get_file_type(path)

        if file_type == 'directory' or file_type == 'paginated_directory':
            # Directory attributes
            st = {
                'st_mode': stat.S_IFDIR | 0o755,
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': time.time(),
                'st_mtime': time.time(),
                'st_atime': time.time(),
            }
        elif file_type in ['csv_file', 'query_file', 'result_file', 'paginated_csv_file']:
            # File attributes
            size = 0
            ctime = time.time()
            mtime = time.time()
            atime = time.time()

            if file_type == 'query_file':
                # Handle query files - they should always appear to exist once created
                if path in self.virtual_files:
                    content = self.virtual_files[path]
                    size = len(content.encode('utf-8'))
                else:
                    # Even if not created yet, report size 0 so editors can create it
                    raise FuseOSError(errno.ENOENT)
                    
            elif file_type == 'result_file':
                # Handle query results
                result_name = Path(path).stem
                
                if result_name in self.query_results and self.query_results[result_name] is not None:
                    content = self.query_results[result_name].to_csv(index=False)
                    size = len(content.encode('utf-8'))
                else:
                    # File doesn't exist yet
                    raise FuseOSError(errno.ENOENT)
                    
            elif file_type == 'csv_file':
                # Get size from original CSV or database
                table_name = Path(path).stem
                df = self.csv.query(f'SELECT * FROM `{table_name}`')
                if df is not None:
                    size = len(df.to_csv().encode('utf-8'))
                    # Get last modified from database
                    for filename, fmtime in self.csv.last_modified.items():
                        if Path(filename).stem == table_name:
                            mtime = fmtime
                            break
                else:
                    raise FuseOSError(errno.ENOENT)
            elif file_type == 'paginated_csv_file':
                # Handle paginated CSV file
                pagination_info = self._parse_pagination(path)
                if pagination_info:
                    table_name, start_row, end_row = pagination_info
                    
                    # Get paginated data
                    limit = end_row - start_row + 1
                    df = self.csv.query(f'SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {start_row}')
                    
                    if df is not None and len(df) > 0:
                        size = len(df.to_csv(index=False).encode('utf-8'))
                        # Get last modified from database
                        for filename, fmtime in self.csv.last_modified.items():
                            if Path(filename).stem == table_name:
                                mtime = fmtime
                                break
                    else:
                        # This page doesn't exist (beyond end of data)
                        raise FuseOSError(errno.ENOENT)
                else:
                    raise FuseOSError(errno.ENOENT)
                
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
            entries.extend(['data', 'sql'])
        elif path == '/data':
            # List all CSV tables from data base
            cursor = self.csv.db.execute('SELECT name FROM sqlite_master WHERE type="table" AND name != "LastModified" AND name != "sqlite_sequence"')
            for row in cursor:
                count = self.csv.query(f'SELECT COUNT(*) FROM `{row[0]}`')
                if count.iloc[0,0] <= self.PAGE_SIZE:
                    entries.append(f'{row[0]}.csv')
            # Add paginated directories for large tables
            paginated_dirs = self._get_paginated_directories()
            entries.extend(paginated_dirs)
        elif path.startswith('/data/paged_'):
            # Handle paginated directory listing
            dir_name = Path(path).name
            table_name = dir_name[6:]  # Remove 'paged_' prefix
            
            # Get total row count
            count_df = self.csv.query(f'SELECT COUNT(*) as count FROM `{table_name}`')
            if count_df is not None and len(count_df) > 0:
                total_rows = count_df.iloc[0]['count']
                
                # Generate page files
                for start_row in range(0, total_rows, self.PAGE_SIZE):
                    end_row = min(start_row + self.PAGE_SIZE - 1, total_rows - 1)
                    if start_row <= end_row:  # Valid page
                        page_filename = f'{table_name}.{start_row}-{end_row}.csv'
                        entries.append(page_filename)
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
        elif file_type == 'paginated_csv_file':
            # Read paginated CSV data
            pagination_info = self._parse_pagination(path)
            if pagination_info:
                table_name, start_row, end_row = pagination_info
                
                # Get paginated data
                limit = end_row - start_row + 1
                df = self.csv.query(f'SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {start_row}')
                
                if df is not None:
                    content = df.to_csv(index=False)
                else:
                    content = f'Error reading paginated table `{table_name}` rows {start_row}-{end_row}'
            else:
                content = 'Invalid pagination format'
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
        
    def open(self, path, flags):
        '''
        Open a file.
        '''
        file_type = self._get_file_type(path)

        if file_type in ['query_file', 'result_file', 'csv_file', 'paginated_csv_file']:
            return 0

        raise FuseOSError(errno.EACCES)
    
    def flush(self, path, fh):
        '''
        Flush a file. Used when a file is closed.
        '''
        # Nothing for virtual files
        return 0
    
    def release(self, path, fh):
        '''
        Release (close) a file.
        '''
        # Nothing for virtual files
        return 0
    
    def fsync(self, path, datasync, fh):
        '''
        Synchronize file contents.
        '''
        # Nothing for virtual files
        return 0
        
    def access(self, path, amode):
        '''
        Check file access permissions.
        '''
        file_type = self._get_file_type(path)

        if file_type == 'directory' or file_type == 'paginated_directory':
            return 0 # Directories are always accessible
        elif file_type in ['query_file', 'result_file', 'csv_file', 'paginated_csv_file']:
            # Check what type of access is required
            if amode == os.F_OK:
                if file_type == 'query_file':
                    return 0 # Query files can always be created
                elif file_type == 'result_file':
                    result_name = Path(path).stem
                    if result_name in self.query_results:
                        return 0
                    else:
                        raise FuseOSError(errno.ENOENT)
                elif file_type == 'paginated_csv_file':
                    # Check if this page exists
                    pagination_info = self._parse_pagination(path)
                    if pagination_info:
                        table_name, start_row, end_row = pagination_info
                        # Verify the page has data
                        df = self.csv.query(f'SELECT COUNT(*) as count FROM `{table_name}` WHERE rowid > {start_row}')
                        if df is not None and len(df) > 0 and df.iloc[0]['count'] > 0:
                            return 0
                        else:
                            raise FuseOSError(errno.ENOENT)
                    else:
                        raise FuseOSError(errno.ENOENT)
                else:
                    return 0
            elif amode == os.R_OK: # Read access
                return  0
            elif amode == os.W_OK: # Write access
                if file_type == 'query_file':
                    return 0
                else:
                    raise FuseOSError(errno.EACCES)
            elif amode == os.X_OK: # Execute access
                raise FuseOSError(errno.EACCES)
        
        raise FuseOSError(errno.ENOENT)
    
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