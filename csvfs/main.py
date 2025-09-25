#!/usr/bin/env python3

from fuse import FUSE, Operations, FuseOSError
from pathlib import Path

import os
import stat
import time
import errno
import pandas as pd
import json 
import re
import argparse
from datetime import datetime

from backend import CSVFilesystemBackend
    
class CSVFS(Operations):
    def __init__(self, root: str, page_size: int=3000):
        self.PAGE_SIZE = page_size
        self.root = os.path.realpath(root)
        self.csv = CSVFilesystemBackend(root)

        # Virtual directory structure
        self.virtual_dirs = {
            '/',
            '/data',
            '/sql',
            '/sql/queries',
            '/sql/results',
            '/stats',
            '/schemas'
        }

        # Statistics of the mounted filesystem 
        self.stats = {}

        # Storage for virual files
        self.virtual_files = {} # path -> content
        self.query_results = {} # query_name -> dataframe
        print('CSVFS initialization complete')
        
    # REQUIRED FUSE OPERATIONS

    def getattr(self, path, fh=None):
        '''
        Get file attributes (like ls -l).
        '''
        file_type = self._get_file_type(path)

        if file_type == 'directory' or file_type == 'paginated_directory' or file_type == 'paginated_leaf_directory':
            # Directory attributes
            st = {
                'st_mode': stat.S_IFDIR | 0o755,
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': self.csv.c_time,
                'st_mtime': self.csv.c_time,
                'st_atime': time.time(),
            }
        elif file_type in ['stats_file', 'csv_file', 'query_file', 'result_file', 'paginated_csv_file']:
            # File attributes
            size = 0
            ctime = self.csv.c_time
            mtime = self.csv.c_time
            atime = time.time()

            if file_type == 'stats_file':
                size = 4096 * 4096 # Default to ensure first read gets all data

                # If the table has been read before we have good data
                table_name = Path(path).stem
                if table_name in self.stats:
                    size = len(json.dumps(self.stats[table_name], indent=2).encode('utf-8'))
            
            elif file_type == 'query_file':
                # Handle query files - they should always appear to exist once created
                if path in self.virtual_files:
                    content = self.virtual_files[path]
                    size = len(content.encode('utf-8'))
                else:
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
            entries.extend(['data', 'sql', 'stats'])
        elif path == '/data':
            # List all CSV tables from data base
            tables = self._get_tables()
            for table in tables:
                count = self.csv.query(f'SELECT COUNT(*) FROM `{table}`')
                if count.iloc[0,0] <= self.PAGE_SIZE:
                    entries.append(f'{table}.csv')
            # Add paginated directories for large tables
            paginated_dirs = self._get_paginated_directories()
            entries.extend(paginated_dirs)
        elif path.startswith('/data/paged_'):
            file_type = self._get_file_type(path)
            if file_type == 'paginated_directory':
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
                            page_filename = f'{table_name}.{start_row+1}-{end_row+1}'
                            entries.append(page_filename)
            elif file_type == 'paginated_leaf_directory':
                # Get the information from the directory path
                pagination_info = self._parse_pagination(path)
                if pagination_info is not None:
                    filename, start_row, end_row = pagination_info
                    entries.append(f'{filename}.{start_row}-{end_row}.csv')
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
        elif path == '/stats':
            # List all statistic files
            tables = self._get_tables()
            for table in tables:
                entries.append(f'{table}.json')
            entries.append('global.json')
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
        elif file_type == 'stats_file':
            # Read statistics for a global/table state
            table_name = Path(path).stem
            self._update_stats(table_name)
            content = json.dumps(self.stats[table_name], indent=2)

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

        if file_type in ['query_file', 'result_file', 'csv_file', 'paginated_csv_file', 'stats_file']:
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

        if file_type == 'directory' or file_type == 'paginated_directory' or file_type == 'paginated_leaf_directory':
            return 0 # Directories are always accessible
        elif file_type in ['query_file', 'result_file', 'csv_file', 'paginated_csv_file', 'stats_file']:
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
                        table_name, start_row, _ = pagination_info
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

    def _get_file_type(self, path: str):
        '''
        Determine what type of file/directory this path represents.
        '''
        if path in self.virtual_dirs:
            return 'directory'
        elif path.startswith('/stats/') and path.endswith('.json'):
            return 'stats_file'
        elif path.startswith('/data/') and path.endswith('.csv'):
            if self._is_paginated_file(path):
                return 'paginated_csv_file'
            return 'csv_file'
        elif path.startswith('/data/paged_'):
            if self._is_paginated_file(path):
                return 'paginated_leaf_directory'
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
        filename = Path(path).name

        return re.match(r'.+\.\d+-\d+', filename) is not None
    
    def _parse_pagination(self, path: str):
        '''
        Parse out the pagination information for a paginated file.
        '''
        filename = Path(path).name

        m = re.match(r'(.+)\.(\d+)-(\d+)', filename)
        if m is None:
            return None
        return (m.group(1), int(m.group(2)), int(m.group(3)))
    
    def _get_paginated_directories(self):
        '''
        Get list of tables that should have paginated directories.
        '''
        paginated_dirs = []
        tables = self._get_tables()

        for table_name in tables:
            # Check if table has more than PAGE_SIZE rows
            count_df = self.csv.query(f'SELECT COUNT(*) as count FROM `{table_name}`')
            if count_df is not None and len(count_df) > 0:
                row_count = count_df.iloc[0]['count']
                if row_count > self.PAGE_SIZE:
                    paginated_dirs.append(f'paged_{table_name}')

        return paginated_dirs
    
    def _update_stats(self, table_name: str):
        '''
        Update internal statistics for global/table stats files.
        '''
        # Check if the table is in stats.
        exists = False
        if table_name in self.stats:
            exists = True

        if table_name == 'global':
            # Global statistics should be up to date by default
            if exists and self.stats[table_name]['up_to_date']:
                return
            
            # Global statistics
            tables = self._get_tables()
        
            total_rows = 0
            total_columns = 0
            files = []
            for table in tables:
                row_count = int(self.csv.query(f'SELECT COUNT(*) FROM `{table}`').iloc[0, 0])
                column_count = len(self.csv.query(f'SELECT * FROM `{table}` LIMIT 1'))

                total_rows += row_count
                total_columns += column_count

                files.append({
                    'filename': f'{self.root}/{table}.csv',
                    'stat_file': f'/stats/{table}.json',   
                })

            self.stats[table_name] = {
                'up_to_date': True,
                'files': files,
                'total_rows': total_rows,
                'total_columns': total_columns,
            }
        else:
            # If the table exists, just show calculated data regardless of staleness
            # (User must refresh manually; cuts down on expensive analysis)
            if exists:
                return
            
            # Get the size of the file
            data = self.csv.query(f'SELECT * FROM `{table_name}`')
            size_bytes = len(data.to_csv().encode('utf-8'))
                
            schema = {}
            for column in data.columns:
                if self.csv.typists[table_name].schema[column]['type'] == int:
                    schema[column] = {
                        'type': 'int',
                        'inferred': self.csv.typists[table_name].schema[column]['inferred'],
                        'nulls': len(data[data[column].isna()]),
                        'min': int(data[column].dropna().min()),
                        'max': int(data[column].dropna().max()),
                    }   
                elif self.csv.typists[table_name].schema[column]['type'] == float:
                    schema[column] = {
                        'type': 'float',
                        'inferred': self.csv.typists[table_name].schema[column]['inferred'],
                        'nulls': len(data[data[column].isna()]),
                        'min': float(data[column].dropna().min()),
                        'max': float(data[column].dropna().max()),
                        'avg': float(data[column].dropna().mean()),
                    }  
                elif self.csv.typists[table_name].schema[column]['type'] == bool:
                    schema[column] = {
                        'type': 'bool',
                        'inferred': self.csv.typists[table_name].schema[column]['inferred'],
                        'nulls': len(data[data[column].isna()]),
                    }
                elif self.csv.typists[table_name].schema[column]['type'] == str:
                    schema[column] = {
                        'type': 'string',
                        'inferred': self.csv.typists[table_name].schema[column]['inferred'],
                        'nulls': len(data[data[column].isnull()]),
                        'distinct': len(data[column].drop_duplicates()),
                    }
                elif self.csv.typists[table_name].schema[column]['type'] == type(datetime):
                    data[column] = pd.to_datetime(data[column], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                    schema[column] = {
                        'type': 'datetime',
                        'inferred': self.csv.typists[table_name].schema[column]['inferred'],
                        'nulls': len(data[data[column].isnull()]),
                        'start_date': datetime.strftime(data[column].dropna().min(), '%Y-%m-%d %H:%M:%S'),
                        'end_date': datetime.strftime(data[column].dropna().max(), '%Y-%m-%d %H:%M:%S'),
                    }
                else:
                    schema[column] = {
                        'type': 'unknown',
                    }

            self.stats[table_name] = {
                'file': f'{self.root}/{table_name}.csv',
                'size_bytes': size_bytes,
                'last_modified': datetime.strftime(datetime.fromtimestamp(self.csv.m_cache[f'{table_name}.csv']), '%Y-%M-%d %H:%M:%S'),
                'up_to_date': True,
                'last_analyzed': datetime.strftime(datetime.fromtimestamp(time.time()), '%Y-%m-%d %H:%M:%S'),
                'stale_reason': None,
                'rows': int(self.csv.query(f'SELECT COUNT(*) FROM `{table_name}`').iloc[0, 0]),
                'columns': len(schema),
                'schema': schema,
            }

    def _get_tables(self):
        ''' 
        Get a list of all tables currently in the database.
        '''
        tables = []

        cursor = self.csv.db.execute('SELECT name FROM sqlite_master WHERE name != "LastModified" AND name != "sqlite_sequence"')
        for row in cursor:
            tables.append(row[0])

        return tables

    def _execute_query(self, query_path):
        '''
        Execute SQL query and store results.
        '''
        query_content = self.virtual_files.get(query_path, '')
        query_name = Path(query_path).stem

        # Execute the query
        for query in query_content.split(';'):
            if len(query.strip()) > 0:
                df = self.csv.query(query)
                self.query_results[query_name] = df
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source_dir', help='Directory containing CSV files')
    parser.add_argument('mount_point', help='Mount point for the filesystem')
    parser.add_argument('-f', '--foreground', action='store_true', help='Run in foreground')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
    parser.add_argument('-n', '--page-size', default=3000, type=int, help='Set number of rows per page for paginated CSVs (default 3000).')
    
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
        FUSE(CSVFS(args.source_dir, args.page_size), args.mount_point, 
             nothreads=True, foreground=args.foreground, debug=args.debug)
    except Exception as e:
        print(f"FUSE mount failed: {e}")
        return 1
    
if __name__ == '__main__':
    main()