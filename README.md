# csvFS – A FUSE Filesystem for CSV-as-Database  

**csvFS** is a Python-based FUSE filesystem that lets you interact with CSV files as if they were a live database. It supports SQL querying, paginated access to extremely large CSVs, and transient (non-destructive) updates — all without modifying your original files unless you explicitly export the changes.  

---

## Features  

- **Mount CSVs as a Filesystem** – Treat a folder of CSV files as a mounted virtual filesystem.  
- **Automatic Database Backend** – On mount, csvFS creates and manages an SQLite3 database in a `.backend/` folder alongside your original CSVs.  
- **Pagination for Large CSVs** – Work with massive CSVs without running out of memory:
  - Small CSVs appear as flat files in the `data/` folder.
  - Large CSVs are split into `paginated_{table_name}/` folders containing sequentially numbered chunks.  
- **SQL Query Interface** – Drop `.sql` files into the `sql/queries/` folder to run SQL commands:
  - Sequentially executes all statements in the file.
  - The result of the **last query** is automatically written as a CSV file in `sql/results/`.  
- **Transient Updates** – Make INSERTs, UPDATEs, and DELETEs against the database without affecting the original CSVs.  
- **Fast Mounting** – Only updates the backend database if the original CSVs have changed since the last mount.
- **Quick Data Analysis** – Quickly parse out simple data analytics (e.g., number of nulls, min, max) just by looking at the stats/{table_name}.json files.
- **Robust Type Handling** – Manually manage CSV data types or let the heuristic intelligence system manage it for you.
    - By default, the system intelligently decides data types for you.
    - For more control, add a {csv_file}.schema file to your source directory to coerce types.

---

## Filesystem Layout  

Once mounted, your filesystem will look like this:  

```
{mount_point}
|- data/
|  |- small_table.csv
|  |- paginated_large_table
|     |- large_table.1-1000
|        |- large_table.1-1000.csv
|     |- large_table.1001-2000
|        |- large_table.1001-2000.csv
|     |- ...
|- sql/
|  |- queries/
|     |- my_query.sql
|     |- ...
|  |- results/
|     |- my_query.csv
|     |- ...
|- stats/
|  |- global.json
|  |- table1.json
|  |- table2.json
|  |- ...
```

---

## Usage

`csvfs <source_dir> <mount_point> [options]`

### Options

| **Option** | **Description** |
| --- | --- |
| `-h`, `--help` | Show help message and exit. |
| `-f`, `--foreground` | Run in foreground (do not daemonize). |
| `-d`, `--debug` | Enable debug output for troubleshooting. |
| `-n PAGE_SIZE`, `--page-size PAGE_SIZE` | Set number of rows per page for paginated CSVs (default 3000). |

### Example

`csvfs ./my_csvs ./mnt -f -n 2000`

This mounts `./my_csvs` to `./mnt` with a page size of 2000 rows per file.

---

## Workflow Example

1. **Mount the filesystem**:
    - `csvfs ./data ./mnt -f`
2. **Navigate into the mounted folder**:
    - `cd ./mnt/data`
    - `cat small_table.csv`
3. **Check file statistics**:
    - To check file statistics such as data types just check the /mnt/stats/{table_name}.json file:
        - `cat /mnt/stats/small_table.json`
4. **Run a query**:
    - Write an SQL file in `/sql/queries/`
        - `echo 'SELECT * FROM users WHERE age > 30;' > /mnt/sql/queries/data.sql`
    - Once saved the results will appear in `/sql/results` as shown:
        - `cat /mnt/sql/results/data.csv`
5. **Export modified data**
    - If data was modified in the SQL query before the final `SELECT`:
        - `cp /mnt/sql/results/{query_name}.csv <export_file>`

---

## Why csvFS?

- No need to manually load CSVs into a database - just mount and go.
- Makes massive CSVs usable even on resource-constrained machines.
- Great for ad-hoc analysis, ETL pipelines, and data exploration.
- Eliminates the need for cumbersome CSV parsing scripts.
- Simple analytics baked into the file system.
- Advanced type handling means no need for complicated data type parsing.

---

## Requirements

- Python 3.8+
- fusepy (included in installation instructions)
- pandas (included in installation instructions)
- SQLite3 (included with Python)
- libFUSE or macFUSE (depends on your OS)
---

## Installation

Clone GitHub repository:

`git clone https://github.com/Austinhamilton1/csvfs.git`

Build project:

`pip install --user -e .`

---

## Contributing

Contributions are welcome! Open an issue or submit a pull request if you hae ideas for new features, bug fixes, or optimizations.
