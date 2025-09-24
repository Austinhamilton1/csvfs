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

---

## Filesystem Layout  

Once mounted, your filesystem will look like this:  

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
|  |- results/
|     |- my_query.csv

---

## Usage

`python csvfs.py <source_dir> <mount_point> [options]`

### Options

| **Option** | **Description** |
| --- | --- |
| `-f` | Run in foreground (do not daemonize). |
| `-d` | Enable debug output for troubleshooting. |
| `-n SIZE`, `--page-size SIZE` | Set number of rows per page for paginated CSVs (default 1000). |
| `-h` | Show help message and exit. |

### Example

`python csvfs.py ./my_csvs ./mnt -f -p 2000`

This mounts `./my_csvs` to `./mnt` with a page size of 2000 rows per file.

---

## Workflow Example

1. **Mount the filesystem**:
    - `python csvs.py ./data ./mnt -f`
2. **Navigate into the mounted folder**:
    - `cd ./mnt/data`
    - `cat small_table.csv`
3. **Run a query**:
    - Write an SQL file in `/sql/queries/`
        - `echo 'SELECT * FROM users WHERE age > 30;' > ./mnt/sql/queries/data.sql`
    - Once saved the results will appear in `/sql/results` as shown:
        - `cat ./mnt/sql/results/data.csv`
4. **Export modified data**
    - If data was modified in the SQL query before the final `SELECT`:
        - `cp ./mnt/sql/results/{query_name}.csv <export_file>`

---

## Why csvFS?

- No need to manually load CSVs into a database - just mount and go.
- Makes massive CSVs usable even on resource-constrained machines.
- Great for ad-hoc analysis, ETL pipelines, and data exploration.
- Eliminates the need for cumbersome CSV parsing scripts.

---

## Requirements

- Python 3.8+
- fusepy
- pandas
- SQLite3 (included with Python)

Install dependencies:

`pip install fusepy pandas`

---

## Contributing

Contributions are welcome! Open an issue or submit a pull request if you hae ideas for new features, bug fixes, or optimizations.
