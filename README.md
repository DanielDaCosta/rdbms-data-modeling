# rdbms-data-modeling

## Database Schema

![DatabaseSchema](Images/schema_database.png)

## Usage

1. Starting local PostgreSQL instance:
```
docker-compose up
```

You can edit your database credentials in `database.ini`

2. Create the database running:
```
python create_tables.py
```

3. (optional) Run ETL.ipynb in order to check the full pipeline

4. Run `elt.py`: it will read and process all the files from `song_data` and `log_data`, loading them into the database