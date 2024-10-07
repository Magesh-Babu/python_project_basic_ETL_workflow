# Basic ETL process

### Extract
Data imported from local MS SQL Server in import_data.py

### Transform
ScreenResolution, OpSys, Weight, ram. These are the columns modified in the database table using process_data.py

### Load
Uploaded the modified table back to the MS SQL Server using update_database.py
