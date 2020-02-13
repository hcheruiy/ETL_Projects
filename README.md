The application runs on python 3.5

## About
This is a command line application to demonstrate an example ETL process in Python, MySQL and MongoD. It takes a MySQL database and creates a corresponding MongoDB one.

## Prerequisties

Make sure the following conditions are met:
- ap Database is loaded into MySQL(use the provided `create_db_ap.sql` file.)
- A MySQL server is running and has ap database ready
- A MongoDB server is running
- Make sure you configure ```config.py``` with appropirate variables
- Install the dependencies 
```
pip install -r requirements.txt
```

## How to run

To run, simply type
```
python3 pipeline.py
```

## Author
Hillary Cheruiyot