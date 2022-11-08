# Database Dump by Table Script

The goal of this php script was to help us do incremental dumps of some REDCap tables
to minimize downtime during a migration.  What this does is identify some
tables that we know are not modified after rows are added so we can dump them
in chunks and migrate them to another server.

## Steps
1. Create the `config.ini` file from the `config.ini.example` file
2. Run the script!

The first time you run, a file called `chunked_tables.json` will be created.  This is a cache
of where the script is.  If you delete the file, it will start over.

Chunked tables only include the drop/create statements if it is the first run for tha table (starting at 0)

