#!/bin/bash

if [[ $1 == ????-??-?? ]]
	then DS=$1
else
	DS=$(date +'%Y-%m-%d')	
fi;

echo "Will run ETL for ds: $DS";

source "/Users/slaw/osobiste/py3/bin/activate";

python /Users/slaw/osobiste/nieruchom/etl.py -d $DS;


# To run it periodically set it in e.g. cronos like:
# daily ETL run:     01 10 * * * cd /Users/slaw/osobiste/nieruchom && ./run_etl.sh
# weekly db backup:  00 13 * * 1 cd /Users/slaw/osobiste/nieruchom && pg_dump postgres | gzip > postgres_backup_$(date +"%m_%d_%Y").gz