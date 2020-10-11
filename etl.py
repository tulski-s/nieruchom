# built-in
import argparse
import datetime
import os

# custom
import config
import otodom

# 3rd party
import psycopg2


def query_dwh(query, output=False):
    """
    Queries DWH. if `output` is True it will fetch all results and returns those.
    """
    conn_settings = {
        'user': 'slaw',
        'host': 'localhost',
        'dbname': 'postgres', 
        'port': 5432,
        'options': '-c search_path=dwh',  # schema
    }
    with psycopg2.connect(**conn_settings) as conn:
        cur = conn.cursor()
        print(f'Executing:\n{query}')
        cur.execute(query)
        print(f'Success: {cur.rowcount}')
        if output == True:
            return cur.fetchall()


def get_etl_sql(sql_file_name):
    full_path = os.path.join(
        config.ETL_SQL_PATH,
        sql_file_name,
    )
    with open(full_path, 'r') as fh:
        return fh.read()


def scrape_data(scrapers, ds):
    """
    Scrapes sources and store data. 
    """
    for s in scrapers:
        if s.check_file_for_ds(ds):
            print(f'Data exists on disk for {s.scraper_id} and {ds}. No need to scrape.')
            continue
        else:
            print('No data on disk')
        offers = s.scrape()
        print(f'[{s.scraper_id}] Got total {len(offers)} offers')
        s.store_offers(offers)
        print(f'[{s.scraper_id}] Stored data')


def load_to_stg(scrapers, ds):
    """
    Load necessery files into staging tables
    """
    for s in scrapers:
        stg_loaded = query_dwh(f"""
            SELECT
                stg_loaded
            FROM
                etl_tracker
            WHERE
                table_name = 'stg_{s.scraper_id}'
                AND ds = '{ds}'
                AND stg_loaded = True
        ;""", output=True)
        if (len(stg_loaded) == 1) and (stg_loaded[0][0] == True):
            print(f'stg_{s.scraper_id} already loaded for {ds}. Skipping load.')
            continue
        file_name = s.get_full_file_name(ds)
        query_dwh(f"""
            COPY stg_{s.scraper_id} FROM '{file_name}' (FORMAT csv);

            INSERT INTO etl_tracker (
                table_name, ds, stg_loaded, stg_load_ts, dwh_loaded, dwh_load_ts
            )
            VALUES (
                'stg_{s.scraper_id}'
                , '{ds}'
                , True
                , now()::timestamp
                , False
                , NULL
            );
        """)
        print(f'Loaded file ({file_name}) into stg_{s.scraper_id}')


def load_to_dwh(scrapers, ds):
    """
    Load data from staging into DWH offer tables
    """
    for s in scrapers:
        max_dwh_load_ds = query_dwh(f"""
            SELECT
                MAX(ds) AS max_dwh_load_ds
            FROM
                etl_tracker
            WHERE
                table_name = 'stg_{s.scraper_id}'
                AND dwh_loaded = True
        ;""", output=True)[0][0]
        if max_dwh_load_ds == None:
            # random date in past
            max_dwh_load_ds = '1990-01-01'
        else:
            max_dwh_load_ds = max_dwh_load_ds.strftime("%Y-%m-%d")
            print(f'Last loaded ds is: {max_dwh_load_ds}')

        # load to main table only if newer days 
        if max_dwh_load_ds < ds:
            print('Loading from stg to main table')
            sql_query = get_etl_sql('dwh_offers.sql').format(
                scraper_id = s.scraper_id,
                ds = ds,
            )
            query_dwh(sql_query)
        else:
            print(f'Not loading. ds ({ds}) should be older than {max_dwh_load_ds}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--ds', action='store', dest='ds', help='Date in formar YYYY-MM-DD')
    args = parser.parse_args()
    ds = args.ds
    if not ds:
        ds = datetime.date.today().strftime("%Y-%m-%d")
    scrapers = [
        otodom.OtoDom()
    ]
    scrape_data(scrapers, ds)
    load_to_stg(scrapers, ds)
    load_to_dwh(scrapers, ds)
    

    """
    TODOs:
    OK - P0 load from stg to DWH
    OK - P0 enforce correct order of loading
    - P1 enforce retention of stg table
    """


if __name__ == '__main__':
    main()