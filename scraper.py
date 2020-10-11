# built-in
from abc import ABCMeta, abstractmethod
import csv
import datetime
import json
import os
import random

# custom
import user_agents
import config


class MultipleSchemasInScraper(Exception):
    pass


class InconsistentScraperSchema(Exception):
    pass


class Scraper(metaclass=ABCMeta):
    """
    Base class for all scrapers
    """
    def __init__(self, ds=None):
        self.headers = {
            'Accept':'application/json, text/plain, */*',
            'Connection':'keep-alive',
        }
        self.file_path = config.OFFERS_FILE_PATH
        # order of schema matters. make sure it refers to stg table
        self.schema = [
            ('offer_source_id', 'varchar'),
            ('offer_type', 'varchar'),
            ('offer_title', 'varchar'),
            ('offer_url', 'varchar'),
            ('offer_location_raw', 'varchar'),
            ('province', 'varchar'),
            ('county', 'varchar'),
            ('city', 'varchar'),
            ('district', 'varchar'),
            ('neighbourhood', 'varchar'),
            ('no_rooms', 'integer'),
            ('price', 'decimal'),
            ('area', 'decimal'),
            ('offer_source', 'varchar'),
        ]
        self.filed_names = [n for n,t in self.schema]
        if not ds:
            self.ds = datetime.date.today().strftime("%Y-%m-%d")
        else:
            self.ds = ds

    @abstractmethod
    def scrape(self):
        pass

    def get_headers(self):
        ua = {
            'User-Agent': random.choice(user_agents.USER_AGENTS),
        }
        return {**self.headers, **ua}

    def store_offers(self, offers):
        self._check_schema(offers)
        full_file_name = self.get_full_file_name(self.ds)
        with open(full_file_name, 'w', encoding='utf8') as fh:
            writer = csv.writer(fh)
            for offer in offers:
                row = [
                    str(offer.get(k,'')) if offer.get(k,'') != None else ''
                    for k in self.filed_names
                ]
                row = [self.ds, self.scraper_id] + row
                writer.writerow(row)

    def check_file_for_ds(self, ds):
        full_file_name = self.get_full_file_name(ds)
        if os.path.exists(full_file_name):
            return True
        return False

    def get_full_file_name(self, ds):
        ds = ds.replace('-', '_')
        return os.path.join(
            self.file_path,
            f'{self.scraper_id}_{ds}.csv'
        )

    def _check_schema(self, offers):
        schemas = []
        for offer in offers:
            keys = list(offer.keys())
            if not keys in schemas:
                schemas.append(keys)
        if len(schemas) > 1:
            raise MultipleSchemasInScraper(
                f'There are following schemas: {schemas}. Should be only 1.'
            )
        for item in schemas[0]:
            if item not in self.filed_names:
                raise InconsistentScraperSchema(
                    f'{item} does not exists in base Scraper schema definition'
                )
        # TODO(slaw): check type contraint also
