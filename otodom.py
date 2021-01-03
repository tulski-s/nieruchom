# built in
import datetime
import json
import logging
import random
import re
import sys
import time

# 3rd party
import requests
import bs4

# custom
import scraper

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f'otodom_at_{datetime.date.today().strftime("%Y_%m_%d")}.log')
logger.addHandler(fh)  # log to file
logger.addHandler(logging.StreamHandler(sys.stdout))  # and to console

class OtoDom(scraper.Scraper):
    """
    https://www.otodom.pl/
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraper_id = 'otodom'
        self.base_sitemap = 'https://www.otodom.pl/sitemap.xml'  # is not updated frequently so better not to use
        self.flat_rent_listing_url = 'https://www.otodom.pl/wynajem/mieszkanie/'
        self.flat_sell_listing_url = 'https://www.otodom.pl/sprzedaz/mieszkanie/'
        self.params = {
            'nrAdsPerPage': 72  # no of offers per page. 72 is max
        }
        self.selected_cities = set([
            'bydgoszcz', 'gdansk', 'katowice', 'krakow', 'lodz', 'lublin',
            'lubin', 'poznan', 'szczecin', 'warszawa', 'wroclaw', 'gdynia',
            'zielona-gora', 'leszno', 'jelenia-gora', 'gdynia', 'swidnica'
        ])

    def scrape(self, limit_pages=None, filter_cities=True):
        listings = self._get_all_listing()
        listings_and_types = []
        for lst in listings:
            if filter_cities == True:
                loc = self._url2loc(lst)
                if loc not in self.selected_cities:
                    continue
            if 'wynajem' in lst:
                listings_and_types.append((lst, 'rent'))
            elif 'sprzedaz/mieszkanie' in lst:
                listings_and_types.append((lst, 'sell'))
            elif 'sprzedaz/nowe-mieszkanie' in lst:
                listings_and_types.append((lst, 'sell_new'))
        offers = []
        counter = 0
        for idx, (listing, _type) in enumerate(listings_and_types):
            logger.debug(f'Process listing: {listing} [{idx+1}/{len(listings_and_types)}]')
            for page_idx in range(1, self._get_no_pages(listing)+1):
                offers.extend(
                    self._get_offers(listing, _type, page_idx)
                )
                self._sleep()
                counter += 1
                if (limit_pages != None) and (counter >= limit_pages):
                    return self._dedup_offers(offers)
        return self._dedup_offers(offers) 

    def _sleep(self):
        """sleep for random (real) time between <start, stop>"""
        time.sleep(random.uniform(1, 2))

    def _get_no_pages(self, listing_base):
        r = requests.get(
            listing_base,
            headers=self.get_headers(),
            params=self.params
        )
        return int(re.findall('"page_count":"(\d+)"',  r.text)[0])

    def _parse_location(self, loc_raw):
        """
        Parse raw location. There are following possibilities:
        1. Miasto, powiat, wojewodztwo
            - Ząbki, wołomiński, mazowieckie
            - Głuchołazy, nyski, opolskie 
        2. Miasto, Dzielnica, Osiedle
            - Wrocław, Psie Pole, Lipa Piotrowska
            - Wrocław, Fabryczna, Oporów
        3. Miasto, Dzielnica
            - Gdańsk, Jelitkowo
        4. Miasto, wojewodztwo
            - Gliwice, śląskie
        """
        province = None 
        county = None 
        city = None 
        district = None 
        neighbourhood = None 
        loc_els = [el.strip() for el in loc_raw.split(',')]
        city = loc_els[0].lower()
        if len(loc_els) == 3:
            if loc_els[1][0].isupper():
                district = loc_els[1].lower()
                neighbourhood = loc_els[2].lower()
            else:
                county = loc_els[1].lower()
                province = loc_els[2].lower()
        elif len(loc_els) == 2:
            if loc_els[1][0].isupper():
                district = loc_els[1].lower()
            else:
                province = loc_els[1].lower()
        return province, county, city, district, neighbourhood

    def _get_offers(self, listing, listing_type, page_idx):
        logger.debug(f'Getting offers from: {listing}, page: {page_idx}')
        if page_idx == 1:
            # base listing is also a first pages
            params=dict(**self.params)
        else:
            params=dict(**self.params, page=page_idx)
        r = requests.get(
            listing,
            headers=self.get_headers(),
            params=params
        )
        bs_obj = bs4.BeautifulSoup(r.text, features='lxml')
        tags = bs_obj.find_all(['article'])
        offers = []
        for tag in tags:
            # filter to offers only
            offer_match = re.match(r'offer-item-ad_id(.*)', tag.get('id'))
            if not offer_match:
                continue
            else:
                offer_source_id = offer_match[1]
            offer_title = tag.find_all(class_='offer-item-title')[0].text
            offer_url = tag.find_all(['a'])[0].get('href')
            if listing_type == 'rent':
                loc_pat = r'Mieszkanie na wynajem: (.*)'
            elif listing_type in ('sell', 'sell_new'):
                loc_pat =  r'Mieszkanie na sprzedaż: (.*)'
            offer_location_raw = re.findall(loc_pat, tag.find_all(['p'])[0].text)[0]
            province, county, city, district, neighbourhood = self._parse_location(offer_location_raw)
            offer_details_tag = tag.find_all('ul', {'class': 'params'})[0]
            details_tag_lis = offer_details_tag.find_all('li')
            try:
                no_rooms = int(re.findall('(\d+) [pokoje|pokój]', details_tag_lis[0].text.strip())[0])
            except IndexError:
                # there are rare cases where there is no rooms info
                no_rooms = None

            if len(details_tag_lis) == 1:
                # should not be the case. sth odd with this offer
                continue

            price_raw_tag = details_tag_lis[1].text.strip()
            if 'Zapytaj o cenę' in price_raw_tag:
                # no price available. ignore this offer
                continue
            elif no_rooms == None:
                price = float(
                    re.findall(
                        '([\d\s,]+)zł.*', details_tag_lis[0].text.strip()
                    )[0].replace(' ', '').replace(',', '.')
                )
            else:
                price = float(
                    re.findall(
                        '([\d\s,]+)zł.*',
                        price_raw_tag
                    )[0].replace(' ', '').replace(',', '.')
                )

            if no_rooms != None:
                area = float(
                    re.findall('([\d,]+) m²', details_tag_lis[2].text.strip())[0].replace(',', '.')
                )
            else:
                area = float(
                    re.findall('([\d,]+) m²', details_tag_lis[1].text.strip())[0].replace(',', '.')
                )
            
            offer_bottom_tag = tag.find_all('div', {'class': 'offer-item-details-bottom'})[0]
            offer_bottom_lis = offer_bottom_tag.find_all('li')
            if len(offer_bottom_lis) == 1:
                offer_source = offer_bottom_lis[0].text.strip()
            else:
                offer_source = offer_bottom_lis[1].text.strip()
            offers.append({
                'offer_source_id': offer_source_id,
                'offer_title': offer_title,
                'offer_url': offer_url,
                'offer_location_raw': offer_location_raw,
                'province': province,
                'county': county,
                'city': city,
                'district': district,
                'neighbourhood': neighbourhood,
                'no_rooms': no_rooms,
                'price': price,
                'area': area,
                'offer_source': offer_source,
                'offer_type': listing_type,
            })
        return offers

    def _dedup_offers(self, offers):
        # dedup in case promoted offers gets scraped multiple times
        list_of_strings = [json.dumps(d, sort_keys=True) for d in offers]
        list_of_strings = set(list_of_strings)
        return [json.loads(s) for s in list_of_strings]

    def _url2loc(self, url):
        url = url.replace('https://www.otodom.pl/sprzedaz/nowe-mieszkanie/' ,'')
        url = url.replace('https://www.otodom.pl/sprzedaz/mieszkanie/' ,'')
        url = url.replace('https://www.otodom.pl/wynajem/mieszkanie/' ,'')
        url = url[:-1]
        return url

    def _get_all_listing(self):
        base_listings = set()
        with open('./otodom_locations_urls.txt', 'r') as fh:
            for line in fh.readlines():
                base_listings.add(line.strip())

        extra_locs_keys = set([
            'jelenia-gora', 'legnica', 'lubin', 'walbrzych', 'wroclaw', 'bydgoszcz', 'grudziadz',
            'inowroclaw', 'torun', 'wloclawek', 'lodz', 'pabianice', 'piotrkow-trybunalski', 
            'tomaszow-mazowiecki', 'chelm', 'lublin', 'zamosc', 'gorzow-wielkopolski', 
            'zielona-gora', 'krakow', 'nowy-sacz', 'tarnow', 'plock', 'pruszkow', 'radom', 
            'warszawa', 'kedzierzyn-kozle', 'rzeszow', 'bialystok', 'lomza', 'gdansk', 'gdynia', 
            'sopot', 'tczew', 'bedzin', 'bielsko-biala', 'chorzow', 'czestochowa', 'dabrowa-gornicza',
            'jastrzebie-zdroj', 'jaworzno', 'katowice', 'mikolow', 'myslowice', 'rybnik', 
            'siemianowice-slaskie', 'sosnowiec', 'swietochlowice', 'tarnowskie-gory', 'tychy',
            'zabrze', 'zory', 'kielce', 'ostrowiec-swietokrzyski', 'elblag', 'olsztyn', 'gniezno',
            'kalisz', 'konin', 'leszno', 'ostrow-wielkopolski', 'pila', 'kolobrzeg', 'koszalin',
            'stargard', 'szczecin',
        ])

        with_extra_locs = set()
        for listing in base_listings:
            base_loc = self._url2loc(listing)
            if base_loc in extra_locs_keys:
                with_extra_locs.add(listing)

        extended_listings = set()
        for idx, listing in enumerate(with_extra_locs):
            logger.debug(f'Getting extra listing from: {listing} [{idx+1}/{len(with_extra_locs)}]')
            r = requests.get(
                listing,
                headers=self.get_headers(),
            )
            bs_obj = bs4.BeautifulSoup(r.text, features='lxml')
            extra_links_section = bs_obj.find_all('div', {'id': 'locationLinks'})[0]
            extra_links = extra_links_section.find_all('a', href=True)
            for link in extra_links:
                _href = link['href']
                if _href != '#':
                    extended_listings.add(_href)
            self._sleep()
        all_listings = list(base_listings.union(extended_listings))
        return all_listings


def main():
    s = OtoDom()
    # offers = s.scrape(limit_pages=3)
    try:
        offers = s.scrape()
    except:
        # this will log full trackeback message
        dfsds
        logger.exception('Got exception on main handler!')
        raise

    logger.debug(f'No of offers: {len(offers)}')
    logger.debug(f'Random offer: {random.choice(offers)}')
    
    s.store_offers(offers)
    logger.debug('Saved data')


if __name__ == '__main__':
    main()
