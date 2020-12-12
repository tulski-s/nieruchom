# built in
import json
import random
import re
import time

# 3rd party
import requests
import bs4

# custom
import scraper


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

    def scrape(self, limit_pages=None):
        listing_and_type = zip(
            [self.flat_rent_listing_url, self.flat_sell_listing_url],
            ['rent', 'sell']
        )
        offers = []
        counter = 0
        for listing, _type in listing_and_type:
            for page_idx in range(1, self._get_no_pages(listing)+1):
                offers.extend(
                    self._get_offers(listing, _type, page_idx)
                )
                # sleep for random (real) time between <start, stop>
                time.sleep(random.uniform(1, 3))
                counter += 1
                if (limit_pages != None) and (counter >= limit_pages):
                    return self._dedup_offers(offers)
        return self._dedup_offers(offers) 

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
        print(f'Getting offers from: {listing}, page: {page_idx}')
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
            elif listing_type == 'sell':
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


def main():
    s = OtoDom()
    # offers = s.scrape(limit_pages=3)
    offers = s.scrape()
    print('No of offers: ', len(offers))
    print('Random offer: ', random.choice(offers))
    s.store_offers(offers)
    print('Saved data')


if __name__ == '__main__':
    main()

