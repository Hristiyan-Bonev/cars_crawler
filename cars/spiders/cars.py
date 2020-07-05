from scrapy.spiders import Spider
from scrapy.http import Request, response, FormRequest
from datetime import datetime
import logging
import json
# from car_models import CARS_LIST
# from cars_moide
from cars.spiders.car_models import CARS_LIST
from ..features_lookup import FEATURES_LOOKUP


class NoTotalPagesException(Exception):
    pass


HEADERS = {
    'act': '3', 'rub': '1', 'pubtype': '1', 'marka': None,
    'topmenu': '1', 'model': None, 'sort': '1', 'nup': '01'
}


class CarsCrawlerMobile(Spider):
    name = 'carsbg'
    processed_cars_count = {}
    start_urls = [
        'https://www.mobile.bg/pcgi/mobile.cgi?{}'
        # 'https://variant-m.mobile.bg/'
    ]

    def parse(self, response: response):
        for manufacturer, models in CARS_LIST.items():
            # print(f'[#####] {manufacturer}')
            HEADERS['marka'] = manufacturer
            for model in models:
                HEADERS['model'] = model
                next_url = CarsCrawlerMobile.start_urls[0].format("&".join([f"{k}={v}" for k, v in HEADERS.items()]))
                # print(f'       [-----] {model}')
                yield Request(next_url, callback=self.parse_category,
                              meta={'manufacturer': manufacturer, 'model': model})

    def parse_category(self, response):
        total_pages = int(response.xpath('//span[contains(@class,"pageNumbersInfo")]/b/text()').re('\d+$')[0])
        ads_available = response.xpath('//table[contains(@class,"tablereset")]//a[contains(@href,"act=4")]')
        if not ads_available:
            logging.error(f"[#!#] No ads for | {response.meta['manufacturer']} {response.meta['model']} |")
            return {}
        logging.warning(
            f"Processing total of {len(ads_available)} posts for {response.meta['manufacturer']} {response.meta['model']}")
        CarsCrawlerMobile.processed_cars_count[
            response.meta['manufacturer']] = CarsCrawlerMobile.processed_cars_count.get(response.meta['manufacturer'],
                                                                                        0) + len(ads_available)
        url_without_page_number = "=".join(response.url.split('=')[:-1])
        for page_number in range(1, total_pages + 1):
            next_page = f'{url_without_page_number}={page_number}'
            yield Request(next_page, callback=self._parse_page, meta=response.meta)

    def _parse_page(self, response):
        ads = ["https:" + x for x in response.xpath('//td[@class="valgtop"]//a[@class="mmm"]/@href').extract()]
        for ad_url in ads:
            yield Request(ad_url, callback=self._parse_ad, meta=response.meta)

    def _parse_ad(self, response):
        price, title, *_ = sorted(response.xpath('//div/strong/text()').extract())
        car_details = dict(zip(response.xpath('//ul[@class="dilarData"]/li/text()').extract()[::2],
                               response.xpath('//ul[@class="dilarData"]/li/text()').extract()[1::2]))
        car_features = [x[2:] for x in
                        response.xpath('//div[contains(@style,"margin-bottom:") and contains(.,"•")]/text()').extract()]
        car_features_mapped = {x: 1 if x in car_features else 0 for x in FEATURES_LOOKUP}
        ad_description = "\n".join(
            response.xpath('//div[contains(.,"Допълнителна")]/following::table[1]/tr/td/text()').extract())
        yield {
            'manufacturer': response.meta['manufacturer'].upper(),
            'model': response.meta['model'].upper(),
            'date': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
            'ad_title': title,
            'price': price,
            'ad_description': ad_description,
            **car_details,
            **car_features_mapped,
        }
