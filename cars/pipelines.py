# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


class CarsPipeline(object):
    def process_item(self, item, spider):
        return item


from datetime import datetime
import csv
import os
import logging

OUT_CSV = '/home/hristiyan.bonev/crawl/cars/{}_data.csv'



class WriteToExcelPipeline(object):
    DATA_STORAGE = []

    def close_spider(self, spider):
        data_keys = list(set([x for y in WriteToExcelPipeline.DATA_STORAGE for x in y]))
        if not os.path.isfile(OUT_CSV.format(spider.name)):
            _create_csv(OUT_CSV.format(spider.name), data_keys)
        with open(OUT_CSV.format(spider.name), 'a') as outf:
            curr_datetime = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            writer = csv.DictWriter(outf, fieldnames=data_keys)
            for item in WriteToExcelPipeline.DATA_STORAGE:
                out_data = {
                    'date': curr_datetime,
                    **item
                }
                writer.writerow(out_data)
        logging.exception(
            '\n'.join('Manufacturer: %s -> Crawled count: %s' % (k, v) for k, v in spider.processed_cars_count.items()))

    def process_item(self, item, spider):
        WriteToExcelPipeline.DATA_STORAGE.append(item)
        return item


def _create_csv(file_name, headers):
    try:
        with open(file_name, 'w') as out_file:
            headers = [x for x in headers]
            if 'date' not in headers:
                headers.insert(0, 'date')
            writer = csv.DictWriter(out_file, fieldnames=headers)
            writer.writeheader()
    except (OSError, PermissionError):
        logging.exception(f"[ERROR] Cannot open {file_name}!")
