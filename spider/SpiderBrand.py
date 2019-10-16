# -*- coding: utf-8 -*-
import scrapy


class SpiderbrandSpider(scrapy.Spider):
    name = 'SpiderBrand'
    allowed_domains = ['http//api.cheegu.com']
    start_urls = ['http://http//api.cheegu.com/']

    def parse(self, response):
        pass
