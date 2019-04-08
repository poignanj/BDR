# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request

class MonsterSpider(scrapy.Spider):
    name = 'monster'
    allowed_domains = ['www.d20pfsrd.com']
    start_urls = ['https://www.d20pfsrd.com/bestiary/bestiary-hub/monsters-by-cr']

    def parse(self, response):
        m_links = response.xpath('//article//tbody/tr/td/a//@href').getall()
        for m_link in m_links:
            yield Request(m_link, callback=self.parse_monster)

    def parse_monster(self, response):
        name = response.xpath('//h1//text()').getall()#.encode("utf-8")
        spells = list(set(response.xpath('//a[has-class("spell")]//text()').getall()))
        yield {
            'name' : name,
            'spells' : spells
        }


    #def parse_spell(self, response):
    #    name = response.xpath('//h1//text()').getall()#.encode("utf-8")
    #    print(name)
