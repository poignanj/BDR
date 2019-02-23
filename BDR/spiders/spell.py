# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.http import Request

regexClearHTML = re.compile(r"</?a[^>]*>") # replace by ""

regexExtractLevels = re.compile(r".*<b>Level</b>([^<]*)") #res in group 1
regexLevel = re.compile(r"(([a-zA-Z/][a-zA-Z/ ]*) ([0-9]+))") #each match a class+level || group 2 : class ; group 3 : level

regexExtractComponents = re.compile(r".*<b>Components</b>([^<]*)") #res in group 1
regexComponents = re.compile(r"([A-Z]+([^,/)]*\))?)") #each match is a comp

regexExtractSR = re.compile(r".*<b>Spell Resistance</b>([^<]*)") #res in group 1


class SpellSpider(scrapy.Spider):
    name = 'spell'
    allowed_domains = ['www.d20pfsrd.com']
    start_urls = ['https://www.d20pfsrd.com/magic/spell-lists-and-domains/spell-lists-sorcerer-and-wizard/']

    def parse(self, response):
        s_links = response.xpath('//tr/td[has-class("text")][1]/a/@href').getall()
        for s_link in s_links:
            yield Request(s_link, callback=self.parse_spell)
    
    def parse_spell(self, response):

        #name
        name = response.xpath('//h1//text()').get().encode("utf-8")
        #ALL THE TEEEEXT
        temp = ''.join(response.xpath('//div[has-class("article-content")]/*').getall()).encode("utf-8")
        #temp = response.xpath('//div[has-class("article-content")]/*').getall()).encode("utf-8")
        te = re.sub(regexClearHTML,"",temp)
        temp = re.sub(r" +"," ",te)
        te2 = "".join(temp.split("=\r\n"))
        te = "".join(te2.split("\r"))
        te2 = "".join(te.split("\n")) 
        
        #Levels
        leveltab = regexExtractLevels.match(te2)
        
        levels = {}
        try:
            l = regexLevel.findall(leveltab.group(1))
            
            for levelstemp in l:
                levels[levelstemp[1]] = levelstemp[2]
        except:
             print("Spell " + name+" : levels extract broke")
             pass
        
        #Components
        componenttab = regexExtractComponents.match(te)
        components=[]
        if(componenttab):
            c = componenttab.group(1)
            c1 = regexComponents.findall(c)
            for compo in c1:
                components.append(compo[0])
        
        #Spell resistance
        SRtab = regexExtractSR.match(te)
        SR=""
        if(SRtab):
            SR = SRtab.group(1)
        yield {
            'name' : name,
            'levels' : levels,
            'components' : components if(componenttab) else [],
            'SR' : SR if(SRtab) else ""
        }
        