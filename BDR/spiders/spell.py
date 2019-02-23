# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.http import Request

regexLevel = re.compile("(([a-zA-Z/][a-zA-Z/ ]*) ([0-9]+))") #each match a class+level || group 2 : class ; group 3 : level
regexComponents = re.compile("[A-Z]+([^,)]*\))?") #each match is a comp
#regexSR = re.compile("^.*Spell Resistance ([^\n]*)DESCRIPTION.*\.") # muda muda muda
regexNumeral = re.compile("^(.*) ([0-9])$") # mudada ?


regexClearHTML = re.compile("</?a[^>]*>") # replace by ""
regexExtractLevels = re.compile("<b>Level</b>")#([^<]*)".encode("utf-8")) #res in group 1
regexExtractComponents = re.compile("<b>Components</b>([^<]*)") #res in group 1
regexExtractSR = re.compile("<b>Spell Resistance</b>([^<]*)") #res in group 1


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
        temp = ' '.join(response.xpath('//div[has-class("article-content")]/*').getall()).encode("utf-8")
        te = re.sub(regexClearHTML,"",temp)
        #print(te)
        print(regexExtractLevels.pattern)
        #Levels
        leveltab = regexExtractLevels.match(te)
        print(name + " : " + leveltab)
        levels = {}
        try:
            
            print(name + " : " + leveltab.groupdict)
            l = regexLevel.match(leveltab.group(1))
            for levelstemp in l:
                levels[levelstemp.group(2)] = levelstemp.group(3)
        except:
            #print("Spell " + name+" broke : \n" + te +"\n" )
            pass
        
        #Components
        componenttab = regexExtractComponents.match(te)
        components=[]
        if(componenttab):
            c = componenttab.group(1)
            c1 = regexComponents.match(c)
            for compo in c1:
                components.append(compo)
        
        #Spell resistance
        SRtab = regexExtractSR.match(te).group(1)
        SR=""
        if(SRtab):
            SR = SRtab.group(1)
        yield {
            'name' : name,
            'levels' : levels,
            'components' : components if(componenttab) else [],
            'SR' : SR if(SRtab) else ""
        }
        