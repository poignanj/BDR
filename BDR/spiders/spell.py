# -*- coding: utf-8 -*-
import scrapy
import re

from scrapy.http import Request

regexLevel = re.compile("^.*; Level ((([a-zA-Z/ ]+) ([0-9]+)(, )?)+)( |;|(CASTING)).*\.")
regexComponents = re.compile("^.*Components ([^\n]*)EFFECT.*\.")
regexSR = re.compile("^.*Spell Resistance ([^\n]*)DESCRIPTION.*\.")
regexNumeral = re.compile("^(.*) ([0-9])$")

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
        name = response.xpath('//h1//text()').get()
        #ALL THE TEEEEXT
        temp = ''.join(response.xpath('//div[has-class("article-content")]//*/text()').getall())

        #Levels
        leveltab =  regexLevel.match(temp)
        levels = {}
        for levelstemp in leveltab.group(1).split(', '):
            t = regexNumeral.match(levelstemp)   
            levels[t.group(1)] = t.group(2)

        #Components
        componenttab = regexComponents.match(temp)
        components=[]
        if(componenttab):
            c = componenttab.group(1).split('(')
            for compo in c[0].split(', '):
                if(compo.find('/')!=-1) :
                    l = compo.split("/")
                    for i in l:
                        if(i == 'M' and len(c)>1):
                            i+='('+c[1]
                        components.append(i)
                else:
                    if(compo == 'M' and len(c)>1): #exception for specific material components
                        compo+= '('+c[1]
                    components.append(compo)
        
        #Spell resistance
        SRtab = regexSR.match(temp)
        SR=""
        if(SRtab):
            SR = SRtab.group(1)
        yield {
            'name' : name,
            'levels' : levels,
            'components' : components if(componenttab) else [],
            'SR' : SR if(SRtab) else ""
        }
        