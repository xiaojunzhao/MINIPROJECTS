import scrapy
import datetime 

from social_graph.items import SocialGraphItem
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class socialdataSpider(CrawlSpider):
    name = "socialdata"
    allowed_domains = ["newyorksocialdiary.com"]
    base_url = "http://www.newyorksocialdiary.com/party-pictures"
    start_urls = []

    for i in range(0,26):
        start_url = base_url+"?page="+str(i)
        start_urls.append(start_url)

    rules = ( Rule(LinkExtractor(allow=('\/party-pictures\/\d+\/.+',),
              restrict_xpaths=('//span[@class = "field-content"]/a')),
             callback="parse_items",follow=True),)

    def parse_items(self, response):
        item = SocialGraphItem()
        
        date = response.xpath('//div[@class = "panel-pane pane-node-created"]/text()').extract()[0].strip().replace(',','')
        format_date = datetime.datetime.strptime(date,'%A %B %d %Y')
        if format_date<datetime.datetime.strptime("December 1 2014",'%B %d %Y'):
            item["caption"] ='%'.join(response.xpath('//div[@class = "photocaption"]/text()').extract())
            #item["date"] = date
        yield item


        
        
