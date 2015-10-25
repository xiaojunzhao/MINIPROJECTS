from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq
WORD_RE = re.compile(r"\w+")
START_RE = re.compile('.*<page>.*')
END_RE = re.compile('.*</page>.*')
import xml.etree.ElementTree as ET
import Queue
import mwparserfromhell
import random
import math
import numpy as np

class q5(MRJob):
                  
    def mapper_getpage_init(self):
        self.queue = Queue.Queue()

    def mapper_getpage(self,_,line):
        self.queue.put(line)
        if END_RE.match(line):
            # empty the queue
            page = ''
            while not self.queue.empty():
                page += self.queue.get()
            if START_RE.match(page):
                yield ('page',page)
    
    def reducer_getpage(self,key,pages):
        pagecount = 0
        for page in pages:
            pagecount +=1
            yield ('page',(page,pagecount))
    

        
    def mapper_parser(self,key,val):        
        sum_number_links = 0
        reservoir,c = [],0
        k = 5000
        page, pagecount = val
        tree = ET.fromstring(page.encode('utf-8','ignore'))
        tagtext = [(x.tag, x.text) for x in tree.getiterator()]
        for tag,text in tagtext:
            if tag == 'text':
                if text:
                    wikilinks = mwparserfromhell.parse(text).filter_wikilinks()
                    wikilinks = [ links.encode('utf-8','ignore') for links in wikilinks]
                    x = len(set(wikilinks))
                    sum_number_links += x
                    if c < k:
                        reservoir.append(x)
                    else:
                        r=random.randint(0,c-1)
                        if r<k:reservoir[r] = x
                    c+=1
        yield None,(sum_number_links,pagecount,reservoir)
      
    def reducer_compute(self,_, vals):   
            sum_links = 0
            sum_sq = 0
            page_count_list = []
            reservoir_list = []
            for val in vals:
                link,pagecount,reservoir = val
                sum_links+=link
                sum_sq+=link**2
                page_count_list.append(pagecount)
                reservoir_list.append(reservoir)
            total_page = max(page_count_list)   
            counts = [ link_count for item in reservoir_list for link_count in item]
            count_list = sorted(counts)
            mean = sum_links*1.0/total_page
            std= math.sqrt(sum_sq/total_page-mean**2)
            yield ('page_count',int(total_page))
            yield ('mean',mean)
            yield ('std',std)        
            yield ('5%',count_list[int(round(len(count_list)*0.05)-1)])
            yield ('25%',count_list[int(round(len(count_list)*0.25)-1)])
            yield ('median',count_list[int(round(len(count_list)*0.5)-1)])
            yield ('75%',count_list[int(round(len(count_list)*0.75)-1)])
            yield ('95%',count_list[int(round(len(count_list)*0.95)-1)])
            #yield None,counts




    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_getpage_init,
                    mapper = self.mapper_getpage,
                    reducer = self.reducer_getpage),
           MRStep(
                   mapper = self.mapper_parser,
                   reducer=self.reducer_compute)
                ]

if __name__ == '__main__':
    q5.run()