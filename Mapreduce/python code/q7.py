from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq
import xml.etree.ElementTree as ET
import Queue
import mwparserfromhell
import random
import math



WORD_RE = re.compile(r"\w+")
START_RE = re.compile('.*<page>.*')
END_RE = re.compile('.*</page>.*')
nocontent = re.compile('(user:.*)|(user\stalk:)|(template:.*)|(category:.*)|(talk:.*)|(help:.*)|(file:.*)|(special:.*)|(user_talk:)')

class q7(MRJob):
                  
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
        for page in pages:
            yield ('page',page)
    

        
    def mapper_parser(self,key,page):        
        tree = ET.fromstring(page.encode('utf-8','ignore'))
        tagtext = [(x.tag, x.text) for x in tree.getiterator()]
        try:
            for tag,text in tagtext:
                if tag =="title":
                    if text:
                        if not nocontent.match(text):
                            title = text  #extract the title of the page
                elif tag == 'text':
                    if text:
                        wikilinks = mwparserfromhell.parse(text).filter_wikilinks()
                        wikilinks = [ links.encode('utf-8','ignore') for links in wikilinks]
                        # extract the title of the page that links connect to                  
                        link_title_list = list(set([str(mwparserfromhell.parse(link).filter_wikilinks()[0].title) for link in wikilinks if not nocontent.match(str(mwparserfromhell.parse(link).filter_wikilinks()[0].title))]))
                        for link in link_title_list:
                            if link!=title:
                                yield "M1",(title, link, 1.0/(len(link_title_list)+10.0))
                                yield "M2",(title, link, 1.0/(len(link_title_list)+10.0))
                            else:
                                continue
        except:
            pass
        
        
    def mapper_emit(self,key,val):
        row,col,v = val
        if key == "M1":
            yield col,(row,v)
        elif key=="M2":
            yield row,((col,v),)
            
            
    def multiply_values(self,j,values):
        brow=[]
        acol=[]
        for val in values:
            if len(val)==1:
                brow.append(val[0])
            else:
                acol.append(val)
                
        for (bcol,bval) in brow:
            for(arow,aval) in acol:
                yield ((arow,bcol),aval*bval)
    
    def reducer_sum(self,key,vals):
        yield key,sum(vals)
        
        
        
    def heap_init(self):
        self.h = []
        
    def mapper_add_to_head(self,key,val):
        heapq.heappush(self.h,(val,key))
        
    def mapper_pop_top_100(self):
        largest = heapq.nlargest(100,self.h)
        for count,word in largest:
            yield ('heap',(count,word))   
            
    def reducer_heap_init(self):
        self.h = []
        
    def reducer_heap_count_words(self, key,word_counts):
        for count,word in word_counts:
            heapq.heappush(self.h, (count,word))
    
    def reducer_pop_top_100(self):
        largest = heapq.nlargest(100,self.h)
        words = [(word,  count) for count,word in largest]
        yield (None, words) 
        
        
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_getpage_init,
                    mapper = self.mapper_getpage,
                    reducer = self.reducer_getpage
                  ),
           MRStep(
                   mapper = self.mapper_parser,
                 ),
            MRStep(
                   mapper = self.mapper_emit,
                   reducer = self.multiply_values
                  ),
            MRStep(reducer = self.reducer_sum),
            MRStep(mapper_init = self.heap_init,
                   mapper = self.mapper_add_to_head,
                   mapper_final = self.mapper_pop_top_100,
                   reducer_init = self.reducer_heap_init,
                   reducer = self.reducer_heap_count_words,
                   reducer_final = self.reducer_pop_top_100),
            #MRStep(reducer = self.reducer_tuple)
                ]

if __name__ == '__main__':
    q7.run()