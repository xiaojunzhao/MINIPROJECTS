from mrjob.job import MRJob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq
import Queue
import xml.etree.ElementTree as ET
import mwparserfromhell

START_PAGE = re.compile('.*<page>*.')
END_PAGE = re.compile('.*</page>*.')
WORD_RE = re.compile(r"[\w]+")
class ThirdJob(MRJob):

    def mapper_get_page_init(self):
        self.pages = Queue.Queue()
        self.queue= Queue.Queue()

    def mapper_get_page(self,_,line):
        self.queue.put(line)
        if END_PAGE.match(line):
            page = ''
            while not self.queue.empty():
                page+=self.queue.get()
            if START_PAGE.match(page):
                yield ('page',page)

    def reducer_get_page(self,key,pages):
        for page in pages:
            yield ('page',page)

    # Job 2
    def mapper_parser(self,key,page):
        tree = ET.fromstring(page.encode('utf-8'))
        text_tag = [(x.tag,x.text) for x in tree.getiterator()]
        for tag, text in text_tag:
            if tag=="text":
                if text:
                    pure_text = re.sub(r'\([^)]*\)*|\<[^>]*\>*|\[[^]]*\]*|\{[^}]*\}*', '', text)
                    for word in WORD_RE.findall(pure_text):
                        yield(word.lower(),1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))
            
    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)
    
    # Job 3
    
    def init_get_100(self):
        self.counts = []

    def mapper_gettop100(self,_, word_count_pairs):
        heapq.heappush(self.counts, word_count_pairs)


    def mapper_final_top100 (self):        
        largest = heapq.nlargest(100,self.counts)
        for count,word in largest:
            yield ('heap',(count,word))

    def reducer_init(self):
        self.top100list = []


    def reducer_get_top100(self,key,top100):
        for word_count in top100:
            heapq.heappush(self.top100list, (word_count[0],word_count[1]))
    
    def reducer_final(self):
        largest = heapq.nlargest(100,self.top100list)
        words = [(word,  int(count)) for count,word in largest]
        yield (None, words)  

    
        
    def steps(self):
            return [
                MRStep(mapper_init=self.mapper_get_page_init,
                    mapper = self.mapper_get_page,
                   reducer=self.reducer_get_page),
                MRStep(mapper = self.mapper_parser,
                       combiner = self.combiner_count_words,
                       reducer = self.reducer_count_words),
                MRStep(mapper_init = self.init_get_100,
                       mapper = self.mapper_gettop100,
                       mapper_final = self.mapper_final_top100,
                       reducer_init = self.reducer_init,
                       reducer = self.reducer_get_top100,
                       reducer_final = self.reducer_final),
                    ]


if __name__ =="__main__":
    ThirdJob.run()
