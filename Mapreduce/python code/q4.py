from mrjob.job import MRJob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq
import Queue
import xml.etree.ElementTree as ET
import mwparserfromhell
import math

START_PAGE = re.compile('.*<page>*.')
END_PAGE = re.compile('.*</page>*.')
WORD_RE = re.compile(r"[\w]+")
THAI_RE = re.compile(r'[\u0E00-\u0EFF]+')
class FourthJob(MRJob):

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
    def mapper_parser_init(self):
        self.n=1


    def mapper_parser(self,key,page):
        tree = ET.fromstring(page.encode('utf-8'))
        text_tag = [(x.tag,x.text) for x in tree.getiterator()]
        #n_list = [1,2,3,4,5,10,15]
        #n_list = [1,2]
        for tag, text in text_tag:
            if tag=="text":
                if text:
                    wikicode = mwparserfromhell.parse(text)
                    pure_text = " ".join(" ".join(fragment.value.split()) for fragment in wikicode.filter_text())
                    pure_text_ws = " ".join(pure_text.split())
                    for n_gram in [pure_text_ws[i:i+10] for i in range(len(pure_text_ws))]:
                        yield (n_gram,1)

    
    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))
            
    def reducer_count_words(self, word, counts):
        item = sum(counts)
        p1 = item*math.log(item,2)
        yield word,(p1,item)

    # Job 3
    def mapper_entropy(self,key,vals):
        p1,count = vals
        yield None,(p1,count)

    def reducer_entropy(self,_,vals):
        total = 0
        p1_sum = 0
        for val in vals:
            p1,count = val
            total+=count
            p1_sum+=p1
        entropy = (math.log(total,2)-p1_sum/total)/10
        yield None,entropy

    

    def steps(self):
            return [
                MRStep(mapper_init=self.mapper_get_page_init,
                    mapper = self.mapper_get_page,
                   reducer=self.reducer_get_page),
                MRStep(mapper_init = self.mapper_parser_init,
                       mapper = self.mapper_parser,
                       combiner = self.combiner_count_words,
                       reducer = self.reducer_count_words),
                    MRStep(
                        mapper = self.mapper_entropy,
                        reducer = self.reducer_entropy),
                    ]


if __name__ =="__main__":
    FourthJob.run()
