from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import bz2
import re
import heapq


WORD_RE = re.compile(r"[\w]+")
class FirstJob(MRJob):

    #Job 1
    def mapper_get_words (self,_,line):
        print line
        print 10 
        for word in WORD_RE.findall(line):
            yield (word.lower(),1)
    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))
            
    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)
    
    # Job 2
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
                MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
                MRStep(mapper_init = self.init_get_100,
                       mapper = self.mapper_gettop100,
                      mapper_final = self.mapper_final_top100,
                       reducer_init = self.reducer_init,
                       reducer = self.reducer_get_top100,
                       reducer_final = self.reducer_final)
                    ]


if __name__ =="__main__":
    FirstJob.run()
