[New York Social Diary](http://www.newyorksocialdiary.com/) provides a
fascinating lens onto New York's socially well-to-do.  The data forms a natural
social graph for New York's social elite.  

Besides the brand-name celebrities, you will notice the photos have carefully
annotated captions labeling those that appear in the photos.  We can think of
this as implicitly implying a social graph: there is a connection between two
individuals if they appear in a picture together.

In this Project, I explore the following interesting aspects

## 1. degree
The simplest question to ask is "who is the most popular"?  The easiest way to
answer this question is to look at how many connections everyone has.  Return
the top 100 people and their degree.  Remember that if an edge of the graph has
weight 2, it counts for 2 in the degree.


## 2. pagerank
A similar way to determine popularity is to look at their
[pagerank](http://en.wikipedia.org/wiki/PageRank).  Pagerank is used for web
ranking and was originally
[patented](http://patft.uspto.gov/netacgi/nph-Parser?patentnumber=6285999) by
Google and is essentially the stationary distribution of a [markov
chain](http://en.wikipedia.org/wiki/Markov_chain) implied by the social graph.


## 3. best_friends
Another interesting question is who tend to co-occur with each other.  Give
us the 100 edges with the highest weights.


