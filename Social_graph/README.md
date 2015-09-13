[New York Social Diary](http://www.newyorksocialdiary.com/) provides a
fascinating lens onto New York's socially well-to-do.  The data forms a natural
social graph for New York's social elite.  Take a look at this page of a recent
run-of-the-mill holiday party:

`http://www.newyorksocialdiary.com/party-pictures/2014/holiday-dinners-and-doers`

Besides the brand-name celebrities, you will notice the photos have carefully
annotated captions labeling those that appear in the photos.  We can think of
this as implicitly implying a social graph: there is a connection between two
individuals if they appear in a picture together.

Fetching the data can be broken down into two phases:

The first step is to crawl the data.  We want photos from parties before
December 1st, 2014.  Go to `http://www.newyorksocialdiary.com/party-pictures`
to see a list of (party) pages.  For each party's page, grab all the captions.

# Hints
  1. Click on the on the index page and see how they change the url.  Use this
     to determine a strategy to get all the data.
  2. Notice that each party has a date on the index page.  Use python's
     `datetime.strptime` function to parse it.
  3. Some captions are not useful: they contain long narrative texts that
     explain the event.  Usually in two stage processes like this, it is better
     to keep more data in the first stage and then filter it out in the second
     stage.  This makes your work more reproducible.  It's usually faster to
     download more data than you need now than to have to redownload more data
     later.
  4. To avoid having to re-scrape every time you run your code, you can
	 consider saving the data to disk, and having the parsing code load a file.
	 A checkpoint library like
	 [ediblepickle](https://pypi.python.org/pypi/ediblepickle/1.1.3) can
     streamline the process so that the time-consuming code will only be run
     when necessary.
  5. HTTP requests can sometimes fail inconsistently. You should expect to
     run into this issue and deal with it as best you can.

Now that you have a list of all captions, you should probably save the data on
disk so that you can quickly retrieve it.  Now comes the parsing part.
  1. Some captions are not useful: they contain long narrative texts that
     explain the event.  Try to find some heuristic rules to separate captions
     that are a list of names from those that are not.  A few heuristics
     include:
      - look for sentences (which have verbs) and as opposed to lists of nouns.
        For example, [nltk does part of speech
        tagging](http://www.nltk.org/book/ch05.html) but it is a little slow.
        There may also be heuristics that accomplish the same thing.
      - Look for commonly repeated threads (e.g. you might end up picking up
        the photo credits or people such as "a friend").
      - Long captions are often not lists of people.  The cutoff is subjective,
        but for grading purposes, *set that cutoff at 250 characters*.
  2. You will want to separate the captions based on various forms of
     punctuation.  Try using `re.split`, which is more sophisticated than
     `string.split`.
  3. You might find a person named "ra Lebenthal".  There is no one by this
     name.  Can anyone spot what's happening here?
  4. This site is pretty formal and likes to say things like "Mayor Michael
     Bloomberg" after his election but "Michael Bloomberg" before his election.
     Can you find other ('optional') titles that are being used?  They should
     probably be filtered out b/c they ultimately refer to the same person:
     "Michael Bloomberg."
  4. There is a special case you might find where couples are written as eg.
     "John and Mary Smith". You will need to write some extra logic to make
     sure this properly parses to two names: "John Smith" and "Mary Smith".
  5. When parsing names from captions, it can help to look at your output
     frequently and address the problems that you see coming up, iterating
     until you have a list that looks reasonable. This is the approach used
     in the reference solution. Because we can only asymptotically approach
     perfect identification and entity matching, we have to stop somewhere.

**Further considerations (not included in solution)**
  1. Who is Patrick McMullan and should he be included in the results? How would
     you address this?
  2. What else could you do to improve the quality of the graph's information?

For the analysis, we think of the problem in terms of a
[network](http://en.wikipedia.org/wiki/Computer_network) or a
[graph](http://en.wikipedia.org/wiki/Graph_%28mathematics%29).  Any time a pair
of people appear in a photo together, that is considered a link.  What we have
described is more appropriately called an (undirected)
[multigraph](http://en.wikipedia.org/wiki/Multigraph) with no self-loops but
this has an obvious analog in terms of an undirected [weighted
graph](http://en.wikipedia.org/wiki/Graph_%28mathematics%29#Weighted_graph).
In this problem, we will analyze the social graph of the new york social elite.

We recommend using python's `networkx` library.


# Questions

**Checkpoint**
Total number of names found: 113,031
Total number of pairs found: 196,245
Total number of raw captions: 113,325
Total number of valid captions: 102,736

## 1. degree
The simplest question to ask is "who is the most popular"?  The easiest way to
answer this question is to look at how many connections everyone has.  Return
the top 100 people and their degree.  Remember that if an edge of the graph has
weight 2, it counts for 2 in the degree.

**Checkpoint**
Top 100 .describe()
"count": 100.0
"mean": 109.96
"std": 52.4777817343
"min": 71.0
"25%": 79.75
"50%": 91.0
"75%": 120.25
"max": 373.0

## 2. pagerank
A similar way to determine popularity is to look at their
[pagerank](http://en.wikipedia.org/wiki/PageRank).  Pagerank is used for web
ranking and was originally
[patented](http://patft.uspto.gov/netacgi/nph-Parser?patentnumber=6285999) by
Google and is essentially the stationary distribution of a [markov
chain](http://en.wikipedia.org/wiki/Markov_chain) implied by the social graph.

Use 0.85 as the damping parameter so that there is a 15% chance of jumping to
another vertex at random.

**Checkpoint**
Top 100 .describe()
"count": 100.0
"mean": 0.0001841088
"std": 0.0000758068
"min": 0.0001238355
"25%": 0.0001415028
"50%": 0.0001616183
"75%": 0.0001972663
"max": 0.0006085816

## 3. best_friends
Another interesting question is who tend to co-occur with each other.  Give
us the 100 edges with the highest weights.

Google these people and see what their connection is.  Can we use this to
detect instances of infidelity?

**Checkpoint**
Top 100 .describe()
"count": 100.0
"mean": 25.84
"std": 16.0395470855
"min": 14.0
"25%": 16.0
"50%": 19.0
"75%": 29.25
"max": 109.0
