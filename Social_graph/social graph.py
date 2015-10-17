import csv
import re
import networkx as nx
import pandas as pd



def create_edge_tuple(List):
    a = []
    for x in List:
        for y in List:
            if x !=y:
                a.append(tuple(list(set([x,y]))))
        List.remove(x)
    return [item for item in list(set(a))]




input_file = csv.DictReader(open('captiondata1.csv'))


caption_list = []
name_sublist = []
name_list = []
titles = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Mayor', 'CEO', 'M.D.', 'AMC', 'AOHT','ANDRUS', 'AOHT', 'ASF', 'ASPCA',\
         'ACO', 'ACC', 'ABT', 'ACandC', 'AFIPO', 'ALS', 'ALSGNY', 'AAADTs', 'AIA', 'AIS',\
         'Actress', 'Actresses', 'Actor', 'Actors', 'Author', 'Authors', 'Bad', 'C.B.E.', 'COO'\
         'Board Member','Photographs','Benefit Chairman', 'Benefit Chairmen', 'Benefit Chairs','CCBF Chairman',\
         'CCBF Doctors', 'CNBC', 'CUNY Chancellor', 'CSHL President', 'CSUN President', 'President', \
         'Vice President',  'Cardiologist', 'Miss New York', 'New York', 'COO'\
          'Board Member','Photographs','Benefit Chairman', 'Benefit Chairmen', 'Benefit Chairs','CCBF Chairman',\
         'CCBF Doctors', 'CNBC', 'CUNY Chancellor', 'CSHL President', 'CSUN President', 'President', \
         'Vice President',  'Cardiologist', 'Miss New York', 'New York', 'COO'
         ]

for row in input_file:    
    caption = row["caption"].split('%')
    for caption_item in caption:
        if len(caption_item)<250:
            caption_item = caption_item.decode('utf-8').strip().replace('\n',' ').replace('\t',' ')
            for word in titles:
                if word in caption_item:
                    caption_item = re.sub(word,'',caption_item)
            caption_item = re.sub('[^A-Za-z\,\& \.]+', ' ', caption_item)      # remove all the special characters
            split_list = re.split(',|and |with |& ',caption_item)                    
            name_sublist = filter(None, split_list)
            name_sublist = [item.strip() for item in name_sublist]
            name_sublist_filter = filter(lambda name: name.strip() and len(name.split(' '))<=4 and name[0].isupper(),name_sublist) # remove whitespaces strings
            if name_sublist_filter:
                #name_sublist_filter = list(set(name_sublist_filter))     # remove the identical person
                # deal with husband and wife case
                new_list = []
                c = []
                for item in name_sublist_filter:
                    if len(item.split(' ')) ==1:
                        new_list.append(item)
                        #print new_list
                        continue
                    else:
                        last_name = item.split(' ')[-1]
                        b = [first_name+" "+last_name for first_name in new_list]
                        new_list = []
                        c.extend(b)
                        c.append(item)
                name_list.append(c)
                #print name_sublist_filter
        caption_list.append(caption_item)

#Draw a Graph
G = nx.Graph()
node_list = [item for x in name_list for item in x]
new_node_list = list(set(node_list))
G.add_nodes_from(new_node_list)
list_tuple = []
for item in name_list:
    a = create_edge_tuple(item)
    for x in a:
        list_tuple.append(x)
#print list_tuple
for node_pair in list_tuple:
    if G.has_edge(node_pair[0],node_pair[1]):
        G[node_pair[0]][node_pair[1]]['weight']+=1            
    else:
        G.add_edge(node_pair[0],node_pair[1],weight = 1)


# Question 1
degree_dict = G.degree()
table_degree = pd.Series(degree_dict)
sorted_table_degree = table_degree.order(ascending = False)
sorted_list_degree = []
for i in range(0,100):
    Index = sorted_table_degree.index[i]
    sorted_list_degree.append((str(sorted_table_degree.index[i]),sorted_table_degree[Index])) 

#Question 2
pagerank_dict = nx.pagerank(G,alpha=0.85, personalization=None, max_iter=100, tol=1e-06, nstart=None, weight='weight', dangling=None)
table_pagerank = pd.Series(pagerank_dict)
sorted_table_pagerank = table_pagerank.order(ascending = False)
sorted_list_pagerank = []
for i in range(0,100):
    Index = sorted_table_pagerank.index[i]
    sorted_list_pagerank.append((str(sorted_table_pagerank.index[i]),sorted_table_pagerank[Index]))
#print sorted_list_pagerank

#Question 3
weights = G.edges(data = True)
L = []
for (n1,n2,w) in weights:
    t = (n1,n2,w['weight'])
    L.append(t)
df = pd.DataFrame(L, columns=['node1', 'node2', 'weight'])
sorted_df = df.sort(['weight'],ascending = False)

#print sorted_df[0:100]

best_friends = []
for name1,name2,weight in sorted_df[0:100].values:
    best_friends.append(((str(name1),str(name2)),weight))
print best_friends
len(best_friends)





