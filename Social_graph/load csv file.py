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
    return [item for item in list(set(a))]




input_file = csv.DictReader(open('captiondata.csv'))


caption_list = []
name_sublist = []
name_list = []
S = 0
#stop_words = set(stopwords.words("english"))
for row in input_file:    
    caption = row["caption"].split('//')
    S+=len(caption)
    for caption_item in caption:
        if len(caption_item)<250:
            caption_item = caption_item.decode('utf-8').strip().replace('\n',' ').replace('\t','')
            #caption_item = re.sub('[^A-Za-z\,]+', ' ', caption_item)      # remove all the special characters
            split_list = re.split(',|and|with|(|)|&',caption_item)                    
            name_sublist = filter(None, split_list)
            name_sublist = [item.strip() for item in name_sublist]
            name_sublist_filter = filter(lambda name: name.strip() and len(name.split(' '))<=4 and len(name)>=4 and name[0].isupper(),name_sublist) # remove whitespaces strings
            #S+=len(name_sublist)
            if name_sublist_filter:
                name_sublist_filter = list(set(name_sublist_filter))     # remove the identical person
                new_list = []
                c = []
                for item in name_sublist_filter:
                    if len(item.split()) ==1:
                        new_list.append(item)
                        #print new_list
                        continue
                    else:
                        last_name = item.split()[-1]
                        b = [first_name+" "+last_name for first_name in new_list]
                        new_list = []
                        c.extend(b)
                        c.append(item)
                #print c
                #name_list.append(name_sublist_filter)
                name_list.append(c)
                #print name_sublist_filter
        caption_list.append(caption_item)
#print S
#print name_list


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

#G.add_edge_from(list_tuple,weight = 1 )

# Question 1
degree_dict = G.degree()
table_degree = pd.Series(degree_dict)
sorted_table_degree = table_degree.order(ascending = False)
print sorted_table_degree[0:99]

#Question 2
pagerank_dict = nx.pagerank(G,alpha=0.85, personalization=None, max_iter=100, tol=1e-06, nstart=None, weight='weight', dangling=None)
table_pagerank = pd.Series(pagerank_dict)
sorted_table_pagerank = table_pagerank.order(ascending = False)
print sorted_table_pagerank[0:99]


#Question 3
weights = G.edges(data = True)
L = []
for (n1,n2,w) in weights:
    t = (n1,n2,w['weight'])
    L.append(t)
df = pd.DataFrame(L, columns=['node1', 'node2', 'weight'])
sorted_df = df.sort(['weight'],ascending = False)
print sorted_df[0:99]





