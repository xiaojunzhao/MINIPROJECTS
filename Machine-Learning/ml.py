
# coding: utf-8

# In[1]:

import gzip
import ujson
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import pandas as pd
import dill
from sklearn.neighbors import KNeighborsRegressor
from sklearn import grid_search
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.feature_extraction import DictVectorizer
from sklearn import linear_model



# In[2]:

with gzip.open('yelp_train_academic_dataset_business.json.gz', 'rb') as f:
    file_content = f.read()
a = ujson.dumps(file_content,encode_html_chars=True)
b = ujson.loads(a)
b = b.replace('\n',',').replace('&','and')
List = '['+b[:-1]+']'
Data = ujson.loads(List)
df = pd.DataFrame(Data)

#Data


# # Quesstion 1 city_model

# In[3]:

subset = df[['city', 'stars']]
#tuples = [tuple(x) for x in subset.values]

average_subset = subset.groupby(['city'])['stars'].mean()
#print average_subset
average_subset[['Anthem']][0]


# In[4]:

# Define the mean estimator


class MyEstimator(BaseEstimator, RegressorMixin):
    def __init__(self):
        pass
        

    def fit(self,data):
        self.average = data.groupby(['city'])['stars'].mean()
        return self

    def predict(self, record):
        city = record['city']
        if city not in self.average.index.values:
            self.prediction = self.average.mean()     
        else:
             self.prediction = self.average[[city]][0]
        return self.prediction


# In[5]:

test_json = [
    {"business_id": "vcNAWiLM4dR7D2nwwJ7nCA", "full_address": "4840 E Indian School Rd\nSte 101\nPhoenix, AZ 85018", "hours": {"Tuesday": {"close": "17:00", "open": "08:00"}, "Friday": {"close": "17:00", "open": "08:00"}, "Monday": {"close": "17:00", "open": "08:00"}, "Wednesday": {"close": "17:00", "open": "08:00"}, "Thursday": {"close": "17:00", "open": "08:00"}}, "open": True, "categories": ["Doctors", "Health & Medical"], "city": "Phoenix", "review_count": 7, "name": "Eric Goldberg, MD", "neighborhoods": [], "longitude": -111.98375799999999, "state": "AZ", "stars": 3.5, "latitude": 33.499313000000001, "attributes": {"By Appointment Only": True}, "type": "business"},
    {"business_id": "vcNAWiLM4dR7D2nwwJ7nCA", "full_address": "4840 E Indian School Rd\nSte 101\nPhoenix, AZ 85018", "hours": {"Tuesday": {"close": "17:00", "open": "08:00"}, "Friday": {"close": "17:00", "open": "08:00"}, "Monday": {"close": "17:00", "open": "08:00"}, "Wednesday": {"close": "17:00", "open": "08:00"}, "Thursday": {"close": "17:00", "open": "08:00"}}, "open": True, "categories": ["Doctors", "Health & Medical"], "city": "ABC", "review_count": 7, "name": "Eric Goldberg, MD", "neighborhoods": [], "longitude": -111.98375799999999, "state": "AZ", "stars": 3.5, "latitude": 33.499313000000001, "attributes": {"By Appointment Only": True}, "type": "business"},
    {"business_id": "JwUE5GmEO-sH1FuwJgKBlQ", "full_address": "6162 US Highway 51\nDe Forest, WI 53532", "hours": {}, "open": True, "categories": ["Restaurants"], "city": "De Forest", "review_count": 26, "name": "Pine Cone Restaurant", "neighborhoods": [], "longitude": -89.335843999999994, "state": "WI", "stars": 4.0, "latitude": 43.238892999999997, "attributes": {"Take-out": True, "Good For": {"dessert": False, "latenight": False, "lunch": True, "dinner": False, "breakfast": False, "brunch": False}, "Caters": False, "Noise Level": "average", "Takes Reservations": False, "Delivery": False, "Ambience": {"romantic": False, "intimate": False, "touristy": False, "hipster": False, "divey": False, "classy": False, "trendy": False, "upscale": False, "casual": False}, "Parking": {"garage": False, "street": False, "validated": False, "lot": True, "valet": False}, "Has TV": True, "Outdoor Seating": False, "Attire": "casual", "Alcohol": "none", "Waiter Service": True, "Accepts Credit Cards": True, "Good for Kids": True, "Good For Groups": True, "Price Range": 1}, "type": "business"},
    {"business_id": "uGykseHzyS5xAMWoN6YUqA", "full_address": "505 W North St\nDe Forest, WI 53532", "hours": {"Monday": {"close": "22:00", "open": "06:00"}, "Tuesday": {"close": "22:00", "open": "06:00"}, "Friday": {"close": "22:00", "open": "06:00"}, "Wednesday": {"close": "22:00", "open": "06:00"}, "Thursday": {"close": "22:00", "open": "06:00"}, "Sunday": {"close": "21:00", "open": "06:00"}, "Saturday": {"close": "22:00", "open": "06:00"}}, "open": True, "categories": ["American (Traditional)", "Restaurants"], "city": "De Forest", "review_count": 16, "name": "Deforest Family Restaurant", "neighborhoods": [], "longitude": -89.353437, "state": "WI", "stars": 4.0, "latitude": 43.252267000000003, "attributes": {"Take-out": True, "Good For": {"dessert": False, "latenight": False, "lunch": False, "dinner": False, "breakfast": False, "brunch": True}, "Caters": False, "Noise Level": "quiet", "Takes Reservations": False, "Delivery": False, "Parking": {"garage": False, "street": False, "validated": False, "lot": True, "valet": False}, "Has TV": True, "Outdoor Seating": False, "Attire": "casual", "Ambience": {"romantic": False, "intimate": False, "touristy": False, "hipster": False, "divey": False, "classy": False, "trendy": False, "upscale": False, "casual": True}, "Waiter Service": True, "Accepts Credit Cards": True, "Good for Kids": True, "Good For Groups": True, "Price Range": 1}, "type": "business"},
    {"business_id": "LRKJF43s9-3jG9Lgx4zODg", "full_address": "4910 County Rd V\nDe Forest, WI 53532", "hours": {"Monday": {"close": "22:00", "open": "10:30"}, "Tuesday": {"close": "22:00", "open": "10:30"}, "Friday": {"close": "22:00", "open": "10:30"}, "Wednesday": {"close": "22:00", "open": "10:30"}, "Thursday": {"close": "22:00", "open": "10:30"}, "Sunday": {"close": "22:00", "open": "10:30"}, "Saturday": {"close": "22:00", "open": "10:30"}}, "open": True, "categories": ["Food", "Ice Cream & Frozen Yogurt", "Fast Food", "Restaurants"], "city": "De Forest", "review_count": 7, "name": "Culver's", "neighborhoods": [], "longitude": -89.374983, "state": "WI", "stars": 4.5, "latitude": 43.251044999999998, "attributes": {"Take-out": True, "Wi-Fi": "free", "Takes Reservations": False, "Delivery": False, "Parking": {"garage": False, "street": False, "validated": False, "lot": True, "valet": False}, "Wheelchair Accessible": True, "Attire": "casual", "Accepts Credit Cards": True, "Good For Groups": True, "Price Range": 1}, "type": "business"},
    {"business_id": "RgDg-k9S5YD_BaxMckifkg", "full_address": "631 S Main St\nDe Forest, WI 53532", "hours": {"Monday": {"close": "22:00", "open": "11:00"}, "Tuesday": {"close": "22:00", "open": "11:00"}, "Friday": {"close": "22:30", "open": "11:00"}, "Wednesday": {"close": "22:00", "open": "11:00"}, "Thursday": {"close": "22:00", "open": "11:00"}, "Sunday": {"close": "21:00", "open": "16:00"}, "Saturday": {"close": "22:30", "open": "11:00"}}, "open": True, "categories": ["Chinese", "Restaurants"], "city": "De Forest", "review_count": 3, "name": "Chang Jiang Chinese Kitchen", "neighborhoods": [], "longitude": -89.343721700000003, "state": "WI", "stars": 4.0, "latitude": 43.2408748, "attributes": {"Take-out": True, "Has TV": False, "Outdoor Seating": False, "Attire": "casual"}, "type": "business"}
    
]


# In[6]:

# check!
cityestimator = MyEstimator()
cityestimator.fit(subset)
prediction = cityestimator.predict(test_json[3])
prediction.mean()


# In[7]:


dill.dump(cityestimator, open("city_model","w")) 
q1_estimator = dill.load(open("city_model")) 
q1_estimator.predict(test_json[1])


# #Question #2 Lat_lon_model

# In[8]:

## Create a dataframe

lat_lon_df = df[['latitude','longitude','stars']] 
#print lat_lon_df['latitude'].values.tolist()


# In[9]:

## Define a Transformer
from sklearn.base import BaseEstimator, TransformerMixin
class ColumnSelector(BaseEstimator, TransformerMixin):
    def __init__(self,key):
        self.key = key
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[self.key].values.tolist()


# In[10]:

lat_selector = ColumnSelector('latitude')
lon_selector = ColumnSelector('longitude')
latitude_list = lat_selector.transform(df)
longitude_list =lon_selector.transform(df)
X = []
for i in range(0,len(latitude_list)):
    X.append([longitude_list[i],latitude_list[i]])
#X

star_selector = ColumnSelector('stars')
Y = star_selector.transform(df)
#Y


# In[15]:

# define a knnestimator
class knnestimator(BaseEstimator, RegressorMixin):
    def __init__(self):
        self.neigh = KNeighborsRegressor(n_neighbors=2)
        
        
    def fit(self,X,Y):  
        self.parameters = {'leaf_size':range(10,41),'metric':['minkowski'],'n_neighbors':range(1,21)}
        self.clf = grid_search.GridSearchCV(self.neigh, self.parameters,cv=3)
        self.clf.fit(X, Y)
        self.best_estimator = self.clf.best_estimator_ 
        return self.best_estimator

    def predict(self, record): 
        value = self.best_estimator.predict([record['longitude'],record['latitude']])[0]
        return value

#neigh = KNeighborsRegressor(n_neighbors=2)
#neigh.get_params
#parameters = {'leaf_size':range(10,41),'metric':['minkowski'],'n_neighbors':range(1,21)}
#clf = grid_search.GridSearchCV(neigh, parameters,cv=3)
#clf.fit(X, Y)
#best_estimator = clf.best_estimator_ 
#best_estimator.predict([test_json[0]['longitude'],test_json[0]['latitude']])

#best_estimator


# In[16]:

q2_estimator = knnestimator()
q2_estimator.fit(X,Y)
q2_estimator.predict(test_json[0])


# In[17]:

dill.dump(q2_estimator, open("Q2","w")) 
q2_estimator = dill.load(open("Q2")) 
prediction = q2_estimator.predict(test_json[1])
prediction


# # Question 3 Categorical model
# 

# In[18]:

## Define a Transformer

class customTransformer(BaseEstimator, TransformerMixin):
    def __init__(self,key):
        self.key = key
        pass

    def fit(self, X, y=None):
        #select column
        Feature = X[self.key].values.tolist()
        category_list = []
        feature_str = ''
        for item in Feature:
            for sub_item in item:
                feature_str = feature_str+sub_item+' '   
            category_list.append(feature_str)
            feature_str = ''
        self.v = TfidfVectorizer(min_df=1)
        self.my_features = self.v.fit_transform(category_list)
        return self

    def transform(self, X):
        self.feature_vec = []
        feature_str=''       
        for item in X[self.key]:
            feature_str = feature_str+item+' '
        self.feature_vec.append(feature_str)
        #v = TfidfVectorizer(min_df=1)
        self.my_features = self.v.transform(self.feature_vec).A
        return self.my_features
    
# Test customer transform
Transformer = customTransformer('categories') 
Transform_object = Transformer.fit(df) 


# In[19]:



class lrEstimator(BaseEstimator, RegressorMixin):
    def __init__(self):
        self.lr = linear_model.LinearRegression()  

    def fit(self):    
        self.Transformer = customTransformer('categories')
        Transform_object = self.Transformer.fit(df)
        X=Transform_object.my_features
        y = Y
        self.Q3 = self.lr.fit(X, y)
        return self

    def predict(self, record): 
        X = self.Transformer.transform(record)
        value = self.Q3.predict(X)[0]
        return value


# In[20]:

# test estimator class
estimator = lrEstimator()
q3_estimator = estimator.fit()
dill.dump(q3_estimator, open("Q3","w")) 
q3_estimator = dill.load(open("Q3")) 
q3_estimator.predict(test_json[0])




# # Question 4 Attribute model

# In[111]:

# Test  flatten function
test_dic = {u'attributes': {u'Accepts Credit Cards': True,
   u'Alcohol': u'none',
   u'Ambience': {u'casual': False,
    u'classy': False,
    u'divey': False,
    u'hipster': False,
    u'intimate': False,
    u'romantic': False,
    u'touristy': False,
    u'trendy': False,
    u'upscale': False},
   u'Attire': u'casual',
   u'Caters': False,
   u'Delivery': False,
   u'Good For': {u'breakfast': False,
    u'brunch': False,
    u'dessert': False,
    u'dinner': False,
    u'latenight': False,
    u'lunch': True}}}
    

def flatten_dictionary(d):
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dictionary(value).items():
                    yield key + "_" + subkey, subvalue
            elif isinstance(value,bool):
                yield key, value
            elif isinstance(value, str):
                yield key+'_'+value,1
            else:
                yield key,value
    return dict(items())

flattened = flatten_dictionary(test_dic)
#flattened


# In[21]:

# Build a flatten transformer

class flattentransformer(BaseEstimator, TransformerMixin):
    def __init__(self,key):
        self.key = key
        
    def flatten_dictionary(self,d):
        def items():
            for key, value in d.items():
                if isinstance(value, dict):
                    for subkey, subvalue in self.flatten_dictionary(value).items():
                        yield key + "_" + subkey, subvalue
                elif isinstance(value,bool):
                    yield key, value
                elif isinstance(value, str):
                    yield key+'_'+value,1
        return dict(items())    
    
    def fit(self, X, y=None):
        #select column   X is the Data
        attribute_list = []
        for record in X:    
            attribute_dict = self.flatten_dictionary(record[self.key])
            attribute_list.append(attribute_dict)
        self.v = DictVectorizer(sparse=False)
        self.my_features = self.v.fit_transform(attribute_list)
        return self

    def transform(self,X):
        self.feature_vec = self.flatten_dictionary(X[self.key])
        self.single_feature = self.v.transform(self.feature_vec)
        return self.single_feature


# In[126]:

# Test flatten transformer
mytransformer = flattentransformer('attributes')
mytransformer.fit(Data).my_features.shape
#mytransformer.transform(test_json[2]).shape


# In[22]:

# define an estimator

class Q4Estimator(BaseEstimator, RegressorMixin):
    def __init__(self):
        self.clf = linear_model.RidgeCV(alphas=[0.1, 1.0, 10.0])  

    def fit(self,):    
        self.Transformer = flattentransformer('attributes')
        Transform_object = self.Transformer.fit(Data)
        X=Transform_object.my_features
        y = Y
        self.Q4 = self.clf.fit(X, Y)
        return self

    def predict(self, record): 
        X = self.Transformer.transform(record)
        value = self.Q4.predict(X)[0]
        return value

   


# In[23]:


q4=Q4Estimator()
q4_estimator = q4.fit()
dill.dump(q4_estimator,open('Q4','w'))
q4_estimator = dill.load(open("Q4")) 
q4_estimator.predict(test_json[0])


# # Question 5 Full Model

# In[63]:

from sklearn.pipeline import Pipeline, FeatureUnion

#q1_estimator = dill.load(open("city_model")) 
#q2_estimator = dill.load(open("Q2")) 
#q3_estimator = dill.load(open("Q3")) 
#q4_estimator = dill.load(open("Q4"))

class UnionTransformer(BaseEstimator, TransformerMixin):
    def __init__(self,model_name):
        self.estimator = dill.load(open(model_name))
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        self.prediction = self.estimator.predict(X)
        return self.prediction

    
# Define an q5Estimator
class Q5Estimator(BaseEstimator, RegressorMixin):
    def __init__(self):
        self.lr5 = linear_model.RidgeCV(alphas=[0.1, 1.0, 10.0])  

    def fit(self,X):    
        self.uniontransformer1 = UnionTransformer("city_model")
        self.uniontransformer2 = UnionTransformer("Q2")
        self.uniontransformer3 = UnionTransformer("Q3")
        self.uniontransformer4 = UnionTransformer("Q4")
        self.combined_features = FeatureUnion([
                                 ("uf1", self.uniontransformer1), ("uf2", self.uniontransformer2),\
                                 ("uf3",self.uniontransformer3),("uf4", self.uniontransformer4)
                                 ])
        X_feature_mat=[]
        Y_label_list = []
        for item in X:
            X_features = self.combined_features.transform(item)     
            X_feature_mat.append(X_features)
            Y_label_list.append(item['stars'])
        #print item
        #print X_features
        self.q5_estimator = self.lr5.fit(X_feature_mat,Y_label_list)
        return self

    def predict(self, record): 
        X = self.combined_features.transform(record)
        value = self.q5_estimator.predict(X)[0]
        return value


# In[66]:

full_estimator = Q5Estimator()
q5_estimator = full_estimator.fit(Data)
dill.dump(q5_estimator,open('Q5','w'))


# In[67]:

q5_estimator = dill.load(open('Q5'))
print q5_estimator.predict(test_json[0])
print test_json[0]['stars']

