# Overview

Our objective is to predict a new venue's popularity from information available
when the venue opens.  We will do this by machine learning from a dataset of
venue popularities provided by Yelp.  The dataset contains meta data about the
venue (where it is located, the type of food served, etc ...).  It also
contains a star rating. Note that the venues are not limited to restaurants.
This tutorial will walk you through one way to build a machine-learning
algorithm.

## Metric

Your model will be assessed based on the root mean squared error of the number
of stars you predict.  There is a reference solution (which should not be too
hard to beat).  The reference solution has a score of 1.

## Download and parse the incoming data

[link](http://thedataincubator.s3.amazonaws.com/coursedata/mldata/yelp_train_academic_dataset_business.json.gz)

Notice that each row of the file is a json blurb.  You can read it in python.

### Hints:
1. [gzip.open](https://docs.python.org/2/library/gzip.html)) has the same
   interface as `open` but is for `.gz` files.
2. [ujson](http://docs.micropython.org/en/latest/library/ujson.html)) has the
   same interfase as the built-in `json` library, but is *substantially* faster
   (at the cost of non-robust handling of malformed json)

## Setup cross-validation:
In order to track the performance of your machine-learning, you might want to
use
[cross_validation.train_test_split](http://scikit-learn.org/stable/modules/generated/sklearn.cross_validation.train_test_split.html).

## Building models in sklearn

All estimators (e.g. linear regression, kmeans, etc ...) support `fit` and
`predict` methods.  In fact, you can build your own by inheriting from classes
in `sklearn.base` by using this template:

``` python
class Estimator(base.BaseEstimator, base.RegressorMixin):
    def __init__(self, ...):
        # initialization code

    def fit(self, X, y):
        # fit the model ...
        return self

    def predict(self, X):
        return # prediction
```

The intended usage is:

``` python
estimator = Estimator(...)  # initialize
estimator.fit(X_train, y_train)  # fit data
y_pred = estimator.predict(X_test)  # predict answer
estimator.score(X_test, y_test)  # evaluate performance
```

The regressor provides an implementation of `.score`.  Conforming to this
convention has the benefit that many tools (e.g. cross-validation, grid search)
rely on this interface so you can use your new estimators with the existing
`sklearn` infrastructure.

For example `grid_search.GridSearchCV`
([docs](http://scikit-learn.org/stable/modules/generated/sklearn.grid_search.GridSearchCV.html))
takes an estimator and some hyperparameters as arguments, and returns another
estimator.  Upon fitting, it fits the best model (based on the inputted
hyperparameters) and uses that for prediction.

Of course, we sometimes need to process or transform the data before we can do
machine-learning on it.  `sklearn` has Transformers to help with this.  They
implement this interface:

``` python
class Transformer(base.BaseEstimator, base.TransformerMixin):
    def __init__(self, ...):
        # initialization code

    def fit(self, X, y=None):
        # fit the transformation
        # ...
        return self

    def transform(self, X):
        return ... # transformation
```

When combined with our previous `estimator`, the intended usage is

``` python
transformer = Transformer(...)  # initialize
X_trans_train = transformer.fit_transform(X_train)  # fit / transform data
estimator.fit(X_trans_train, y_train)  # fit new model on training data
X_trans_test = transformer.transform(X_test)  # transform test data
estimator.score(X_trans_test, y_test)  # fit new model
```

Here, `.fit_transform` is implemented based on the `.fit` and `.transform`
methods in `sklearn.base.TransformerMixin`.  For many transformers, `.fit` is
empty and only `.transform` actually does something.

The real reason we use transformers is that we can chain them together with
pipelines.  For example, this

``` python
new_model = pipeline.Pipeline([('trans', Transformer(...)),
                               ('est', Estimator(...))
                              ])
new_model.fit(X_train, y_train)
new_model.score(X_test, y_test)
```

would replace all the fitting and scoring code above.  That is, the pipeline
itself is an estimator (and implements the `.fit` and `.predict` methods).
Note that a pipeline can have multiple transformers chained up but at most one
(optional) terminal estimator.


## A few helpful notes about performance and submitting in `__init__.py`.

1. To deploy a model, we suggest using the
   [`dill` library](https://pypi.python.org/pypi/dill) or
   [`joblib`](http://scikit-learn.org/stable/modules/model_persistence.html) to
   save it to disk and check it into git.  This allows you to train the model
   offline in another file but run it here by reading it in this file.  The
   model is way too complicated to be trained in real-time!
2. Make sure you load the `dill` file upon server start, not upon a call to
   the solution function.  This can be done by loading the model into the
   global scope. The model is too complicated to be even loaded in real-time!
3. Make sure you call `predict` once per call of the solution function, and that
   it returns a single number. For testing convenience you may want to allow it
   to work using a list of json dicts as input, but during grading the model
   will be passed a single JSON blob at a time.
4. You probably want to use GridSearchCV to find the best hyperparameters by
   splitting the data into training and test.  But for the final model that you
   submit, don't forget to retrain on all your data (training and test) with
   these best parameters.
5. GridSearchCV objects are capable of prediction, but they contain many
   versions of your model which you'll never use. From a deployment standpoint,
   it makes sense to only submit the best estimator once you've trained on the
   full data set. To troubleshoot deployment errors look
   [here](https://sites.google.com/a/thedataincubator.com/the-data-incubator-wiki/course-information-and-logistics/course/common-miniproject-errors). 

# Questions

## city_model
The venues belong to different cities.  You can image that the ratings in some
cities are probably higher than others and use this as an estimator.

Build an estimator that uses `groupby` and `mean` to compute the
average rating in that city.  Use this as a predictor.

**Note:** `def city_model` etc. takes an argument `record`.

## lat_long_model
You can imagine that a city-based model might not be sufficiently fine-grained.
For example, we know that some neighborhoods are trendier than others.  We
might consider a K Nearest Neighbors or Random Forest based on the latitude
longitude as a way to understand neighborhood dynamics.

You should implement a generic `ColumnSelectTransformer` that is passed which
columns to select in the transformer and use a non-linear model like
`sklearn.neighbors.KNeighborsRegressor` or
`sklearn.ensemble.RandomForestRegressor` as the estimator (why would you choose
a non-linear model?).  Bonus points if you wrap the estimator in
`grid_search.GridSearchCV` and use cross-validation to determine the optimal
value of the parameters.

## category_model
While location is important, we could also try seeing how predictive the
venues' category. Build a custom transformer that massages the data so that it
can be fed into a `sklearn.feature_extraction.DictVetorizer` which in turn
generates a large matrix gotten by One-Hot-Encoding.  Feed this into a Linear
Regression (and cross validate it!).  Can you beat this with another type of
non-linear estimator?

*Hints*:
  - With a large sparse feature set like this, we often use a cross-validated
    regularized linear model.
  - Some categories (e.g. Restaurants) are not very specific.  Others (Japanese
    sushi) are much more so.  How can we account for this in our model (*Hint:*
    look at TF-IDF).

## attribute_knn_model
Venues have (potentially nested) attributes:
```
    {'Attire': 'casual',
     'Accepts Credit Cards': True,
     'Ambience': {'casual': False, 'classy': False}}
```

Categorical data like this should often be transformed by a One Hot Encoding.
For example, we might flatten the above into something like this:

```
    {'Attire_casual' : 1,
     'Accepts Credit Cards': 1,
     'Ambience_casual': 0,
     'Ambience_classy': 0 }
```

Build a custom transformer that flattens attributes and feed this into
`DictVectorizer`.  Feed it into a (cross-validated) linear model (or something
else!)


## full_model
So far we have only built models based on individual features.  We could
obviously combine them.  One (highly recommended) way to do this is through a
`sklearn.pipelline.FeatureUnion`.

Combine all the above models using a feature union.  Notice that a feature
union takes transformers, not models as arguements.  The way around this is to
build a transformer that outputs the prediction in the transform method, thus
turning the model into a transformer.  Use a cross-validated linear regression
(or some other algorithm) to weight these signals.
