
# Overview
Time series prediction presents its own challenges which are different from
machine-learning problems.  Like many other classes of problems, it also
presents a number of special features which are common.

## Fetch the data:
http://s3.amazonaws.com/thedataincubator/coursedata/mldata/train.txt.gz

The columns of the data correspond to the
  - year
  - month
  - day
  - hour
  - temp
  - dew_temp
  - pressure
  - wind_angle
  - wind_speed
  - sky_code
  - rain_hour
  - rain_6hour
  - city

We will focus on using the temporal elements to predict the temperature.

## Cross-validation is Different

1. Cross validation is very different for time series than with other
   machine-learning problem classes.  In normal machine learning, we select a
   random subset of data as a validation set to estimate performance.  In time
   series, we have to consider the problem that we are trying to solve is often
   to predict a value in the future.  Therefore, the validation data always has
   to occur *after* the training data.  As a simple example, consider that it
   would not be very useful to have a predictor of tomorrow's temperature that
   depended on the temperature the day after.

   We usually handle this by doing a *sliding-window validation method*.
   That is, we train on the last $n$ data points and validate the prediction on
   the next $m$ data points, sliding the $n + m$ training / validation window
   in time.  In this way, we can estimate the parameters of our model.  To test
   the validity of the model, we might use a block of data at the end of our
   time series which is reserved for testing the model with the learned
   parameters.

2. Another concern is whether the time series results are predictive.  In
   economics and finance, we refer to this as the ergodicity assumption, that
   past behavior can inform future behavior.  Many wonder if past behavior in
   daily stock returns gives much predictive power for future behavior.

**Warning**: Feature generation is sometimes a little different for
time-series.  Usually, feature generation on a set is only based on data in
that training example (e.g. extracting the time of day of the temperature
measurement).  In time-series, we often want to use *lagged* data (the
temperature an hour ago).  The easiest way to do this is to do the feature
generation *before* making the training and validation split.

## Per city model:

It makes sense for each city to have it's own model.  Build a "groupby"
estimator that takes an estimator as an argument and builds the resulting
"groupby" estimator on each city.  That is, `fit` should fit a model per city
while the `predict` method should look up the corresponding model and perform a
predict on each, etc ...

# Submission                                                                                                                                                                                                 
Replace the return values in `__init__.py` with predictions from your models.                                                                                                                                
Avoid running "on-the-fly" computations or scripts in this file. Ideally you                                                                                                                                 
should load your pickled model from file (in the global scope) then call                                                                                                                                     
`model.predict(line)`.

# Questions

## month_hour_model
There are two ways to handle seasonality.  Seasonality features are nice
because they are good at projecting arbitrarily far into the future.

The simplest (and perhaps most robust) is to have a set of indicator variables
for each month. That is to say, make the assumption that the temperature at any
given time is a function of only the month of the year, and the hour of the
day, and use that to predict the temperature value.  As you can imagine, the
temperature values will be stripped out in the actual text records that are
passed.

**Question**: should month be a continuous or categorical variable?


## fourier_model
Since we know that temperature is roughly sinusoidal, we know that a reasonable
model might be 

    $$ y_t = k \sin(\frac{t - t_0}{T}) + \epsilon $$

where $k$ and $t_0$ are parameters to be learned and $T$ is one year for
seasonal variation.  While this is linear in $k$, it is not linear in $t_0$.
However, we know from Fourier analysis, that the above is
equivalent to 

    $$ y_t = A \sin(\frac{t}{T}) + B \cos(\frac{t}{T}) + \epsilon $$

which is linear in $A$ and $B$.  This can be solved using a linear regression.

