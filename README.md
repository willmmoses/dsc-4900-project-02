# dsc-4900-project-02

## To Do:

1. Linear Regression
2. Random Forests
3. Support Vector Machines
4. Neural Networks
5. Results Comparison

## Data

The data come from the GDelt database, a comprehensive archive of events across the globe as viewed through the lens of
the media. The query this data came from contained solely the root events, and the following fields for each event.

* ``ActionGeo_ADM1Code`` - This is the 2-character FIPS10-4 country code followed by the 2-character FIPS10-4
  administrative division 1 (ADM1) code for the administrative division housing the landmark.
* ``Actor1Code``- The CAMEO code for the primary actor involved.
* ``Actor1Name`` - The name of the primary actor involved.
* ``AvgTone`` - The average sentiment of the articles written about the event. Positive sentiment is positive, negative
  is negative.
* ``EventCode`` - The CAMEO code for the event that transpired.
* ``GLOBALEVENTID`` - A unique ID for each event. Ignored in all models, but useful for tracking events.
* ``NumMentions`` - The number of mentions made of the event in the press.
* ``SQLDATE`` - The date the event occurred, in ``YYYYMMDD`` format.

## Preprocessing

//TODO

## Modeling

### Linear Regression

Linear regression modelling was achieved with the ``sklearn.linear_model.LinearRegression()`` function, providing an R2
of 0.238.

## Random Forests

Random forests were done with ``sklear.ensemble.RandomForestRegressor()``. Leaving it at the default configuration of
100 estimators, with unlimited depth, seemed to have had the best results. Increasing the estimator count resulted in a
lower R2, and with ``n_jobs`` set to -1, my 3900X made short work of the infinite depth, providing an R2 of 0.206

## Support Vector Machines

SVMs were surprisingly disappointing. No matter what I did, the R2 never got above 0.026. ``sklearn.svm.SVR()`` is good,
but not infallible.

## Neural Networks

Neural networks saw the most promising of outcomes, with an R2 of 0.239. This was achieved
with ``sklearn.neural_networks.MLPRegressor``, and the ``adam`` solver with an adaptive learning rate, a max of 1000
iterations, and early stopping.

## Conclusion

None of the models are particularly accurate. This could be due to several reasons:

* Not enough data (rows)
    * With limited compute time, and no GPU acceleration, I had to cut the dataset down to ~70,000 entries. There are
      over 700,000,000.
* Not enough data (columns)
    * Due to limited GCloud credit, I was forced to omit the majority of the fields. I downloaded 8 of the 58 fields.
      Granted, they seemed to be the most populated fields, but that was not based on any scientific merit.