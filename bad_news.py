import os
import time

import dask.dataframe as dd
import pandas as pd
# import sklearn.linear_model
import sklearn.ensemble
import sklearn.linear_model as sklm
import sklearn.preprocessing as skpp
import sklearn.compose as skcp
import vaex
import vaex.ml
import vaex.ml.sklearn

vaex.settings.main.thread_count = 20


def cleaner(folder_glob):
    path = os.path.join(os.getcwd(), "cleaned")
    print(path)
    for file in folder_glob:
        print(os.path.basename(file))
        open_file = open(file, "r")
        new_file = open(os.path.join(path, os.path.basename(file)), "w")
        for line in open_file:
            if not (line.__contains__(",,")):
                new_file.write(line)
        open_file.close()
        new_file.close()


def reader(folder_glob):
    df = pd.DataFrame()
    counter = 0
    for file in folder_glob:
        print("%d\t File name = ", counter, file)
        temp_df = pd.read_parquet(file, low_memory=False,
                                  dtype={'Actor1Code': str, 'Actor1Name': str, 'EventCode': str})
        temp_df['EventType'] = temp_df.EventCode.str[:2]
        temp_df['EventDetails'] = temp_df.EventCode.str[2:]
        temp_df['EventCountry'] = temp_df.ActionGeo_ADM1Code.str[:2]
        temp_df['EventRegion'] = temp_df.ActionGeo_ADM1Code.str[2:]
        temp_df = temp_df.set_index('SQLDATE')
        df = pd.concat([df, temp_df])
    print(df.info())
    daskframe = dd.from_pandas(df)
    print(df.describe())
    return daskframe


def dd_reader(folder):
    df = dd.read_parquet(folder,
                         dtype={'Actor1Code': str,
                                'Actor1Name': str,
                                'EventCode': str,
                                'ActionGeo_ADM1Code': str})
    df['EventType'] = df.EventCode.str[:2].astype(int)
    df['EventDetails'] = df.EventCode.str[2:].astype(int)
    df['EventCountry'] = df.ActionGeo_ADM1Code.str[:2].astype(int)
    df['EventRegion'] = df.ActionGeo_ADM1Code.str[2:].astype(int)
    df = df.dropna(how='any')
    return df


def vx_reader(folder):
    print("Starting read")
    read_time = time.time()
    df = vaex.open(folder,
                   dtype={'Actor1Code': str,
                          'Actor1Name': str,
                          'EventCode': str,
                          'ActionGeo_ADM1Code': str,
                          'SQLDATE': str})
    print("Read completed in", time.time() - read_time, "seconds")
    print("Starting cleanup")
    clean_time = time.time()
    df.dropna()
    df = df[df.NumMentions > 5000]
    df = df[df.SQLDATE > 20000000]
    df['EventType'] = df.EventCode.str.slice(start=0, stop=2)
    df['EventDetails'] = df.EventCode.str.slice(start=2)
    df['EventCountry'] = df.ActionGeo_ADM1Code.str.slice(start=0, stop=2)
    df['EventRegion'] = df.ActionGeo_ADM1Code.str.slice(start=2)
    # df['Year'] = df.SQLDATE.str.slice(start=0, stop=4)
    # df['Month'] = df.SQLDATE.str.slice(start=4, stop=6)
    # df['Month'] = df.Month.astype('str')
    df = df.drop(['EventCode', 'ActionGeo_ADM1Code'])
    df = df.dropna()
    # df = df[df.NumMentions > 500]
    print("Cleanup completed in", time.time() - clean_time, "seconds")
    # print(df.head(5))
    return df


def stats(df, columns):
    print("Total Entries:", df.count("*"))
    for col in columns:
        print(col, " median:", df.mean(df[col]), " min, max:", df.minmax(df[col]))
    print(df.head(5))
    print("Stats Done")


def scatter_plotter(df):
    plot_time = time.time()
    mentions_map = df.viz.heatmap(df.NumMentions, df.AvgTone, limits='95%')
    mentions_map.figure.savefig('test.png')
    print("Plot done in", time.time() - plot_time, "seconds")


def one_hot(df):
    print("Starting one hot encoding")
    features = ['EventType', 'EventDetails', 'EventCountry']
    target = 'AvgTone'
    one_hot_encoder = vaex.ml.OneHotEncoder(features=features)
    one_hot_df = one_hot_encoder.fit_transform(df)
    model = sklearn.linear_model.LinearRegression()
    print("Starting fit")
    fit_time = time.time()
    vaex_model = vaex.ml.sklearn.Predictor(features=features, target=target, model=model, prediction_name='prediction')
    vaex_model.fit(df=one_hot_df)
    print("One hot fitting done in", time.time() - fit_time, "seconds")
    print("Starting one hot transform")
    transform_time = time.time()
    fitted_df = vaex_model.transform(df)
    print("One hot transform done in", time.time() - transform_time, "seconds")
    # print(fitted_df.head(5))
    return fitted_df


def random_forests(features, target, df):
    print("Starting random forests")
    # fit_time = time.time()
    forest_time = time.time()
    model = sklearn.ensemble.RandomForestRegressor()
    print("Starting fit")
    vaex_model = vaex.ml.sklearn.Predictor(features=features, target=target, model=model, prediction_name='rf')
    vaex_model.fit(df=df)
    print("Random forest fit done in", time.time() - forest_time, "seconds")
    transform_time = time.time()
    fitted_df = vaex_model.transform(df)
    print("Linear regression transform done in", time.time() - transform_time, "seconds")
    return fitted_df


def main():
    folder = "data"
    path = os.path.join(os.getcwd(), folder, "*")
    df = vx_reader(path)
    features = ['EventType', 'EventDetails', 'EventCountry', 'Actor1Code', 'Actor1Name', 'NumMentions']
    target = 'AvgTone'
    # one_hot_df = one_hot(df)
    rf_df = random_forests(features, target, df)


if __name__ == '__main__':
    main()
