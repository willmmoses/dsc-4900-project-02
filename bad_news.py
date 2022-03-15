import glob
import os

import dask.dataframe as dd
import pandas as pd


def reader(folder):
    path = os.path.join(os.getcwd(), folder)
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    df = pd.DataFrame()
    counter = 0
    max = len(csv_files)
    for file in csv_files:
        print("%d of %d,\t %f % complete,\t File name = ", counter, max, counter / max, file)
        temp_df = pd.read_csv(file, low_memory=False)
        df = pd.concat([df, temp_df])
    print(df.info())
    # print("Ping")
    print(df.describe())
    return df


def dd_reader():
    df = dd.read_csv("events/*.csv", dtype={'Actor1Code': str, 'Actor1Name': str, 'EventCode': str})
    # df['EventType'] = df.EventCode.str[:2].astype(int)
    # df['EventDetails'] = df.EventCode.str[2:].astype(int)
    # df = df.set_index('SQLDATE')
    df = df.sort_values(by='SQLDATE')
    return df


def main():
    folder = "events"
    df = dd_reader()
    # df = event_splitter(df)
    print(df.info())
    # print(df.head())


if __name__ == '__main__':
    main()
