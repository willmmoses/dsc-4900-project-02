import glob
import os
import dask.dataframe as dd
import pandas as pd
import matplotlib.pylab as plt
import numpy as np
import vaex as vx


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
    # max_file = len(csv_files)
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
    # print("Ping")
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
    df = vx.open(folder,
                 dtype={'Actor1Code': str,
                        'Actor1Name': str,
                        'EventCode': str,
                        'ActionGeo_ADM1Code': str})
    print("Read complete")
    print("Starting cleanup")
    df['EventType'] = df.EventCode[:2]
    df['EventDetails'] = df.EventCode[2:]
    df['EventCountry'] = df.ActionGeo_ADM1Code[:2]
    df['EventRegion'] = df.ActionGeo_ADM1Code[2:]
    df = df.dropna()
    print("Cleanup complete")
    return df


def scatter_plotter(df):
    columns = ['NumMentions']
    for y_data in columns:
        print("Current column:" + y_data)
        xycounts = df.count(binby=[df.AvgTone, y_data], limits=[[-20, 20], [0, 50]])
        plt.rcParams["figure.figsize"] = [12, 12]
        plt.rcParams["figure.autolayout"] = True
        plt.imshow(xycounts.T, origin='lower', extent=[-20, 20, 0, 50])
        plt.show()
        plt.savefig(y_data + '.png', dpi=1200)


def main():
    folder = "data"
    path = os.path.join(os.getcwd(), folder, "*")
    # print(csv_files)
    # # cleaner(csv_files)
    # path = os.path.join(os.getcwd(), "cleaned")
    # cleaned_files = glob.glob(os.path.join(path, "*"))
    # # df = reader(cleaned_files)
    # df = dd_reader(path)
    df = vx_reader(path)
    # df = event_splitter(df)
    # print(df.info())
    # print(df.count())
    scatter_plotter(df)
    print("Done!")
    # print(df.shape[0].compute())
    # df.to_dask_array(lengths=True)
    # pd_df = df.compute()
    # plt.scatter(pd_df['AvgTone'], pd_df['SQLDATE'])
    # plt.show()
    # df.head(5)


if __name__ == '__main__':
    main()
