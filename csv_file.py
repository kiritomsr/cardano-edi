import pandas as pd
import os

import dump


def save_csv_data(directory, table_name, df, fname):
    folder = os.getcwd() + os.sep + directory + os.sep + table_name
    if not os.path.exists(folder):
        os.makedirs(folder)
    if not isinstance(fname,str):
        fname = str(fname)
    path = folder + os.sep + fname + '.csv'
    df.to_csv(path, index=False)
    print("dump csv: " + path)


def load_csv_data(directory, table_name, fname):
    if not isinstance(fname, str):
        fname = str(fname)
    path = os.getcwd() + os.sep + directory + os.sep + table_name + os.sep + fname + '.csv'

    if not os.path.isfile(path) and directory == 'dump':
        dump.dump_needed(table_name, fname)

    df = pd.read_csv(path)
    return df


def load_csvs(directory, table_name, fns):
    dfs = []
    for fn in fns:
        dfs.append(load_csv_data(directory, table_name, fn))
    return dfs


def save_csv(table_name, df, epoch_no):
    folder = os.getcwd() + os.sep + 'dump' + os.sep + table_name
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = folder + os.sep + str(epoch_no) + '.csv'

    df.to_csv(path, index=False)


def save_csv_result(table_name, df, filename):
    folder = os.getcwd() + os.sep + 'result' + os.sep + table_name
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = folder + os.sep + filename + '.csv'

    df.to_csv(path, index=False)


def load_csv(table_name, epoch_no):
    path = os.getcwd() + os.sep + 'dump' + os.sep + table_name + os.sep + str(epoch_no) + '.csv'
    df = pd.read_csv(path)
    return df


def load_csv_result(table_name, file_name):
    path = os.getcwd() + os.sep + 'result' + os.sep + table_name + os.sep + file_name + '.csv'
    df = pd.read_csv(path)
    return df


def load_csv_list(table_name, efrom, eto):
    dfs = []
    for epoch_no in range(efrom, eto):
        path = os.getcwd() + os.sep + 'dump' + os.sep + table_name + os.sep + str(epoch_no) + '.csv'
        dfs.append(pd.read_csv(path))

    return dfs
