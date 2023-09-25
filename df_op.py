import numpy as np
import pandas as pd

import csv_file


def concat_df(dfs, group_col, sum_col):
    if len(dfs) == 1:
        return dfs[0]
    df = dfs[0]
    for d in dfs[1:]:
        df = pd.concat([df, d], ignore_index=True)
    # concatenated_df = pd.concat([dfs], ignore_index=True)

    return df.groupby(dfs[0].columns[group_col], as_index=False)[dfs[0].columns[sum_col]].sum()


def load_range_df(directory, table_name, from_no, to_no):
    dfs = []
    for i in range(from_no, to_no):
        dfs.append(csv_file.load_csv_data(directory, table_name, i))

    return concat_df(dfs, 0, 1)


def left_join_df(df1, df2, join_col):
    return df1.merge(df2, on=df1.columns[join_col], how='left')


def merge_df(df, merge_col, sum_col):
    return df.groupby(df.columns[merge_col], as_index=False)[df.columns[sum_col]].sum()


def sort_df(df, sort_col, asc):
    return df.sort_values(df.columns[sort_col], ascending=asc)


def fill_na_df(df, id_col, fill_col):
    ic = df.columns[id_col]
    fc = df.columns[fill_col]
    df[fc] = df.apply(lambda row: ic + '_' + str(row[ic]) if pd.isnull(row[fc]) else row[fc], axis=1)
    return df


def int64_df(df, col):
    c = df.columns[col]
    df[c] = df[c].astype(np.int64)
    return df


