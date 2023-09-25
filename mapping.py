import pandas as pd

import df_op
import csv_file


def cluster_relation():
    df = csv_file.load_csv_data('dump', "pool_offline_data", 0)
    dict = {}
    unknown = 0

    for index, row in df.iterrows():

        homepage = str(row['homepage'])
        if homepage is None or '.' not in homepage:
            homepage = 'unknown' + str(unknown)
            unknown += 1

        elif 'http://' in homepage:
            homepage = homepage.replace('http://', '')
        elif 'https://' in homepage:
            homepage = homepage.replace('https://', '')

        if 'www.' in homepage:
            homepage = homepage.replace('www.', '')

        if homepage.endswith('/'):
            homepage = homepage[:-1]

        dict.setdefault(homepage, []).append(df.loc[index]['pool_id'])

    return dict


def mapping_pool_entity():
    df = csv_file.load_csv_data('dump', "pool_offline_data", 0)
    df_rel = pd.DataFrame(columns=['pool_id', 'homepage'])
    unknown = 0

    for index, row in df.iterrows():

        homepage = str(row['homepage']).strip()
        if homepage is None or '.' not in homepage:
            continue
            # homepage = 'unknown' + str(unknown)
            # unknown += 1

        elif 'http://' in homepage:
            homepage = homepage.replace('http://', '')
        elif 'https://' in homepage:
            homepage = homepage.replace('https://', '')

        if 'www.' in homepage:
            homepage = homepage.replace('www.', '')

        if homepage.endswith('/'):
            homepage = homepage[:-1]

        df_rel.loc[len(df_rel)] = {'pool_id': df.loc[index]['pool_id'], 'homepage': homepage}
    csv_file.save_csv_data('dump', 'pool_mapping', df_rel, 1)


def cluster_mapping():
    df = csv_file.load_csv_data('dump', "epoch_stake", 400)
    rel = csv_file.load_csv_data('dump', "pool_mapping", 1)
    result = df.merge(rel, on='pool_id', how='left')
    df1 = df_op.fill_na_df(result, 0, 2)
    # print(df1)
    res = df_op.sort_df(df_op.merge_df(df1, 2, 1), 1, False)

    # selected_columns = df[['Name', 'Country']]
    # df.rename(columns={'OldName': 'NewName'}, inplace=True)

    csv_file.save_csv_data('dump', "pool_mapping", res, 3)


def clustering_pool(df):
    rel = csv_file.load_csv_data('dump', "pool_mapping", 0)
    merged = df.merge(rel, on='pool_id', how='left')
    filled = df_op.fill_na_df(merged, 0, 2)
    projected = filled.loc[:, [filled.columns[2], filled.columns[1]]]
    return projected.groupby(projected.columns[0], as_index=False)[projected.columns[1]].sum()


def dump_mapped_df(tabel_name, epoch):
    df = clustering_pool(csv_file.load_csv_data('dump', tabel_name, epoch))
    csv_file.save_csv_data('mapped', tabel_name, df, epoch)


def mapping_epoch_time(tname, fname):
    df = csv_file.load_csv_data('result_epoch', tname, fname)
    dfet = csv_file.load_csv_data('dump', "epoch_time", 0)
    dfm = df.merge(dfet, on='epoch', how='left')
    cols = ['time']
    for col in df.columns[1:]:
        cols.append(col)
    dfmr = dfm.loc[:, cols]
    csv_file.save_csv_data('result_time', tname, dfmr, fname)


if __name__ == "__main__":
    # df = clustering_pool(df_op.load_range_df("epoch_stake", 400, 403))
    # metrics.generate_metrics(df)
    # for e in range(300, 411):
    #     print(e)
    #     dump_mapped_df("epoch_stake", epoch=e)
    #     dump_mapped_df("block_leader", epoch=e)

    # res = csv_file.load_csv_result("block_leader_mp", "entropy")
    mapping_epoch_time("block_leader_mp", "entropy")
