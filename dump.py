import numpy as np
import db_query
import db_query as dq
import pandas as pd

import df_op
import csv_file
import mapping
import metrics
import query
from google.cloud import bigquery
import os

directory = "dump"


def dump_epoch_stake(es):
    conn = dq.get_conn()

    sql = query.prepare_sum_sql("epoch_stake", "pool_id", "amount", "epoch_no")

    for epoch in es:
        data = dq.query_data(conn, sql.format(epoch_no=epoch))
        df = pd.DataFrame(columns=["pool_id", "resource"], data=data)
        csv_file.save_csv_data(directory=directory, table_name="epoch_stake", df=df, fname=epoch)

    conn.close()


def dump_epoch_block(es):
    conn = dq.get_conn()

    sql = query.prepare_cnt_sql(table_name="block", entity="slot_leader_id")

    for epoch in es:
        data = dq.query_data(conn, sql.format(epoch_no=epoch))
        df = pd.DataFrame(columns=["slot_leader_id", "resource"], data=data)

        # dump.save_csv(table_name="block", col=["slot_leader_id", "count"], data=data, epoch_no=epoch)
        csv_file.save_csv_data(directory=directory, table_name="block", df=df, fname=epoch)

    conn.close()


def dump_epoch_blockld(es):
    conn = dq.get_conn()

    sql = query.prepare_pool_block_sql()

    for epoch in es:
        data = dq.query_data(conn, sql.format(epoch_no=epoch))
        df = pd.DataFrame(columns=["pool_id", "resource"], data=data)

        # dump.save_csv(table_name="block", col=["slot_leader_id", "count"], data=data, epoch_no=epoch)
        csv_file.save_csv_data(directory=directory, table_name="block_leader", df=df, fname=epoch)

    conn.close()


def dump_epoch_reward(es):
    conn = dq.get_conn()

    # sql = query.prepare_sum_sql(table_name="reward", entity="pool_id", resource="amount", epoch="earned_epoch")

    for epoch in es:
        data = dq.query_data(conn, query.prepare_epoch_reward_sql(epoch))
        df = pd.DataFrame(columns=["addr_id", "amount"], data=data)
        csv_file.save_csv_data(directory=directory, table_name="reward", df=df, fname=epoch)

    conn.close()


def dump_utxo(epoch):
    conn = dq.get_conn()
    sql = query.prepare_utxo_epoch(epoch)

    data = dq.query_data(conn, sql)
    df = pd.DataFrame(columns=["address", "lovelace"], data=data)
    csv_file.save_csv_data(directory=directory, table_name="utxo", df=df, fname=epoch)

    conn.close()


def dump_genesis():
    conn = dq.get_conn()

    sql = query.prepare_genesis_sql()

    data = dq.query_data(conn, sql)
    df = pd.DataFrame(columns=["address", "amount"], data=data)
    csv_file.save_csv_data(directory=directory, table_name="genesis", df=df, fname=0)

    conn.close()


def dump_pool_offline_data():
    conn = dq.get_conn()

    sql = query.prepare_pool_offline_data()

    data = dq.query_data(conn, sql)
    df = pd.DataFrame(columns=["pool_id", "homepage"], data=data)
    csv_file.save_csv_data(directory=directory, table_name="pool_offline_data", df=df, fname=0)

    conn.close()


def dump_epoch_time():
    conn = dq.get_conn()

    sql = """select no, end_time from epoch;"""

    data = dq.query_data(conn, sql)
    df = pd.DataFrame(columns=["epoch", "time"], data=data)
    csv_file.save_csv_data(directory=directory, table_name="epoch_time", df=df, fname=0)

    conn.close()


def dump_needed(tname, e):
    if not isinstance(e, str):
        e = str(e)
    path = os.getcwd() + os.sep + directory + os.sep + tname + os.sep + e + '.csv'

    e = int(e)

    if not os.path.isfile(path):
        if tname == 'utxo':
            dump_utxo(e)
        elif tname == 'genesis':
            dump_genesis()
        elif tname == 'pool_offline_data':
            dump_pool_offline_data()
        elif tname == 'epoch_time':
            dump_epoch_time()


def dump_missing(tname, efrom, eto):
    es = []
    for e in range(efrom, eto+1):
        path = os.getcwd() + os.sep + directory + os.sep + tname + os.sep + str(e) + '.csv'
        if not os.path.isfile(path):
            es.append(e)

    if tname == 'epoch_stake':
        dump_epoch_stake(es)
    elif tname == 'block_leader':
        dump_epoch_blockld(es)
    elif tname == 'reward':
        dump_epoch_reward(es)

def test_match():
    df_es = csv_file.load_csv_data(directory, 'epoch_stake', 404)
    df_pm = csv_file.load_csv_data(directory, 'pool_mapping', 0)
    count = 0
    for index, row in df_es.iterrows():
        if row['pool_id'] not in df_pm.values:
            print(row['pool_id'])
            count += 1
    return count


# def test_bq():
#     os.environ.setdefault("GCLOUD_PROJECT", "cardano-bigquery")
#     client = bigquery.Client()
#
#     # Perform a query.
#     QUERY = (
#         'select * from `blockchain-analytics-392322.cardano_mainnet.pool_offline_data` order by epoch_no desc limit 1;')
#     query_job = client.query(QUERY)  # API request
#     rows = query_job.result()  # Waits for query to finish
#
#     for row in rows:
#         print(row)
#         print(row.json)


def range_mapping_metrics(k):
    dfs = []
    for i in range(400, 400 + k):
        # d = dump.int64_df(dump.load_csv("epoch_stake", i), 1)
        dfs.append(csv_file.load_csv_data(directory, "epoch_stake", i))

    df = df_op.concat_df(dfs, 0, 1)

    rel = csv_file.load_csv_data(directory, "pool_mapping", 1)
    result = df.merge(rel, on='pool_id', how='left')
    df1 = df_op.fill_na_df(result, 0, 2)
    # print(df1)
    res = df_op.sort_df(df_op.merge_df(df1, 2, 1), 1, False)

    print(metrics.calculate_hhi(res, 1))


def range_mapping_metrics(k):
    dfs = []
    for i in range(400, 400 + k):
        # d = dump.int64_df(dump.load_csv("epoch_stake", i), 1)
        dfs.append(csv_file.load_csv_data(directory, "epoch_stake", i))

    df = df_op.concat_df(dfs, 0, 1)

    rel = csv_file.load_csv_data(directory, "pool_mapping", 1)
    result = df.merge(rel, on='pool_id', how='left')
    df1 = df_op.fill_na_df(result, 0, 2)
    # print(df1)
    res = df_op.sort_df(df_op.merge_df(df1, 2, 1), 1, False)

    print(metrics.calculate_hhi(res, 1))


if __name__ == "__main__":
    # dump_genesis()
    for e in range(300, 420):
        df = csv_file.load_csv_data('dump', 'reward', e)
        csv_file.save_csv_data('dump', 'rewards', df_op.merge_df(df, 0, 1), e)
    # dump_epoch_reward([e for e in range(300, 420)])
    # dump_epoch_stake(429, 430)
    # dump_epoch_time()
    # epoch = [360, 370, 380, 390]
    # for e in epoch:
    #     print(e)
    #     dump_utxo(e)
    # dump_pool_hash()
    # dump_pool_offline_data()
    # dump.load_csv("block", 0)
    # dump_epoch_block(411, 412)
    # dump_epoch_blockld(411, 412)
    # sql = query.prepare_epoch_stake_sql("epoch_stake", "pool_hash", "amount", "epoch_no").format(epoch_no=399)
    # print(sql)
    # dump_epoch_stake()

    # df = dump.load_csv("epoch_stake", 399)
    # print(df.dtypes)
    # client = db_query.get_bq_client()
    # sql = """
    #     select * from `blockchain-analytics-392322.cardano_mainnet.block`
    #     order by block_time desc limit 1;
    # """
    #
    # df = client.query(sql).to_dataframe();
    # print(df)

    # for i in range(2, 6):
    #     range_mapping_metrics(i)
    # dfs = []
    # for i in range(400, 405):
    #     # d = dump.int64_df(dump.load_csv("epoch_stake", i), 1)
    #     dfs.append(dump.load_csv("epoch_stake", i))
    #
    # df = dump.concat_df(dfs, 0, 1)
    # print(df.dtypes)
    # df = dump.load_csv("epoch_stake", 400)
    # print(df.dtypes)
    # df = dump.int64_df(df, 1)
    # print(df.dtypes)

    # print(metrics.calculate_hhi(df, 1))

    # clean_mapping()
    # print('missing: ', test_match())
    # mapping.mapping_pool_entity()
    # print(mapping.cluster_relation())
    # print(dump_genesis())
    # print(dump.load_csv("epoch_stake", 400))
