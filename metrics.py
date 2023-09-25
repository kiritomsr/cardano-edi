import os

import numpy as np
from matplotlib import pyplot as plt

import csv_file
import df_op


def select_metrics(metrix, df):
    if metrix == 'hhi':
        return calculate_hhi(df)
    elif metrix == 'entropy':
        return calculate_entropy(df)
    elif metrix == 'naka':
        return calculate_naka(df)
    elif metrix == 'gini':
        return calculate_gini(df)


def df_proportion_sort(df, resource_col):
    resource = df.columns[resource_col]
    df_sorted = df.sort_values(by=resource, ascending=False)
    total_amount = df_sorted[resource].sum()
    df_sorted['proportion'] = df_sorted[resource] / total_amount
    return df_sorted


def calculate_hhi(df):
    # resource = df.columns[resource_col]
    df = df_proportion_sort(df, 1)
    df['market_share'] = (df['proportion'] * 100).round().astype(int)

    # Square the market shares
    df['squared_market_share'] = df['market_share'] ** 2

    # Calculate HHI
    hhi = df['squared_market_share'].sum()
    # print(df[['oms', 'market_share', 'squared_market_share']])
    return hhi


def calculate_entropy(df):
    df = df_proportion_sort(df, 1)
    df = df.loc[df['proportion'] > 0, :].copy()
    # Calculate total sum of amounts
    # total_amount = df[resource].sum()

    # Calculate probability distribution
    # df['probability'] = df[resource] / total_amount
    # print(df)
    # Calculate Shannon Entropy
    # shannon_entropy = -np.sum(df['probability'] * np.log2(df['probability']))
    shannon_entropy = -np.sum(df['proportion'] * np.log2(df['proportion']))

    return shannon_entropy


def calculate_naka(df):
    # Calculate cumulative hash power
    df = df_proportion_sort(df, 1)
    cumulative_hash_power = 0
    cumulative_population = 0

    for index, row in df.iterrows():
        cumulative_hash_power += row['proportion']
        cumulative_population += 1
        if cumulative_hash_power >= 0.5:
            break
    return cumulative_population


def calculate_gini(df):
    # entity = df0.columns[0]
    df = df_proportion_sort(df, 1)
    df_sorted = df.sort_values(by='proportion', ascending=True)
    df = df_sorted.loc[df_sorted['proportion'] > 0, :].copy()
    # print(df)

    # Calculate cumulative share of wealth and cumulative share of population
    cumulative_share_wealth = np.cumsum(df['proportion']) / df['proportion'].sum()
    cumulative_share_population = np.arange(1, len(df) + 1) / len(df)
    # print(type(cumulative_share_wealth))
    # print(type(cumulative_share_population))
    # # Calculate Gini index
    # gini_index = np.sum(cumulative_share_wealth * cumulative_share_population) + \
    #              2 * np.sum(cumulative_share_wealth * (1 - cumulative_share_population)) - 1

    # Calculate Gini index using A / (A + B) formula
    A = np.trapz(cumulative_share_wealth, dx=1 / len(df))
    B = np.trapz(cumulative_share_population, dx=1 / len(df))
    gini_coefficient = 1 - A / (A + B)

    # # Plot Lorenz curve and annotate points
    # plt.plot(cumulative_share_population, cumulative_share_wealth, label='Lorenz curve')
    # # plt.scatter(cumulative_share_population, cumulative_share_wealth, c='red')
    # plt.plot([0, 1], [0, 1], 'k--', label='Perfect equality')
    #
    # plt.xlabel('Cumulative Share of Population')
    # plt.ylabel('Cumulative Share of Wealth')
    # plt.title('Lorenz Curve')
    # plt.legend()
    # plt.show()
    #
    # cname = 'chart_lorenz' + os.sep + df.columns[1] + '.jpg'
    # plt.savefig(cname)
    # print('save chart: ' + cname)

    return gini_coefficient


def draw_lorenz_curve(df):

    df = df_proportion_sort(df, 1)
    df_sorted = df.sort_values(by='proportion', ascending=True)
    df = df_sorted.loc[df_sorted['proportion'] > 0, :].copy()

    # Calculate cumulative share of wealth and cumulative share of population
    cumulative_share_wealth = np.cumsum(df['proportion']) / df['proportion'].sum()
    cumulative_share_population = np.arange(1, len(df) + 1) / len(df)
    plt.clf()
    # Plot Lorenz curve and annotate points
    plt.plot(cumulative_share_population, cumulative_share_wealth, label='Lorenz curve')
    # plt.scatter(cumulative_share_population, cumulative_share_wealth, c='red')
    plt.plot([0, 1], [0, 1], 'k--', label='Perfect equality')

    plt.xlabel('Cumulative Share of Population')
    plt.ylabel('Cumulative Share of Wealth')
    plt.title('Lorenz Curve')
    plt.legend()
    # plt.show()

    cname = 'chart_lorenz' + os.sep + df.columns[1] + '.jpg'
    plt.savefig(cname)
    print('save chart: ' + cname)




def generate_metrics(df):
    # dfs = df_proportion_sort(df.sort_values(df.columns[1], ascending=False), 1)
    print(df)
    print('hhi: ', calculate_hhi(df))
    print('entropy: ', calculate_entropy(df))
    print('naka: ', calculate_naka(df))
    print('gini: ', calculate_gini(df))


if __name__ == "__main__":
    print(1)
    # df = dump.load_csv("pool_mapping", 3)
    # df = dump.load_csv("epoch_stake", 400)
    # df = dump.load_csv("block", 400)
    # dfs = df_proportion_sort(df, 1)
    # # print(dfs)
    # print(calculate_hhi(dfs))
    # print(calculate_entropy(dfs))
    # print(calculate_naka(dfs))
    # print(calculate_gini(dfs))

    df = df_op.sort_df(csv_file.load_csv_data('dump', 'utxo', 420), 1, False).head(1000)
    # print(calculate_gini(df))
    # print(calculate_entropy(df))
    draw_lorenz_curve(df)
    # print(calculate_entropy(df))
    # generate_metrics(csv_file.load_csv("utxo", 400))
