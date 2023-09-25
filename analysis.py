import os

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import df_op
import csv_file
import metrics

result_dir = 'result_epoch'


def epoch_metrics(ddir, table_name, metrix, efrom, eto):
    dic = {}
    for epoch in range(efrom, eto):
        df = csv_file.load_csv_data(ddir, table_name, epoch)

        dic[epoch] = metrics.select_metrics(metrix, df)
    df = pd.DataFrame(dic.items(), columns=["epoch", metrix])
    csv_file.save_csv_data(result_dir, table_name, df, metrix + '_1')


def epoch_metrics_step(ddir, table_name, metrix, efrom, eto, step):
    dic = {}
    for epoch in range(efrom, eto, step):
        df = csv_file.load_csv_data(ddir, table_name, epoch)
        print("load: " + str(epoch))
        dic[epoch] = metrics.select_metrics(metrix, df)
    df = pd.DataFrame(dic.items(), columns=["epoch", metrix])
    csv_file.save_csv_data(result_dir, table_name, df, metrix + '_' + str(step))


def epoch_step_metrics(ddir, table_name, metrix, efrom, eto, step):
    dic = {}

    epoch = efrom

    while epoch < eto:
        # dfs = csv_file.load_csv_list(table_name, epoch, epoch + step)
        dfs = csv_file.load_csvs(ddir, table_name, [e for e in range(epoch, epoch + step + 1)])

        dfc = df_op.concat_df(dfs, 0, 1)
        index = metrics.select_metrics(metrix, dfc)
        for e in range(epoch, epoch + step):
            dic[e] = index
        epoch += step

    df = pd.DataFrame(dic.items(), columns=["epoch", metrix + "_" + str(step)])
    csv_file.save_csv_data(result_dir, table_name, df, metrix + "_" + str(step))


def epoch_slide_metrics(ddir, table_name, metrix, efrom, eto, step):
    dic = {}

    for epoch in range(efrom, eto-step):
        # df_window = dfs[efrom:efrom + step]
        # dfs = csv_file.load_csv_list(table_name, epoch, epoch + step)
        dfs = csv_file.load_csvs(ddir, table_name, [e for e in range(epoch, epoch + step + 1)])

        dfc = df_op.concat_df(dfs, 0, 1)
        dic[epoch] = metrics.select_metrics(metrix, dfc)

    for epoch in range(eto-step, eto):
        dic[epoch] = dic[epoch-1]

    df = pd.DataFrame(dic.items(), columns=["epoch", metrix + "_" + str(step)])
    csv_file.save_csv_data(result_dir, table_name, df, metrix + "_" + str(step))


def combine_slide_steps(rdir, tname, fname, steps):
    dfs = []
    for step in steps:
        dfs.append(csv_file.load_csv_data(rdir, tname, fname + '_' + str(step)))

    dfc = dfs[0]
    for df in dfs[1:]:
        dfc = dfc.merge(df, on=dfc.columns[0], how='left')
    csv_file.save_csv_data(result_dir, tname, dfc, fname + '_s')


def draw_line_chart(rdir, tname, fname):
    df = csv_file.load_csv_data(rdir, tname, fname)
    # Assuming 'x_column' and 'y_column' are the column names from your DataFrame
    x = df[df.columns[0]]
    y = df[df.columns[1]]
    plt.clf()
    plt.plot(x, y, label=df.columns[1])
    plt.xlabel(df.columns[0])
    plt.ylabel(df.columns[1])
    plt.title('Line Chart: ' + tname + '&' + fname)
    plt.grid(True)

    cname = 'chart_epoch' + os.sep + tname + '&' + fname + '.jpg'
    plt.savefig(cname)
    print('save chart: ' + cname)
    # plt.show()


def draw_time_chart(rdir, tname, fname):
    df = csv_file.load_csv_data(rdir, tname, fname)
    plt.clf()

    x = pd.to_datetime(df[df.columns[0]])
    for i in range(1, len(df.columns)):
        y = df[df.columns[i]]
        plt.plot(x, y, label=df.columns[i])
    plt.xlabel(df.columns[0])
    plt.ylabel(df.columns[1])
    # plt.title('Line Chart: ' + tname + '&' + fname)
    plt.grid(True)
    plt.legend()
    plt.xticks(rotation=30)

    cname = 'chart_time' + os.sep + tname + '&' + fname + '.jpg'
    plt.savefig(cname)
    print('save chart: ' + cname)


def draw_lines_chart(rdir, tname, fname):
    df = csv_file.load_csv_data(rdir, tname, fname)

    plt.clf()
    x = df[df.columns[0]]

    for i in range(1, len(df.columns)):
        y = df[df.columns[i]]
        plt.plot(x, y, label=df.columns[i])

    # y1 = df[df.columns[1]]
    # y5 = df[df.columns[2]]
    # y10 = df[df.columns[3]]
    # plt.plot(x, y1, linestyle='-', label=df.columns[1])
    # plt.plot(x, y5, linestyle='--', color='g', label=df.columns[2])
    # plt.plot(x, y10, linestyle='-.', color='r', label=df.columns[3])

    plt.xlabel(df.columns[0])
    plt.ylabel(df.columns[1])
    plt.title('Line Chart: ' + tname + '&' + fname + '_s')
    plt.grid(True)
    plt.legend()

    cname = 'chart_epoch' + os.sep + tname + '&' + fname + '.jpg'
    plt.savefig(cname)
    print('save chart: ' + cname)


def test_metrix_slide(ddir, rdir, metrix, tname, efrom, eto, steps):
    for step in steps:
        if step == 1:
            epoch_metrics(ddir, tname, metrix, efrom, eto)
            continue
        epoch_slide_metrics(ddir, tname, metrix, efrom, eto, step)
    combine_slide_steps(rdir, tname, metrix, steps)
    draw_lines_chart(rdir, tname, metrix)


def test_multi_metrix_slide(tname, efrom, eto, steps):
    for m in ['hhi', 'entropy', 'naka', 'gini']:
        test_metrix_slide(m, tname, efrom, eto, steps)


def draw_pie_chart(ddir, tname, fname, topn):

    df = metrics.df_proportion_sort(csv_file.load_csv_data(ddir, tname, fname), 1)
    top = df.head(topn)

    # Create labels for top ten organizations
    labels = top[df.columns[0]].tolist()
    labels.append('Others')

    # Calculate market share for top ten and others
    top_prop = top['proportion'].sum()
    other_prop = 1 - top_prop

    # Create data for pie chart
    props = top['proportion'].tolist()
    props.append(other_prop)

    plt.clf()
    harmonic_colors = [
        "#B9DDF1", "#9FCAE6", "#73A4CA", "#497AA7", "#2E5B88",
        "#589F8F", "#86C290", "#A7DAA9", "#C4EAC3", "#E1F7E3",
        "#FFC0CB", "#FF91A4", "#FF6479", "#FF3A4B", "#FF1A23",
        "#FFD700", "#FFA500", "#FF8C00", "#FF6B00", "#FF4500"
    ]
    harmonic_colors[topn] = "LightGray"
    # Plot the pie chart
    plt.pie(props,  autopct='%1.1f%%', startangle=120, colors=harmonic_colors)
    plt.axis('equal')  # Equal aspect ratio ensures that the pie chart is circular.
    plt.subplots_adjust(right=1.1)
    # plt.title('Organization Market Share')
    # plt.show()
    cname = 'chart_pie' + os.sep + tname + '&' + str(fname) + '_' + str(topn) + '.jpg'
    plt.savefig(cname)
    print('save chart: ' + cname)


if __name__ == "__main__":
    # test01("epoch_stake_mp", 300, 410)
    # test02("epoch_stake_mp", "naka", 300, 410, 1)
    # test02("epoch_stake_mp", "naka", 300, 410, 5)
    # test02("epoch_stake_mp", "naka", 300, 410, 10)
    # combine_steps("epoch_stake_mp", "naka")
    # draw_line_chart("epoch_stake_mp", "hhi_mp_1")
    # draw_lines_chart("epoch_stake_mp", "naka_s")
    # test_lines()
    # test_metrix("entropy", "epoch_stake_mp", 300, 410)
    # test_multi_metrix("block_leader_mp", 300, 410)
    # steps = [1, 5, 10]
    # test_multi_metrix_slide("block_leader_mp", 300, 410, steps)
    # draw_time_chart("block_leader_mp", "entropyt")
    print(1)
    # draw_pie_chart('mapped', 'block_leader', 415, 10)
    # draw_pie_chart('mapped', 'block_leader', 400, 10)
    # draw_pie_chart('dump', 'genesis', 0, 5)
    df = csv_file.load_csv_data('dump', 'utxo', 400)
    print(df[df.columns[1]].sum())