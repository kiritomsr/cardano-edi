import time

import analysis
import dump
import mapping


# epoch_stake, block_leader, reward
def task01(tname, efrom, eto, ifmap, metrics, steps, iftime):
    dump.dump_missing(tname, efrom, eto)

    ddir = 'dump'
    if ifmap:
        dump.dump_needed('pool_offline_data', 0)
        ddir = 'mapped'
        for e in range(efrom, eto):
            mapping.dump_mapped_df(tname, e)

    if iftime:
        dump.dump_needed('epoch_time', 0)

    for metrix in metrics:
        for step in steps:
            analysis.epoch_slide_metrics(ddir, tname, metrix, efrom, eto, step)
        analysis.combine_slide_steps('result_epoch', tname, metrix, steps)
        analysis.draw_lines_chart('result_epoch', tname, metrix + '_s')
        if iftime:
            mapping.mapping_epoch_time(tname, metrix + '_s')
            analysis.draw_time_chart('result_time', tname, metrix + '_s')


# epoch_stake, block_leader, reward
def task02(tname, efrom, eto, ifmap, metrics, steps, iftime):

    ddir = 'dump'
    start_time = time.time()
    if ifmap:
        ddir = 'mapped'
        for e in range(efrom, eto):
            mapping.dump_mapped_df(tname, e)
    print('time cost: ' + str(time.time() - start_time))
    for metrix in metrics:
        start_time = time.time()
        for step in steps:
            analysis.epoch_slide_metrics(ddir, tname, metrix, efrom, eto, step)
        print('time cost: ' + str(time.time() - start_time))
        start_time = time.time()
        analysis.combine_slide_steps('result_epoch', tname, metrix, steps)
        analysis.draw_lines_chart('result_epoch', tname, metrix + '_s')
        # print('time cost: ' + str(time.time() - start_time))
        # start_time = time.time()
        if iftime:
            mapping.mapping_epoch_time(tname, metrix + '_s')
            analysis.draw_time_chart('result_time', tname, metrix + '_s')
        print('time cost: ' + str(time.time() - start_time))


# epoch_stake, block_leader, reward
def task011(tname, efrom, eto, ifmap, metrics, iftime):
    dump.dump_missing(tname, efrom, eto)

    ddir = 'dump'
    if ifmap:
        dump.dump_needed('pool_offline_data', 0)
        ddir = 'mapped'
        for e in range(efrom, eto):
            mapping.dump_mapped_df(tname, e)

    if iftime:
        dump.dump_needed('epoch_time', 0)

    for metrix in metrics:

        analysis.epoch_metrics(ddir, tname, metrix, efrom, eto)
        analysis.draw_line_chart('result_epoch', tname, metrix + '_1')
        if iftime:
            mapping.mapping_epoch_time(tname, metrix + '_1')
            analysis.draw_time_chart('result_time', tname, metrix + '_1')


# utxo
def task012(tname, efrom, eto, step, metrics, iftime):
    # dump.dump_missing(tname, efrom, eto)

    ddir = 'dump'

    for metrix in metrics:

        analysis.epoch_metrics_step(ddir, tname, metrix, efrom, eto, step)
        analysis.draw_line_chart('result_epoch', tname, metrix + '_' + str(step))
        if iftime:
            mapping.mapping_epoch_time(tname, metrix + '_' + str(step))
            analysis.draw_time_chart('result_time', tname, metrix + '_' + str(step))


if __name__ == "__main__":
    # start_time = time.time()
    task02('epoch_stake', 300, 420, True, ['hhi'], [1, 3, 10], True)
    # end_time = time.time()
    # print('time cost: ' + str(end_time - start_time))
    # task011('epoch_stake', 300, 420, True, ['hhi'], True)

    # task01('block_leader', 300, 420, True, ['naka'], [1, 3, 10], True)
    # task011('block_leader', 300, 420, True, ['naka'], True)
    # task01('reward', 300, 420, False, ['entropy'], [1, 3, 10], True)
    # task02('rewards', 300, 420, False, ['entropy'], [1], True)


    # task012('utxo', 300, 420, 10, ['entropy'], True)


