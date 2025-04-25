# -*- coding: utf-8 -*-

from openpyxl import Workbook
import concurrent.futures
import pandas as pd
import polars as pl
import time


def process_pid(pid):
    data = pl.read_csv(f'{pid}.csv', columns=['time', 'annotation'], low_memory=False)

    data = (
        data
        .with_columns(
            pl.col('annotation').str.extract(r';MET (\d+\.\d+)', 1).cast(pl.Float64).alias('MET')
        )
        .pipe(segment_mean_imputation, 'MET')
    )

    result = (
        data.group_by('MET')
        .agg(pl.col('time').count().alias('data_count'))
        .with_columns(
            (pl.col('data_count') / 360000).round(4).alias('estimated_time_seconds')
        )
    )

    final_result = (
        result.select([
            pl.when(pl.col('MET') >= 6.0).then(pl.col('estimated_time_seconds')).sum().alias(
                '高强度总时长（小时）'
            ),
            pl.when((pl.col('MET') >= 3.0) & (pl.col('MET') < 6.0)).then(
                pl.col('estimated_time_seconds')
            ).sum().alias('中等强度总时长（小时）'),
            pl.when((pl.col('MET') >= 1.6) & (pl.col('MET') < 3.0)).then(
                pl.col('estimated_time_seconds')
            ).sum().alias('低强度总时长（小时）'),
            pl.when((pl.col('MET') >= 1.0) & (pl.col('MET') < 1.6)).then(
                pl.col('estimated_time_seconds')
            ).sum().alias('静态行为总时长（小时）'),
            pl.when(pl.col('MET') < 1).then(pl.col('estimated_time_seconds')).sum().alias(
                '睡眠总时长（小时）'
            )
        ])

        .with_columns(
            pl.Series(
                '总时长（小时）',
                [round(data.shape[0] / 100 / 3600, 4)]  # 使用数据总量估算总时长（小时）
            )
        )
    )

    print("-" * 100)
    print(f"PID: {pid}")
    print(final_result)
    print("-" * 100)
    print()

    return pid, final_result


def segment_mean_imputation(data: pl.DataFrame, column_name: str) -> pl.DataFrame:
    forward_fill = data[column_name].fill_null(strategy='forward')
    backward_fill = data[column_name].fill_null(strategy='backward')

    filled_column = (forward_fill + backward_fill) / 2
    data = data.with_columns(
        pl.when(data[column_name].is_null())
        .then(filled_column)
        .otherwise(data[column_name])
        .alias(column_name)
    )

    if data[column_name].is_null().tail(1).to_list()[0]:
        data = data.with_columns(
            pl.when(data[column_name].is_null())
            .then(forward_fill)
            .otherwise(data[column_name])
            .alias(column_name)
        )

    return data


def save_results(results_list):
    wb = Workbook()
    ws = wb.active

    headers = [
        '志愿者ID', '记录总时长（小时）', '睡眠总时长（小时）', '高等强度运动总时长（小时）',
        '中等强度运动总时长（小时）', '低等强度运动总时长（小时）', '静态活动总时长（小时）'
    ]

    ws.append(headers)

    for pid, result in sorted(results_list, key=lambda x: int(x[0][1:])):
        result_dict = result.to_dicts()[0]

        rounded_data = {
            key: round(value, 4) if isinstance(value, float) else value
            for key, value in result_dict.items()
        }

        result_data = {
            "志愿者ID": pid,
            "记录总时长（小时）": rounded_data['总时长（小时）'],
            "睡眠总时长（分钟）": rounded_data['睡眠总时长（小时）'],
            "高等强度运动总时长（分钟）": rounded_data['高强度总时长（小时）'],
            "中等强度运动总时长（分钟）": rounded_data['中等强度总时长（小时）'],
            "低等强度运动总时长（分钟）": rounded_data['低强度总时长（小时）'],
            "静态活动总时长（分钟）": rounded_data['静态行为总时长（小时）']
        }

        ws.append(list(result_data.values()))

    wb.save(result_path)


def main():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_pid, pid) for pid in pids]
        results_list = []

        for future in concurrent.futures.as_completed(futures):
            pid, result = future.result()
            results_list.append((pid, result))

    save_results(results_list)


if __name__ == '__main__':
    time_start = time.time()

    result_path = 'result_1.xlsx'
    metadata = pd.read_csv('Metadata1.csv')

    pids = metadata['pid'].tolist()
    main()

    print(f'Time cost: {round((time.time() - time_start), 2)} seconds')
