import matplotlib.pyplot as plt
import pandas as pd
import os
import shutil
import numpy as np


def read_sink_file(path):
    final_lines = []
    with open(path) as file:
        for line in file:
            line = line[:-1].strip('(').strip(')')
            open_ = line.find('(') - 1
            close_ = line.find(')') + 1
            line = line[:open_] + line[close_:]
            final_lines += [line.split(",")]

    df = pd.DataFrame(final_lines, columns=['category', 'mean', 'time_stamp', 'idx', 'moving_average', 'milliseconds'])
    type_list = ['string', 'float', 'string', 'int', 'float', 'float']
    for name, col_type in zip(df.columns, type_list):
        df.loc[:, name] = df.loc[:, name].astype(col_type)

    df.loc[:, 'moving_average'] = df.loc[:, 'moving_average'].replace(-1, np.nan)

    df.loc[:, 'time_stamp'] = pd.to_datetime(df.loc[:, 'time_stamp'])

    return df


def plot(log_df, task_name):
    categories = log_df['category'].unique()
    time_diff = log_df.max()['milliseconds'] - log_df.min()['milliseconds']
    print(task_name, time_diff, time_diff / 741023 * 1000)
    for category in categories:
        path = f"plots/{category}/{task_name}"
        if os.path.exists(path):
            shutil.rmtree(path)

        os.mkdir(path)

        category_df = log_df.loc[log_df['category'] == category]
        category_df.plot(x='time_stamp', y=['mean', 'moving_average'],
                         title=f'Mean/moving_mean of rate: {category} - {task_name}')
        plt.savefig(f'{path}/final_plot.jpg')


def main(sub_directory):
    base_path = f'results/{sub_directory}'
    data_frames = []
    for directory in os.listdir(base_path):
        sub_path = f'{base_path}/{directory}'
        for path in os.listdir(sub_path):
            data_frames += [read_sink_file(f'{sub_path}/{path}')]

    main_data_frame = pd.concat(data_frames)
    plot(main_data_frame, sub_directory)


if __name__ == '__main__':
    for sub_dir in ['file', 'kafka']:
        main(sub_dir)



