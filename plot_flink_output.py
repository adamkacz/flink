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

    df = pd.DataFrame(final_lines, columns=['category', 'mean', 'time_stamp', 'idx', 'moving_average'])
    type_list = ['string', 'float', 'string', 'int', 'float']
    for name, col_type in zip(df.columns, type_list):
        df.loc[:, name] = df.loc[:, name].astype(col_type)

    df.loc[:, 'moving_average'] = df.loc[:, 'moving_average'].replace(-1, np.nan)

    df.loc[:, 'time_stamp'] = pd.to_datetime(df.loc[:, 'time_stamp'])

    return df


def plot(path, task_name):
    log_df = read_sink_file(path)
    print(log_df)
    categories = log_df['category'].unique()
    for category in categories:
        path = f"plots/{category}"
        if os.path.exists(path):
            shutil.rmtree(path)

        os.mkdir(path)

        category_df = log_df.loc[log_df['category'] == category]
        category_df.plot(x='time_stamp', y=['mean', 'moving_average'],
                         title=f'Mean/moving_mean of rate: {category} - {task_name}')
        plt.savefig(f'{path}/final_plot.jpg')


def main():
    base_path = 'results'
    for directory in os.listdir(base_path):
        sub_path = f'{base_path}/{directory}'
        for path in os.listdir(sub_path):
            path_split = path.split('-')
            task_name = path_split[1]
            plot(f'{sub_path}/{path}', task_name)


if __name__ == '__main__':
    main()



