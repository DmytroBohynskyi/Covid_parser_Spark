import os

import matplotlib.pyplot as plt
import numpy as np

DIR = 'out_data/test/'
COVID_WORDS = ('pages', 'wirus', 'covid19', 'covid', 'pandemi')
DICT = {}


def auto_label(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


def plot(data_2019, data_2020, month):
    x = np.arange(len(COVID_WORDS) - 1)  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, data_2019[1:], width, label=f'{month} 2020: {data_2019[0]} websites')
    rects2 = ax.bar(x + width / 2, data_2020[1:], width, label=f'{month} 2019: {data_2020[0]} websites')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xticks(x)
    ax.set_xticklabels(COVID_WORDS[1:])
    ax.legend()

    auto_label(ax, rects1)
    auto_label(ax, rects2)

    fig.tight_layout()

    plt.show()


def change(line: str):
    start_data = line.find(" (")
    end_data = line.find("))")
    count_data = line[start_data + 2:end_data].split(",")
    data = [int(data) for data in count_data]

    return data


def open_data(filenames):
    with open(f"{DIR}{filenames}") as file:
        line = file.readline()
        while line:
            yield change(line)
            line = file.readline()


def get_count(index=0):
    filenames = os.listdir(path=DIR)
    filenames_2019 = filenames[index]
    text = open_data(filenames_2019)

    count = list(map(sum, zip(*text)))
    return count


if __name__ == '__main__':
    months = ['April 2019', 'March 2019', 'March 2020', 'April 2019']
    data = [get_count(index=n) for n in range(4)]
    pass
    plot(data[1], data[2], month='March')
    plot(data[0], data[3], month='April')
