import os, csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def plot_lpl_parts(folder, prefix='lpl_parts', suffix='.csv'):
    min_y = 1e6
    max_y = -1
    min_x = 1e6
    max_x = -1
    fig = plt.figure()
    ax = fig.add_subplot(111)
    #ax.set_title('Disk cache hits ratio as a function of disk capacity and replacement policy', fontsize=17)
    ax.set_xlabel('# of partitions', fontsize=16)
    ax.set_ylabel("Execution time (relative)", fontsize=16)
    ax.grid(True)

    for fn in os.listdir(folder):
        if (fn.startswith(prefix) and fn.endswith(suffix)):
            ffn = os.path.join(folder, fn)
            data = pd.read_csv(ffn).values
            y = data[:, 0]
            x = data[:, 1]
            min_y = min(min_y, np.min(y))
            max_y = max(max_y, np.max(y))
            min_x = min(min_x, np.min(x))
            max_x = max(max_x, np.max(x))
            lbl = fn.split(prefix)[1].split(suffix)[0].replace('_', '')
            print(lbl)
            ax.plot(x, y, label=lbl)
    #fig.gca().invert_xaxis()
    legend = ax.legend(loc="upper right", shadow=True, prop={'size':15})
    ax.set_xlim([max_x + 10, min_x - 10])
    ax.set_ylim([min_y - 10, max_y + 10])

    plt.show()


if __name__=="__main__":
    plot_lpl_parts('/Users/Chen/Documents/logical_physical_mapping/Daliuge_paper')
