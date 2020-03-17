import os, csv, itertools, sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def plot_lpl_parts(folder, prefix='lpl_parts', suffix='.csv'):
    min_y = 1e6
    max_y = -1
    min_x = 1e6
    max_x = -1
    min_z = 1e6
    max_z = -1
    fig = plt.figure()
    ax = fig.add_subplot(111)
    #ax.set_title('Disk cache hits ratio as a function of disk capacity and replacement policy', fontsize=17)
    ax.set_xlabel('# of partitions', fontsize=16)
    ax.set_ylabel("PGT completion time (relative)", fontsize=16)
    ax.grid(True)
    marker = itertools.cycle(('o', 'x', '+', '*', '.'))
    linestyle = itertools.cycle(('-', '--', '-.', ':'))
    ax2 = ax.twinx()
    ax2.set_ylabel("Median Degree of Parallelism per Partition", fontsize=16)

    for fn in os.listdir(folder):
        if (fn.startswith(prefix) and fn.endswith(suffix)):
            ffn = os.path.join(folder, fn)
            data = pd.read_csv(ffn).values
            y = data[:, 0]
            x = data[:, 1]
            z = data[:, 2]
            min_y = min(min_y, np.min(y))
            max_y = max(max_y, np.max(y))
            min_x = min(min_x, np.min(x))
            max_x = max(max_x, np.max(x))
            min_z = min(min_z, np.min(z))
            max_z = max(max_z, np.max(z))
            lbl = fn.split(prefix)[1].split(suffix)[0].replace('_', '')\
            .replace('img', '_img')
            print(lbl)
            lsn = linestyle.next()
            lineobj = ax.plot(x, y, label=lbl, linestyle=lsn, linewidth=3)#, marker=marker.next(), markerfacecolor='white')
            ax2.plot(x, z, label='%s Median DoP' % (lbl), linestyle='', linewidth=3,
            marker=marker.next(), markerfacecolor='None', markersize=7,
            markeredgecolor=lineobj[0].get_color())
    #fig.gca().invert_xaxis()
    legend = ax.legend(loc="upper left", shadow=False, prop={'size':15})
    legend = ax2.legend(loc="center left", shadow=False, prop={'size':15})
    ax.set_xlim([max_x + 10, min_x - 10])
    ax.set_ylim([min_y - 10, max_y + 10])
    ax2.set_ylim([min_z - 1, max_z + 1])
    ax.xaxis.set_ticks(list(reversed(np.arange(max(min_x - 10, 0), max_x + 10, 50))))
    ax.yaxis.set_ticks(np.arange(min(min_y - 10, 0), max_y + 10, 100))
    ax2.yaxis.set_ticks(np.arange(min(min_z - 1, 0), max_z + 1, 1))
    ax.tick_params(axis='x', labelsize=14)
    ax.tick_params(axis='y', labelsize=14)
    ax2.tick_params(axis='y', labelsize=14)

    plt.show()

if __name__=="__main__":
    if (len(sys.argv) == 2):
        fd = sys.argv[1]
    else:
        fd = '/Users/Chen/Documents/logical_physical_mapping/Daliuge_paper'\
        '/DropMake_Paper'
    print(fd)
    plot_lpl_parts(fd)
