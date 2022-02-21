"""
Split the measurementset using MSTransform along the frequency axis
Used as a template to be wrapped by the DROP framework

chen.wu@icrar.org
"""

import sys, os, datetime, subprocess, re, logging
from string import Template
from optparse import OptionParser


logger = logging.getLogger(__name__)

ms_transform_tpl = """
mstransform(vis='${infile}',
            outputvis='${outfile}',
            regridms=T,
            restfreq='1420.405752MHz',
            mode='frequency',
            nchan=${no_chan},
            outframe='lsrk',
            interpolation='linear',
            veltype='radio',
            start='${freq1}MHz',
            width='${width_freq}kHz',
            spw='${spw_range}',
            combinespws=True,
            nspw=1,
            createmms=False,
            datacolumn="data")
"""


def freq_map(low_req, hi_req, *args):
    """
    Ported from Chiles AWS code by R.Dodson (richard.dodson@icrar.org)

    Return the Spectral Window Required given the lower and upper bounds requested.
    The requests are compared to the 3 possible freq ranges (starting at 941, 946 or 951 MHz).
    2 MHz buffer is added to these values.
    2MHz is equiv to 40km/s, which covers the range of calculated observatory velocities of ~+-30km/s

    Future: We could use the _actual_ spectral windows for each day to return a smaller range.
    Additional parameters would need to be passed (just the data file name?)

    Call doctest with # python -m doctest -v freq_map.py

    >>> freq_map(951,956)
    '0~0'

    >>> freq_map(951,983)
    '0~1'

    >>> freq_map(973,978,951)
    '0~0'

    >>> freq_map(973,983,951)
    '0~1'

    >>> freq_map(1005,1007,951)
    '1~1'

    >>> freq_map(1003,1007,941)
    '1~2'

    >>> freq_map(1003,1007,946)
    '1~1'

    >>> freq_map(1003,1007,951)
    '1~1'

    >>> freq_map(1007,1011,941)
    '2~2'

    >>> freq_map(1009,1013,946)
    '1~2'

    >>> freq_map(1009,1013,951)
    '1~2'

    >>> freq_map(1200,1210)
    '8~8'

    >>> freq_map(1360,1400)
    '13~14'

    >>> freq_map(1400,1404)
    '14~14'
    """

    #     SpwID  Name           #Chans   Frame   Ch0(MHz)   Ch0(MHz)   Ch0(MHz)ChanWid(kHz)  TotBW(kHz) BBC Num  Corrs
    #     0      EVLA_L#A0C0#0    2048   TOPO     941.000    946.000    951.000      15.625     32000.0      12  RR  LL
    #     1      EVLA_L#A0C0#1    2048   TOPO     973.000    978.000    983.000      15.625     32000.0      12  RR  LL
    #     2      EVLA_L#A0C0#2    2048   TOPO    1005.000   1010.000   1015.000      15.625     32000.0      12  RR  LL
    #     3      EVLA_L#A0C0#3    2048   TOPO    1037.000   1042.000   1047.000      15.625     32000.0      12  RR  LL
    #     4      EVLA_L#A0C0#4    2048   TOPO    1069.000   1074.000   1079.000      15.625     32000.0      12  RR  LL
    #     5      EVLA_L#A0C0#5    2048   TOPO    1101.000   1106.000   1111.000      15.625     32000.0      12  RR  LL
    #     6      EVLA_L#A0C0#6    2048   TOPO    1133.000   1138.000   1143.000      15.625     32000.0      12  RR  LL
    #     7      EVLA_L#A0C0#7    2048   TOPO    1165.000   1170.000   1175.000      15.625     32000.0      12  RR  LL
    #     8      EVLA_L#A0C0#8    2048   TOPO    1197.000   1202.000   1207.000      15.625     32000.0      12  RR  LL
    #     9      EVLA_L#A0C0#9    2048   TOPO    1229.000   1234.000   1239.000      15.625     32000.0      12  RR  LL
    #     10     EVLA_L#A0C0#10   2048   TOPO    1261.000   1266.000   1271.000      15.625     32000.0      12  RR  LL
    #     11     EVLA_L#A0C0#11   2048   TOPO    1293.000   1298.000   1303.000      15.625     32000.0      12  RR  LL
    #     12     EVLA_L#A0C0#12   2048   TOPO    1325.000   1330.000   1335.000      15.625     32000.0      12  RR  LL
    #     13     EVLA_L#A0C0#13   2048   TOPO    1357.000   1362.000   1367.000      15.625     32000.0      12  RR  LL
    #     14     EVLA_L#A0C0#14   2048   TOPO    1389.000   1394.000   1399.000      15.625     32000.0      12  RR  LL

    f_tab = [
        [941.00, 946.00, 951.00],
        [973.00, 978.00, 983.00],
        [1005.00, 1010.00, 1015.00],
        [1037.00, 1042.00, 1047.00],
        [1069.00, 1074.00, 1079.00],
        [1101.00, 1106.00, 1111.00],
        [1133.00, 1138.00, 1143.00],
        [1165.00, 1170.00, 1175.00],
        [1197.00, 1202.00, 1207.00],
        [1229.00, 1234.00, 1239.00],
        [1261.00, 1266.00, 1271.00],
        [1293.00, 1298.00, 1303.00],
        [1325.00, 1330.00, 1335.00],
        [1357.00, 1362.00, 1367.00],
        [1389.00, 1394.00, 1399.00],
        [1421.00, 1426.00, 1431.00],
    ]

    if_low = 0
    if_hi = 14
    ifn_low = 0
    ifn_hi = 2

    if args:
        if args[0] == 941:
            ifn_low = 0
            ifn_hi = 0
        elif args[0] == 946:
            ifn_low = 1
            ifn_hi = 1
        elif args[0] == 951:
            ifn_low = 2
            ifn_hi = 2

    for nif_low in range(0, 16):
        f = f_tab[nif_low]
        # print nif_low,f
        if (f[ifn_low]) > ((low_req) - 2):
            if_low = nif_low - 1
            # print 'Using '+str(if_low)+' for lower SPW edge'
            nif_low = 14
            break

    for nif_hi in range(0, 16):
        f = f_tab[nif_hi]
        # print nif_hi,f
        if f[ifn_hi] > (hi_req + 2):
            if_hi = nif_hi - 1
            # print 'Using '+str(if_hi)+' for upper SPW edge'
            nif_hi = 14
            break

    if if_low == -1:
        logger.info("Lower bound (%f) out of range", low_req)

    if if_hi == -1:
        logger.info("Upper bound (%f) out of range", hi_req)

    spw = "{0}~{1}".format(if_low, if_hi)
    return spw


def launch_mstransform(
    infile, outfile, no_chan, freq1, width_freq, spw_range, gen_script_dir, casa_bin_dir
):
    """
    launch CASA MSTransform as a separate (CASA) process

    Return: process reference
    """
    # each casapy must launch from a different directory
    mst_dir = "{0}/{1}".format(gen_script_dir, freq1)
    os.system("mkdir -p {0}".format(mst_dir))
    os.chdir(mst_dir)

    mst_dict = {}
    mst_dict["infile"] = infile
    mst_dict["outfile"] = outfile
    mst_dict["no_chan"] = no_chan
    mst_dict["freq1"] = freq1
    mst_dict["width_freq"] = width_freq
    mst_dict["spw_range"] = spw_range

    ts = Template(ms_transform_tpl)
    casa_script = ts.safe_substitute(mst_dict)

    gen_script_fn = "{0}/mstransform.py".format(mst_dir)
    with open(gen_script_fn, "w") as casa_file:
        casa_file.write(casa_script)

    casapy_cmd = "{0}/casapy --nologger -c {1}".format(casa_bin_dir, gen_script_fn)
    casa_process = subprocess.Popen(casapy_cmd.split())
    return casa_process


def do_split(
    infile, outdir, min_freq, max_freq, step_freq, width_freq, work_dir, casa_bin_dir
):
    """
    do actual mstransform, if outdir exists, it will be removed first

    TODO - add timer to measure completion time
    """

    dt = datetime.datetime.now()
    timestr = dt.strftime("%Y-%m-%dT%H-%M-%S")

    outdir += "/{0}".format(timestr)
    if os.path.exists(outdir):
        os.system("rm -rf {0}".format(outdir))
    os.system("mkdir -p {0}".format(outdir))

    steps = (max_freq - min_freq) / step_freq
    rem = (max_freq - min_freq) % step_freq
    if rem:
        steps += 1
    freq1 = min_freq
    freq2 = min_freq + step_freq
    bottom_edge = re.search("_[0-9]{3}_", infile)
    if bottom_edge:
        # e.g. 20131122_941_6_FINAL_PRODUCTS --> 941
        bedge = bottom_edge.group(0)
        bedge = int(bedge[1:4])
    else:
        bedge = None

    casa_proc_list = []
    gen_script_dir = "{0}/{1}".format(work_dir, timestr)

    for i in range(steps):  # potentially parallel
        spw_range = freq_map(freq1, freq2, bedge)
        if rem and (i == steps - 1):
            freq_range = "{0}~{1}".format(min_freq + i * step_freq, max_freq)
        else:
            freq_range = "{0}~{1}".format(freq1, freq2)

        no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!
        outfile = outdir + "vis_" + freq_range
        outfile = "{0}/vis_{1}".format(outdir, freq_range)
        logger.info("working on: %s", outfile)

        casa_proc = launch_mstransform(
            infile,
            outfile,
            no_chan,
            freq1,
            width_freq,
            spw_range,
            gen_script_dir,
            casa_bin_dir,
        )
        casa_proc_list.append(casa_proc)
        freq1 += step_freq
        freq2 += step_freq

    for csp in casa_proc_list:
        csp.wait()  # join all sub-processes before exiting


if __name__ == "__main__":
    """
    Setup all the parameters
    """
    parser = OptionParser()
    # compulsory directory parameters
    parser.add_option(
        "-i",
        "--input_ms",
        action="store",
        type="string",
        dest="infile",
        help="Input measurementset (Required)",
    )
    parser.add_option(
        "-o",
        "--output_dir",
        action="store",
        type="string",
        dest="outdir",
        help="Output directory (Required)",
    )
    parser.add_option(
        "-k",
        "--work_dir",
        action="store",
        type="string",
        dest="work_dir",
        help="Working directory (Required)",
    )
    parser.add_option(
        "-c",
        "--casa_bin_dir",
        action="store",
        type="string",
        dest="casa_bin_dir",
        help="CASA executable directory (Required)",
    )

    # optional mstransform parameters
    parser.add_option(
        "-n",
        "--min_freq",
        action="store",
        type="int",
        dest="min_freq",
        default=1020,
        help="Mininum frquency (MHz)",
    )
    parser.add_option(
        "-x",
        "--max_freq",
        action="store",
        type="int",
        dest="max_freq",
        default=1028,
        help="Maximum frquency (MHz)",
    )
    parser.add_option(
        "-s",
        "--freq_step",
        action="store",
        type="float",
        dest="step_freq",
        default=4,
        help="Subband width(MHz)",
    )
    parser.add_option(
        "-w",
        "--freq_width",
        action="store",
        type="float",
        dest="width_freq",
        default=15.625,
        help="Frquency resolution(KHz)",
    )

    (options, args) = parser.parse_args()
    if (
        None == options.infile
        or None == options.outdir
        or None == options.work_dir
        or None == options.casa_bin_dir
    ):
        parser.print_help()
        sys.exit(1)

    do_split(
        options.infile,
        options.outdir,
        options.min_freq,
        options.max_freq,
        options.step_freq,
        options.width_freq,
        options.work_dir,
        options.casa_bin_dir,
    )
