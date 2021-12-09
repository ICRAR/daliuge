"""
This is an example of splitting a measurementset using the CASA MSTRANSFORM task
We need to develop a template based on it so it can get multiple spws based on
frequency ranges and widths
To run it standalone, change the directories, which are now hardcoded
"""

from sys import argv
import sys, os, datetime, time

if __name__ == "__main__":

    dt = datetime.datetime.now()
    timestr = dt.strftime("%Y-%m-%dT%H-%M-%S")

    ms_dir = "/scratch/partner1024/chiles/final_products/20131122_941_6_FINAL_PRODUCTS/13B-266.sb27261805.eb28549602.56618.334173599535_calibrated_deepfield.ms"
    # ms_dir = '/scratch/jason/13B-266.sb28624226.eb28625769.56669.43262586805_calibrated_deepfield_adios.ms'
    log_dir = "/home/cwu/chiles/processing"
    # log_dir = '/scratch/jason/tmp'
    output_vis = "/scratch/partner1024/chiles/split_vis/{0}".format(timestr)
    # output_vis = '/scratch/jason/tmp/split_vis/{0}'.format(timestr)
    log_file = "{0}/{1}_split_time.log".format(log_dir, timestr)

    os.mkdir(output_vis)
    gap = 4

    st = time.time()
    for i in range(1):
        mstransform(
            vis=ms_dir,
            outputvis="{0}/vis_{1}~{2}".format(
                output_vis, 1020 + i * gap, 1024 + i * gap
            ),
            start="{0}MHz".format(1020 + i * gap),
            width="15.625kHz",
            spw="2~2",
            nchan=256,
            regridms=T,
            restfreq="1420.405752MHz",
            mode="frequency",
            outframe="lsrk",
            interpolation="linear",
            veltype="radio",
            combinespws=True,
            nspw=1,
            createmms=False,
            datacolumn="data",
        )

    tt = time.time() - st
    f = open(log_file, "w")
    f.write("total split time = {0} seconds".format(tt))
    f.close()

    """
    mstransform(vis=/scratch/partner1024/chiles/final_products//20131025_951_4_FINAL_PRODUCTS/13B-266.sb27248272.eb28094627.56590.41308579861_calibrated_deepfield.ms,
    outputvis=/scratch/partner1024/chiles/split_vis/12345/20131025_951_4/vis_1024~1028,
    start=1024MHz,width=15.625,spw=2~2,nchan=256)
    
    mstransform(vis=/scratch/partner1024/chiles/final_products//20131025_951_4_FINAL_PRODUCTS/13B-266.sb27248272.eb28094627.56590.41308579861_calibrated_deepfield.ms,
    outputvis=/scratch/partner1024/chiles/split_vis/12345/20131025_951_4/vis_1028~1032,
    start=1028MHz,width=15.625,spw=2~2,nchan=256)
    
    mstransform(vis=/scratch/partner1024/chiles/final_products//20131025_951_4_FINAL_PRODUCTS/13B-266.sb27248272.eb28094627.56590.41308579861_calibrated_deepfield.ms,
    outputvis=/scratch/partner1024/chiles/split_vis/12345/20131025_951_4/vis_1032~1036,
    start=1032MHz,width=15.625,spw=2~2,nchan=256)
    """
