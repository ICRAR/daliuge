import os
import sys


def do_clean(infiles, outfile):

    if not infiles:
        raise Exception('infiles is empty')

    if not outfile:
        raise Exception('outfile is empty')

    clean(vis=infiles,
        imagename=outfile,
        field='deepfield',
        spw='',
        mode='frequency',
        restfreq='1420.405752MHz',
        nchan=-1,
        start='',
        width='',
        interpolation='nearest',
        niter=0,
        gain=0.1,
        threshold='0.0mJy',
        imsize=[256],
        cell=['1.0arcsec'],
        phasecenter='10h01m53.9,+02d24m52s',
        weighting='natural',
        usescratch=False)


if __name__ == '__main__':

    try:
        print inputs
        print outcube
        do_clean(inputs, outcube)

    except Exception as ex:
        print >> sys.stderr, 'SEVERE ' + str(ex)

