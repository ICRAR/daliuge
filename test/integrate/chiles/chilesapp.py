import threading
import Queue
import os
import sys
import drivecasa

CASAPY = '/mnt/data/chiles/casa4.4/'
SPLIT = '/mnt/data/chiles/split.py'
CLEAN = '/mnt/data/chiles/clean.py'
VIS_ROOT = '/mnt/data/chiles/DataFiles/'
VIS_OUT = '/mnt/data/chiles-output/vis/'
CUBE_OUT = '/mnt/data/chiles-output/cube/'
CUBE_NAME = 'cube1408~1412'

VIS = [
        (VIS_ROOT + '20131025_951_4_FINAL_PRODUCTS/20131025_951_4_FINAL_PRODUCTS.14__calibrated_deepfield.ms', VIS_OUT + '20131025_951_4/'), 
        (VIS_ROOT + '20131031_951_4_FINAL_PRODUCTS/20131031_951_4_FINAL_PRODUCTS.14__calibrated_deepfield.ms', VIS_OUT + '20131031_951_4/'),
        (VIS_ROOT + '20131121_946_6_FINAL_PRODUCTS/20131121_946_6_FINAL_PRODUCTS.14__calibrated_deepfield.ms', VIS_OUT + '20131121_946_6/'),
        (VIS_ROOT + '20140105_946_6_FINAL_PRODUCTS/20140105_946_6_FINAL_PRODUCTS.14__calibrated_deepfield.ms', VIS_OUT + '20140105_946_6/')
        ]


def invoke_split(q,
                infile, 
                outdir, 
                min_freq = 1408, 
                max_freq = 1412, 
                step_freq = 4, 
                width_freq = 15.625, 
                spec_window = '*'):

    try:
        inputs = ['input_vis="'"%s"'"' % infile, 
                'output_dir="'"%s"'"' % outdir, 
                'min_freq=%s' % min_freq, 
                'max_freq=%s' % max_freq, 
                'step_freq=%s' % step_freq, 
                'width_freq=%s' % width_freq, 
                'spec_window="'"%s"'"' % spec_window, 
                'sel_freq=%s' % str(1)]
        
        casa = drivecasa.Casapy(casa_dir = CASAPY, timeout = 1800)
        casaout, errors = casa.run_script(inputs)
        casaout, errors = casa.run_script_from_file(SPLIT)
        q.put(0)

    except Exception as e:
        print str(e)
        q.put(-1)


def invoke_clean(q, indir, outcube):

    try:
        inputs = ['indir="'"%s"'"' % indir, 
                'outcube="'"%s"'"' % outcube]
        
        casa = drivecasa.Casapy(casa_dir = CASAPY, timeout = 1800)
        casaout, errors = casa.run_script(inputs)
        casaout, errors = casa.run_script_from_file(CLEAN)
        q.put(0)

    except Exception as e:
        print str(e)
        q.put(-1)
      

def do_split():

    q = Queue.Queue()
    workers = []
    for v in VIS:
        t = threading.Thread(target = invoke_split, args = (q, v[0], v[1]))
        workers.append(t)
        t.start()

    for w in workers:
        w.join()

    for w in workers:
        result = q.get()
        if result != 0:
            raise Exception('error splitting')


def do_clean():
    q = Queue.Queue()    
    t = threading.Thread(target = invoke_clean, args = (q, VIS_OUT, CUBE_OUT + CUBE_NAME))
    t.start()
    t.join()

    result = q.get()
    if result != 0:
        raise Exception('error cleaning')


def do_source_flux(imagecube):

    casa = drivecasa.Casapy(casa_dir = CASAPY, timeout = 30)
    casa.run_script(['ia.open("'"%s"'")' % imagecube])
    casa.run_script(['flux = ia.pixelvalue([128,128,0,179])["'"value"'"]["'"value"'"]'])
    casaout, _ = casa.run_script(['print flux'])
    flux = float(casaout[0])
    if flux > 9E-4:
        print 'Valid flux: %s' % flux
    else:
        raise Exception('invalid source flux: %s' % flux)


if __name__ == '__main__':
    try:
        os.system('rm -rf %s' % VIS_OUT)
        os.system('rm -rf %s' % CUBE_OUT)
        os.system('mkdir -p %s' % VIS_OUT)
        os.system('mkdir -p %s' % CUBE_OUT)

        print 'Splitting...'
        do_split()
        print 'Splitting Complete!'

        print 'Cleaning...'
        do_clean()
        print 'Cleaning Complete!'
        
        print 'Extracting flux...'
        do_source_flux(CUBE_OUT + CUBE_NAME + '.image')
        print 'Extracting flux Complete!'

    except Exception as e:
        print str(e)
        sys.exit(-1)

    sys.exit(0)

