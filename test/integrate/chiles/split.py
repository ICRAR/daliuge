import re
import os
import sys

def freq_map(low_req,hi_req,*args):
    """

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


    #	  SpwID  Name           #Chans   Frame   Ch0(MHz)   Ch0(MHz)   Ch0(MHz)ChanWid(kHz)  TotBW(kHz) BBC Num  Corrs
    #	  0      EVLA_L#A0C0#0    2048   TOPO     941.000    946.000    951.000      15.625     32000.0      12  RR  LL
    #	  1      EVLA_L#A0C0#1    2048   TOPO     973.000    978.000    983.000      15.625     32000.0      12  RR  LL
    #	  2      EVLA_L#A0C0#2    2048   TOPO    1005.000   1010.000   1015.000      15.625     32000.0      12  RR  LL
    #	  3      EVLA_L#A0C0#3    2048   TOPO    1037.000   1042.000   1047.000      15.625     32000.0      12  RR  LL
    #	  4      EVLA_L#A0C0#4    2048   TOPO    1069.000   1074.000   1079.000      15.625     32000.0      12  RR  LL
    #	  5      EVLA_L#A0C0#5    2048   TOPO    1101.000   1106.000   1111.000      15.625     32000.0      12  RR  LL
    #	  6      EVLA_L#A0C0#6    2048   TOPO    1133.000   1138.000   1143.000      15.625     32000.0      12  RR  LL
    #	  7      EVLA_L#A0C0#7    2048   TOPO    1165.000   1170.000   1175.000      15.625     32000.0      12  RR  LL
    #	  8      EVLA_L#A0C0#8    2048   TOPO    1197.000   1202.000   1207.000      15.625     32000.0      12  RR  LL
    #	  9      EVLA_L#A0C0#9    2048   TOPO    1229.000   1234.000   1239.000      15.625     32000.0      12  RR  LL
    #	  10     EVLA_L#A0C0#10   2048   TOPO    1261.000   1266.000   1271.000      15.625     32000.0      12  RR  LL
    #	  11     EVLA_L#A0C0#11   2048   TOPO    1293.000   1298.000   1303.000      15.625     32000.0      12  RR  LL
    #	  12     EVLA_L#A0C0#12   2048   TOPO    1325.000   1330.000   1335.000      15.625     32000.0      12  RR  LL
    #	  13     EVLA_L#A0C0#13   2048   TOPO    1357.000   1362.000   1367.000      15.625     32000.0      12  RR  LL
    #	  14     EVLA_L#A0C0#14   2048   TOPO    1389.000   1394.000   1399.000      15.625     32000.0      12  RR  LL

    f_tab=[[ 941.00,   946.00,   951.00],[ 973.00,   978.00,   983.00],[1005.00,  1010.00,  1015.00],[1037.00,  1042.00,  1047.00],[1069.00,  1074.00,  1079.00],[1101.00,  1106.00,  1111.00],[1133.00,  1138.00,  1143.00],[1165.00,  1170.00,  1175.00],[1197.00,  1202.00,  1207.00],[1229.00,  1234.00,  1239.00],[1261.00,  1266.00,  1271.00],[1293.00,  1298.00,  1303.00],[1325.00,  1330.00,  1335.00],[1357.00,  1362.00,  1367.00],[1389.00,  1394.00,  1399.00],[1421.00,  1426.00,  1431.00]]

    if_low=0
    if_hi=14
    ifn_low=0
    ifn_hi=2

    if (args):
     if (args[0]==941):
	   ifn_low=0
	   ifn_hi=0
     elif (args[0]==946):
	   ifn_low=1
	   ifn_hi=1
     elif (args[0]==951):
	   ifn_low=2
	   ifn_hi=2

    for nif_low in range(0,16):
        f=f_tab[nif_low]
        #print nif_low,f
        if ((f[ifn_low])>((low_req)-2)):
            if_low=nif_low-1
            #print 'Using '+str(if_low)+' for lower SPW edge'
            nif_low=14
            break

    for nif_hi in range(0,16):
        f=f_tab[nif_hi]
        #print nif_hi,f
        if (f[ifn_hi]>(hi_req+2)):
            if_hi=nif_hi-1
            #print 'Using '+str(if_hi)+' for upper SPW edge'
            nif_hi=14
            break

    if (if_low==-1):
        print 'Lower bound ('+str(low_req)+') out of range'

    if (if_hi==-1):
        print 'Upper bound ('+str(hi_req)+') out of range'

    spw=str(if_low)+'~'+str(if_hi)

    return spw


def split_vis(infile, outdir, min_freq, max_freq, step_freq, width_freq, spec_window, sel_freq):

    if not infile:
        raise Exception('infile is empty')

    if not outdir:
        raise Exception('output dir is empty')
 
    steps = (max_freq - min_freq) / step_freq
    rem = (max_freq - min_freq) % step_freq
    if rem:
        steps += 1

    freq1 = min_freq
    freq2 = min_freq + step_freq
    bottom_edge = re.search('_[0-9]{3}_', infile)
    if (bottom_edge):
        bedge = bottom_edge.group(0)
        bedge = int(bedge[1:4])
    
    if (not sel_freq):
        steps = 1
    
    for i in range(steps):
        if (sel_freq):
            if (rem and (i == steps - 1)):
                freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
                cvel_freq_range = '%f~%f' % (min_freq - 0.2 + i * step_freq, max_freq + 0.2)
            else:
                freq_range = str(freq1) + '~' + str(freq2)
                cvel_freq_range =  str(int(freq1-1)) + '~' + str(int(freq2+1))

            spw_range = spec_window + ':' + freq_range + 'MHz'
            cvel_spw_range = spec_window + ':' + cvel_freq_range + 'MHz'
            # spanning spectral windows and selecting freq fails
            # so use freq_map
            # THEREFORE ~10 lines above are IGNORED!!
            cvel_spw_range = freq_map(freq1, freq2, bedge)
        else:
            freq_range = 'min~max'
            spw_range = spec_window
    
        cvel_spw_range = ''
        no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!

        mstransform(vis=infile,
                    outputvis=outdir,
                    regridms=True,
                    restfreq='1420.405752MHz',
                    mode='frequency',
                    nchan=no_chan,
                    outframe='lsrk',
                    interpolation='linear',
                    veltype='radio',
                    start=str(freq1)+'MHz',
                    width=str(width_freq)+'kHz',
                    spw=cvel_spw_range,
                    combinespws =  True,
                    nspw        =  1,
                    createmms   =  False,
                    datacolumn  =  "data")
          
        freq1 = freq1 + step_freq
        freq2 = freq2 + step_freq


if __name__ == '__main__':

    try:
        print input_vis, output_dir, min_freq, max_freq, step_freq, width_freq,spec_window,sel_freq
    
        split_vis(
            input_vis, 
            output_dir, 
            min_freq, 
            max_freq, 
            step_freq, 
            width_freq,
            spec_window,
            sel_freq)

    except Exception as ex:
        print >> sys.stderr, 'SEVERE ' + str(ex)

