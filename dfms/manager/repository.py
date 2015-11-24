#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
This module contains template functions for building physical graphs. It
currently is part of the repository for practical reasons, but it needs to be
moved out from the dfms project since it's too specific.

A piece of code exists already in the DM that would potentially look up for
these kind of template functions under a per-user plug-in directory system. A
different way would have to be thought if we want to have these templates as
system-wide installed artifacts.
"""

from dfms.drop import dropdict

def socket(uid, port=1111):
    return dropdict({'oid':uid, 'type':'socket', 'port':port})

def memory(uid, **kwargs):
    dropSpec = dropdict({'oid':uid, 'type':'plain', 'storage':'memory'})
    dropSpec.update(kwargs)
    return dropSpec

def sleepAndCopy(uid, **kwargs):
    dropSpec = dropdict({'oid':uid, 'type':'app', 'app':'test.graphsRepository.SleepAndCopyApp'})
    dropSpec.update(kwargs)
    return dropSpec

def complex_graph():
    """
    This method creates the following graph

    SL_A --> A -----> E --> G --|                         |--> N ------> Q --> S
                                |              |---> L ---|
    SL_B --> B -----------------|--> I --> J --|          |--> O -->|
                                |              |                    |--> R --> T
    SL_C --> C --|              |              |---> M ------> P -->|
                 |--> F --> H --|                |
    SL_D --> D --|                SL_K --> K ----|

    In this example the "leaves" of the graph are S and T, while the
    "roots" are SL_A, SL_B, SL_C, SL_D and SL_K.

    E, F, I, L, M, Q and R are AppDROPs; SL_* are SocketListenerApps. The
    rest are plain in-memory DROPs
    """

    sl_a,sl_b,sl_c,sl_d,sl_k       = [socket('sl_' + uid,port) for uid,port in ('a',1111),('b',1112),('c',1113),('d',1114),('k',1115)]
    a,b,c,d,k       = [memory(uid) for uid in ('a'),('b'),('c'),('d'),('k')]
    e,f,i,l,m,q,r   = [sleepAndCopy(uid) for uid in ['e', 'f', 'i', 'l', 'm', 'q', 'r']]
    g,h,j,n,o,p,s,t = [memory(uid) for uid in ['g', 'h', 'j', 'n', 'o', 'p', 's', 't']]

    for plainDrop, appDrop in [(a,e), (b,i), (c,f), (d,f), (h,i), (j,l), (j,m), (k,m), (n,q), (o,r), (p,r)]:
        plainDrop.addConsumer(appDrop)
    for appDrop, plainDrop in [(sl_a,a), (sl_b,b), (sl_c,c), (sl_d,d), (sl_k,k), (e,g), (f,h), (i,j), (l,n), (l,o), (m,p), (q,s), (r,t)]:
        appDrop.addOutput(plainDrop)

    return [sl_a,sl_b,sl_c,sl_d,sl_k,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t]

def archiving_app(uid, host, port):
    return [{'oid':uid, 'type':'app', 'app':'dfms.apps.archiving.NgasArchivingApp','ngasSrv':host,'ngasPort':port}]

def pip_cont_img_pg(num_beam=1, num_time=2, num_freq=2, num_facet=2, num_grid=4, stokes=['I', 'Q', 'U', 'V']):
    """
    PIP continuum imaging pipeline
    """

    allDrops = []
    dob_root_sl = socket('FullDataset_SL')
    dob_root = memory('FullDataset')
    dob_root_sl.addOutput(dob_root)
    allDrops.append(dob_root_sl)
    allDrops.append(dob_root)

    adob_sp_beam = sleepAndCopy("SPLT_BEAM")
    allDrops.append(adob_sp_beam)
    adob_sp_beam.location = "Buf01"
    dob_root.addConsumer(adob_sp_beam)

    for i in range(1, num_beam + 1):
        oid = i
        dob_beam = memory("BEAM_{0}".format(oid))
        allDrops.append(dob_beam)
        adob_sp_beam.addOutput(dob_beam)
        adob_sp_time = sleepAndCopy("SPLT_TIME_{0}".format(oid))
        allDrops.append(adob_sp_time)
        dob_beam.addConsumer(adob_sp_time)
        for j in range(1, num_time + 1):
            oid = "%d-%d" % (i, j)
            dob_time = memory("TIME_{0}".format(oid))
            allDrops.append(dob_time)
            adob_sp_time.addOutput(dob_time)
            adob_sp_freq = sleepAndCopy("SPLT_FREQ_{0}".format(oid))
            allDrops.append(adob_sp_freq)
            dob_time.addConsumer(adob_sp_freq)
            for k in range(1, num_freq + 1):
                oid = "%d-%d-%d" % (i, j, k)
                dob_freq = memory("FREQ_{0}".format(oid))
                allDrops.append(dob_freq)
                adob_sp_freq.addOutput(dob_freq)
                adob_sp_facet = sleepAndCopy("SPLT_FACET_{0}".format(oid))
                allDrops.append(adob_sp_facet)
                dob_freq.addConsumer(adob_sp_facet)
                for l in range(1, num_facet + 1):
                    oid = "%d-%d-%d-%d" % (i, j, k, l)
                    dob_facet = memory("FACET_{0}".format(oid))
                    allDrops.append(dob_facet)
                    adob_sp_facet.addOutput(dob_facet)

                    adob_ph_rot = sleepAndCopy("PH_ROTATN_{0}".format(oid))
                    allDrops.append(adob_ph_rot)
                    dob_facet.addConsumer(adob_ph_rot)
                    dob_ph_rot = memory("PH_ROTD_{0}".format(oid))
                    allDrops.append(dob_ph_rot)
                    adob_ph_rot.addOutput(dob_ph_rot)
                    adob_sp_stokes = sleepAndCopy("SPLT_STOKES_{0}".format(oid))
                    allDrops.append(adob_sp_stokes)
                    dob_ph_rot.addConsumer(adob_sp_stokes)

                    adob_w_kernel = sleepAndCopy("CACL_W_Knl_{0}".format(oid))
                    allDrops.append(adob_w_kernel)
                    dob_facet.addConsumer(adob_w_kernel)
                    dob_w_knl = memory("W_Knl_{0}".format(oid))
                    allDrops.append(dob_w_knl)
                    adob_w_kernel.addOutput(dob_w_knl)

                    adob_a_kernel = sleepAndCopy("CACL_A_Knl_{0}".format(oid))
                    allDrops.append(adob_a_kernel)
                    dob_facet.addConsumer(adob_a_kernel)
                    dob_a_knl = memory("A_Knl_{0}".format(oid))
                    allDrops.append(dob_a_knl)
                    adob_a_kernel.addOutput(dob_a_knl)

                    adob_create_kernel = sleepAndCopy("CREATE_Knl_{0}".format(oid))
                    allDrops.append(adob_create_kernel)
                    dob_w_knl.addConsumer(adob_create_kernel)
                    dob_a_knl.addConsumer(adob_create_kernel)

                    for stoke in stokes:
                        oid = "%s-%d-%d-%d-%d" % (stoke, i, j, k, l)
                        #print "oid = {0}".format(oid)

                        dob_stoke = memory("STOKE_{0}".format(oid))
                        allDrops.append(dob_stoke)
                        adob_sp_stokes.addOutput(dob_stoke)

                        dob_stoke.addConsumer(adob_create_kernel)


                        dob_aw = memory("A_{0}".format(oid))
                        allDrops.append(dob_aw)
                        adob_create_kernel.addOutput(dob_aw)

                        # we do not do loop yet
                        griders = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_gridding = sleepAndCopy("Gridding_{0}".format(gid))
                            allDrops.append(adob_gridding)
                            dob_stoke.addConsumer(adob_gridding)
                            dob_gridded_cell = memory("Grided_Cell_{0}".format(gid))
                            allDrops.append(dob_gridded_cell)
                            adob_gridding.addOutput(dob_gridded_cell)
                            griders.append(dob_gridded_cell)
                        adob_gridded_bar = sleepAndCopy("GRIDDED_BARRIER_{0}".format(oid))
                        allDrops.append(adob_gridded_bar)
                        for grider in griders:
                            grider.addConsumer(adob_gridded_bar)
                        dob_gridded_stoke = memory("GRIDDED_STOKE_{0}".format(oid))
                        allDrops.append(dob_gridded_stoke)
                        adob_gridded_bar.addOutput(dob_gridded_stoke)

                        FFTers = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_fft = sleepAndCopy("FFT_{0}".format(gid))
                            allDrops.append(adob_fft)
                            dob_gridded_stoke.addConsumer(adob_fft)

                            dob_ffted_cell = memory("FFTed_Cell_{0}".format(gid))
                            allDrops.append(dob_ffted_cell)
                            adob_fft.addOutput(dob_ffted_cell)
                            FFTers.append(dob_ffted_cell)
                        adob_ffted_bar = sleepAndCopy("FFTed_BARRIER_{0}".format(oid))
                        allDrops.append(adob_ffted_bar)
                        for ffter in FFTers:
                            ffter.addConsumer(adob_ffted_bar)
                        dob_ffted_stoke = memory("FFTed_STOKE_{0}".format(oid))
                        allDrops.append(dob_ffted_stoke)
                        adob_ffted_bar.addOutput(dob_ffted_stoke)

                        cleaners = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_cleaner = sleepAndCopy("DECONV_{0}".format(gid))
                            allDrops.append(adob_cleaner)
                            dob_ffted_stoke.addConsumer(adob_cleaner)

                            dob_cleaned_cell = memory("CLEANed_Cell_{0}".format(gid))
                            allDrops.append(dob_cleaned_cell)
                            adob_cleaner.addOutput(dob_cleaned_cell)
                            cleaners.append(dob_cleaned_cell)
                        adob_cleaned_bar = sleepAndCopy("CLEANed_BARRIER_{0}".format(oid))
                        allDrops.append(adob_cleaned_bar)
                        for cleaner in cleaners:
                            cleaner.addConsumer(adob_cleaned_bar)
                        dob_decon_stoke = memory("CLEANed_STOKE_{0}".format(oid))
                        allDrops.append(dob_decon_stoke)
                        adob_cleaned_bar.addOutput(dob_decon_stoke)

                        adob_create_prod = sleepAndCopy("CRT-PROD_{0}".format(oid))
                        allDrops.append(adob_create_prod)
                        dob_decon_stoke.addConsumer(adob_create_prod)

                        dob_prod = memory("PRODUCT_{0}".format(oid))
                        allDrops.append(dob_prod)
                        adob_create_prod.addOutput(dob_prod)

    return allDrops