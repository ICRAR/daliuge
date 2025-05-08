.. _cli_remote:
   
Remote Deployment Commands
##########################

Configuration
*************

The folling::   

    dlg config --setup

    > Do you want to create a $HOME/.config/dlg directory to store your custom configuration files and scripts  (y/n)?

Selecting ``y`` will set up the config directory, along with the default environ ini files and the slurm submission scripts::

    User Configs (~/.config/dlg)
    ----------------------------
    Environments (--config_file):
        setonix.ini
        default.ini
    Slurm scripts (--slurm_template):
        default.slurm
        setonix.slurm

    DALiuGE Defaults (-f/--facility):
    -----------------------------------
        galaxy_mwa
        galaxy_askap
        magnus
        galaxy
        setonix
        shao
        hyades
        ood
        ood_cloud

The above can be listed at any time after initially created using the ``list`` option:

    dlg config --list

Remote Deployment
********************

The following is a complete example::

    dlg create -a submit -n 1 -s 1 -u -f setonix 
    -L ~/github/EAGLE_test_repo/eagle_test_graphs/daliuge_tests/dropmake/logical_graphs/ArrayLoop.graph 
    -v 5 --remote --submit 
    --config_file setonix.ini --slurm_template setonix.slurm