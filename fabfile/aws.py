#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
Module containing AWS-related methods and tasks
"""

import os
import time

from fabric.colors import green, red, blue, yellow
from fabric.contrib.console import confirm
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts, abort, fastprint

from APPspecific import APP_revision, APP_user, APP_name
from utils import default_if_empty, whatsmyip, check_ssh, key_filename
from config import DEFAULT_AWS_KEY_NAME, DEFAULT_AWS_SEC_GROUP, DEFAULT_PORTS

# Don't re-export the tasks imported from other modules
__all__ = ['create_aws_instances', 'list_instances', 'terminate']

# Available known AMI IDs
AMI_IDs = {
           'Amazon': 'ami-6178a31e',
           'Amazon-hvm': 'ami-5679a229',
           'CentOS': 'ami-8997afe0',
           'Old_CentOS': 'ami-aecd60c7',
           'SLES-SP2': 'ami-e8084981',
           'SLES-SP3': 'ami-c08fcba8'
           }

# Instance creation defaults
DEFAULT_AWS_AMI_NAME = 'Amazon'
DEFAULT_AWS_INSTANCES = 1
DEFAULT_AWS_INSTANCE_NAME_TPL = '{0}'.format(APP_name()+'_{0}') #  gets formatted with the git branch name
DEFAULT_AWS_INSTANCE_TYPE = 't1.micro'

# Connection defaults
DEFAULT_AWS_PROFILE = 'NGAS'
DEFAULT_AWS_REGION = 'us-east-1'


def connect():
    import boto.ec2
    default_if_empty(env, 'AWS_PROFILE', DEFAULT_AWS_PROFILE)
    default_if_empty(env, 'AWS_REGION',  DEFAULT_AWS_REGION)
    return boto.ec2.connect_to_region(env.AWS_REGION,
                                      profile_name=env.AWS_PROFILE)


def userAtHost():
    return os.environ['USER'] + '@' + whatsmyip()


def aws_create_key_pair(conn):

    key_name = env.AWS_KEY_NAME
    key_file = key_filename(key_name)

    # key does not exist on AWS, create it there and bring it back,
    # overwriting anything we have
    kp = conn.get_key_pair(key_name)
    if not kp:
        kp = conn.create_key_pair(key_name)
        if os.path.exists(key_file):
            os.unlink(key_file)

    # We don't have the private key locally, save it
    if not os.path.exists(key_file):
        kp.save('~/.ssh/')


def check_create_aws_sec_group(conn):
    """
    Check whether the APP security group exists
    """

    default_if_empty(env, 'AWS_SEC_GROUP', DEFAULT_AWS_SEC_GROUP)

    app_secgroup = env.AWS_SEC_GROUP
    sec = conn.get_all_security_groups()
    conn.close()
    appsg = None
    for sg in sec:
        if sg.name.upper() == app_secgroup:
            puts(green("AWS Security Group {0} exists ({1})".
                       format(app_secgroup, sg.id)))
            appsg = sg

    # Not found, create a new one
    if appsg is None:
        appsg = conn.create_security_group(app_secgroup,
                                           '{0} default permissions'.
                                           format(APP_name))
        for p in DEFAULT_PORTS.values():
            appsg.authorize('tcp', p, p, '0.0.0.0/0')

        appsg.authorize('tcp', 22, 22, '0.0.0.0/0')
        appsg.authorize('tcp', 80, 80, '0.0.0.0/0')

    return appsg.id


def create_instances(conn, sgid):
    """
    Create one or more EC2 instances
    """

    default_if_empty(env, 'AWS_AMI_NAME',            DEFAULT_AWS_AMI_NAME)
    default_if_empty(env, 'AWS_INSTANCE_TYPE',       DEFAULT_AWS_INSTANCE_TYPE)
    default_if_empty(env, 'AWS_INSTANCES',           DEFAULT_AWS_INSTANCES)

    n_instances = int(env.AWS_INSTANCES)
    if n_instances > 1:
        names = ["%s_%d" % (env.AWS_INSTANCE_NAME, i) for i in
                 xrange(n_instances)]
    else:
        names = [env.AWS_INSTANCE_NAME]
    puts('Creating instances {0}'.format(names))

    public_ips = None
    if 'AWS_ELASTIC_IPS' in env:

        public_ips = env.AWS_ELASTIC_IPS.split(',')
        if len(public_ips) != n_instances:
            abort("n_instances != #AWS_ELASTIC_IPS (%d != %d)" %
                  (n_instances, len(public_ips)))

        # Disassociate the public IPs
        for public_ip in public_ips:
            if not conn.disassociate_address(public_ip=public_ip):
                abort('Could not disassociate the IP {0}'.format(public_ip))

    reservations = conn.run_instances(AMI_IDs[env.AWS_AMI_NAME],
                                      instance_type=env.AWS_INSTANCE_TYPE,
                                      key_name=env.AWS_KEY_NAME,
                                      security_group_ids=[sgid],
                                      min_count=n_instances,
                                      max_count=n_instances)
    instances = reservations.instances

    # Sleep so Amazon recognizes the new instance
    for i in range(4):
        fastprint('.')
        time.sleep(5)

    # Are we running yet?
    iid = [x.id for x in instances]
    stat = conn.get_all_instance_status(iid)
    running = [x.state_name == 'running' for x in stat]
    puts('\nWaiting for instances to be fully available:\n')
    while sum(running) != n_instances:
        fastprint('.')
        time.sleep(5)
        stat = conn.get_all_instance_status(iid)
        running = [x.state_name == 'running' for x in stat]
    puts('.')  # enforce the line-end

    # Local user and host
    userAThost = userAtHost()

    # We save the user under which we install APP for later display
    nuser = APP_user()

    # Tag the instance
    for name, instance in zip(names, instances):
        conn.create_tags([instance.id], {'Name': name,
                                         'Created By': userAThost,
                                         'APP User': nuser,
                                         })

    # Associate the IP if needed
    if public_ips:
        for instance, public_ip in zip(instances, public_ips):
            puts('Current DNS name is {0}. About to associate the Elastic IP'.
                 format(instance.dns_name))
            if not conn.associate_address(instance_id=instance.id,
                                          public_ip=public_ip):
                abort('Could not associate the IP {0} to the instance {1}'.
                      format(public_ip, instance.id))

    # Load the new instance data as the dns_name may have changed
    host_names = []
    for instance in instances:
        instance.update(True)
        print_instance(instance)
        host_names.append(str(instance.dns_name))
    return host_names

def default_instance_name():
    rev = APP_revision()
    return DEFAULT_AWS_INSTANCE_NAME_TPL.format(rev)

@task
def create_aws_instances():
    """
    Create AWS instances and let Fabric point to them

    This method creates AWS instances and points the fabric environment to them
    with the current public IP and username.
    """

    default_if_empty(env, 'AWS_KEY_NAME',      DEFAULT_AWS_KEY_NAME)
    default_if_empty(env, 'AWS_INSTANCE_NAME', default_instance_name)

    # Create the key pair and security group if necessary
    conn = connect()
    aws_create_key_pair(conn)
    sgid = check_create_aws_sec_group(conn)

    # Create the instance in AWS
    host_names = create_instances(conn, sgid)

    # Update our fabric environment so from now on we connect to the
    # AWS machine using the correct user and SSH private key
    env.hosts = host_names
    env.key_filename = key_filename(env.AWS_KEY_NAME)
    if env.AWS_AMI_NAME in ['CentOS', 'SLES']:
        env.user = 'root'
    else:
        env.user = 'ec2-user'

    # Instances have started, but are not usable yet, make sure SSH has started
    puts('Started the instance(s) now waiting for the SSH daemon to start.')
    execute(check_ssh, timeout=300)

@task
def list_instances(name=None):
    """
    Lists the EC2 instances associated to the user's amazon key
    """
    conn = connect()
    res = conn.get_all_instances()
    for r in res:
        for inst in r.instances:
            print_instance(inst, name=name)


def print_instance(inst, name=None):
    inst_id    = inst.id
    inst_state = inst.state
    inst_type  = inst.instance_type
    pub_name   = inst.public_dns_name
    tagdict    = inst.tags
    l_time     = inst.launch_time
    key_name   = inst.key_name
    nuser = None
    outdict = {}
    outfl = True    # Controls whether info is printed

    outlist = [u'Name', u'Instance', u'Launch time', u'APP User', u'Connect',
               u'Terminate']  # defines the print order
    outdict['Instance'] = '{0} ({1}) is {2}'.format(inst_id, inst_type,
                                                    color_ec2state(inst_state
                                                                   ))
    for k, val in tagdict.items():
        if k == 'Name':
            val = blue(val)
            if name is not None:
                name = unicode(name)
                if val.find(name) == -1:
                    outfl = False
                else:
                    puts(name)
        outdict[k] = val
    if u'APP User' in outdict.keys():
        nuser = outdict[u'APP User']
    if u'NGAS User' in outdict.keys():
        nuser = outdict[u'NGAS User']
    if inst_state == 'running':
        ssh_user = ' -l%s' % (nuser) if nuser else ''
        outdict['Connect'] = 'ssh -i ~/.ssh/{0}.pem {1}{2}'.format(key_name,
                                                                   pub_name,
                                                                   ssh_user)
        outdict['Terminate'] = 'fab aws.terminate:instance_id={0}'.format(
            inst_id)
        outdict['Launch time'] = '{0}'.format(l_time)
    if outfl:
        for k in outlist:
            if outdict.has_key(k):
                puts("{0}: {1}".format(k, outdict[k]))
        puts('')


def color_ec2state(state):
    if state == 'running':
        return green(state)
    elif state == 'terminated':
        return red(state)
    elif state == 'shutting-down':
        return yellow(state)
    return state

@task
def terminate(instance_id):
    """
    Task to terminate the boto instances
    """
    if not instance_id:
        abort('No instance ID specified. Please provide one.')

    conn = connect()
    inst = conn.get_all_instances(instance_ids=[instance_id])
    inst = inst[0].instances[0]
    tagdict = inst.tags
    print_instance(inst)

    puts('')
    if tagdict.has_key('Created By') and tagdict['Created By'] != userAtHost():
        puts('******************************************************')
        puts('WARNING: This instances has not been created by you!!!')
        puts('******************************************************')
    if confirm("Do you really want to terminate this instance?"):
        puts('Teminating instance {0}'.format(instance_id))
        conn.terminate_instances(instance_ids=[instance_id])
    else:
        puts(red('Instance NOT terminated!'))
    return
