import getpass
import os
import shutil

from .discovery import Discovery
from .configure import Configure

from twitter.common import app
from twitter.common import log

@app.command_option(
  '--output',
  dest='output',
  default='theconfig.xml',
  type=str,
  help='Location of the file to write XML configuration to.')
@app.command_option(
  '--nm_web_port',
  dest='nm_web_port',
  type=int)
@app.command_option(
  '--nm_main_port',
  dest='nm_main_port',
  type=int)
@app.command_option(
  '--nm_loc_port',
  dest='nm_loc_port',
  type=int)
@app.command_option(
  '--nm_shuffle_port',
  dest='nm_shuffle_port',
  type=int)
@app.command_option(
  '--dn_web_port',
  dest='dn_web_port',
  type=int)
@app.command_option(
  '--dn_ipc_port',
  dest='dn_ipc_port',
  type=int)
@app.command_option(
  '--dn_rpc_port',
  dest='dn_rpc_port',
  type=int)
@app.command
def create_workernode(args, options):
  """Bootstrap a new Hadoop Worker node
  
  Usage: create_workernode <zookeeper_url>
  """
  try:
    zookeeper_ensemble_url = args[0]
    log.debug("Using %s" % zookeeper_ensemble_url)
  except IndexError:
    print("Please specify a url to perform Service Discovery")
    app.quit(1)
  
  log.debug('Identifying headnode')
  headnode_endpoints = Discovery(zookeeper_ensemble_url).retrieve_headnode_endpoint(
      '/twitter/service/%s/devel/headnode' % getpass.getuser())
  log.debug('Found endpoints: %s' % headnode_endpoints)
  
  configured_xml = Configure(**headnode_endpoints).generate_xml(options.nm_web_port,
      options.nm_main_port, options.nm_loc_port, options.nm_shuffle_port, options.dn_web_port,
      options.dn_ipc_port, options.dn_rpc_port)
  
  with open(options.output, 'w') as fp:
    fp.write(configured_xml)
    
  os.symlink(options.output, 'core-site.xml')
  os.symlink(options.output, 'yarn-site.xml')
  os.symlink(options.output, 'hdfs-site.xml')
  os.symlink(options.output, 'mapred-site.xml')
  
  with open('fair-scheduler.xml', 'w') as fp:
    fp.write('<allocations><queue name=\'root\'></queue></allocations>')
  
  with open('hadoop-release', 'w') as fp:  
    fp.write('hadoop-client-2.4.0.t04')
  
  shutil.copy('./etc/hadoop/log4j.properties', '.')