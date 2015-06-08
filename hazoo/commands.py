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
  '--preset_properties',
  dest='preset_properties',
  type='string')
@app.command_option(
  '--dynamic_properties',
  dest='dynamic_properties',
  type='string')
@app.command_option(
  '--headnode_path',
  dest='headnode_path',
  type=str,
  default='/twitter/service/%s/devel/headnode' % getpass.getuser(),
  help='Which Service Discovery path to lookup for the headnode',
)
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
      options.headnode_path)
  log.debug('Found endpoints: %s' % headnode_endpoints)
  
  configured_xml = Configure(options.dynamic_properties.split(','),
                             **headnode_endpoints).generate_xml(options.preset_properties)
  
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