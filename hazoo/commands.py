from .discovery import Discovery
from .configure import Configure

from twitter.common import app

@app.command_option(
  '--output',
  dest='output',
  default='theconfig.xml'
  type=str,
  help='Location of the file to write XML configuration to.')
@app.command
def create_workernode(args, options):
  """Bootstrap a new Hadoop Worker node
  
  Usage: create_workernode <zookeeper_url>
  """
  try:
    zookeeper_ensemble_url = args[1]
  except IndexError:
    app.quit("Please specify a url to perform Service Discovery")
  
  headnode = Discovery(zookeeper_ensemble_url).retrieve_headnode()
  
  configured_xml = Configure(headnode).generate_xml()
  
  with open(options.output, 'w') as fp:
    fp.write(configured_xml)