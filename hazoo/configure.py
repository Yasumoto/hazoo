import os
import socket
from textwrap import dedent

from config import HadoopXmlConf

class Configure(object):
  HEADNODE_HOSTNAME = 'hostname'
  RM_RPC_PORT = 'rm_rpc'
  NN_RPC_PORT = 'nn_rpc'
  NN_SRPC_PORT = 'nn_srpc'
  RM_TRACKER_PORT = 'rm_tracker'
  RM_SCH_PORT = 'rm_sch'
  RM_HIST_PORT = 'rm_hist'
  RM_HISTWEB_PORT = 'rm_hist_web'
  RM_WEB_PORT = 'rm_web'
  RM_ADMIN_PORT = 'rm_admin'
  NN_HTTP_PORT = 'nn_http'

  def __init__(self, **kwargs):
    """Requires an AdditionalEndpoints dictionary used when announcing a headnode
    """
    self.headnode = kwargs[self.RM_RPC_PORT].host
    self.rm_rpc_port = kwargs[self.RM_RPC_PORT].port
    self.nn_rpc_port = kwargs[self.NN_RPC_PORT].port
    self.nn_srpc_port = kwargs[self.NN_SRPC_PORT].port
    self.rm_tracker_port = kwargs[self.RM_TRACKER_PORT].port
    self.rm_sch_port = kwargs[self.RM_SCH_PORT].port
    self.rm_hist_port = kwargs[self.RM_HIST_PORT].port
    self.rm_histweb_port = kwargs[self.RM_HISTWEB_PORT].port
    self.rm_web_port = kwargs[self.RM_WEB_PORT].port
    self.rm_admin_port = kwargs[self.RM_ADMIN_PORT].port
    self.nn_http_port = kwargs[self.NN_HTTP_PORT].port
    self.cwd = os.getcwd()
    self.hostname = socket.gethostname()
    

    self.hadoopxml = HadoopXmlConf()

  def generate_xml(self, preset_properties):
    """
    :rtype: str
    """
    with open(preset_properties,'r') as f:
      for line in f:
        k,v = line.split('=')
        self.hadoopxml.set(k, v)

    return self.hadoopxml.to_str()
