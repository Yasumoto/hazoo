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
    
    config_string = dedent("""<configuration>
                                <property>
                              	  <name>mapred.fairscheduler.allocation.file</name>
                              		<value>./fair-scheduler.xml</value>
                              	</property>
                                <property>
                              	  <name>mapreduce.framework.name</name>
                              		<value>yarn</value>
                              	</property>
                                <property>
                              	  <name>yarn.nodemanager.aux-services</name>
                              		<value>mapreduce_shuffle</value>
                              	</property>
                                <property>
                              	  <name>yarn.resourcemanager.scheduler.class</name>
                              		<value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
                              	</property>
                              </configuration>""")
    self.hadoopxml = HadoopXmlConf(config_string)

  def generate_xml(self, nm_web_port, nm_main_port, nm_loc_port, nm_shuffle_port, dn_web_port, dn_ipc_port, dn_rpc_port):
    """
    :rtype: str
    """
    hadoop_opts = {
        'yarn.resourcemanager.hostname': self.headnode,
        'fs.defaultFS': 'hdfs://%s:%d' % (self.headnode, self.nn_rpc_port),
        'fs.default.name': 'hdfs://%s:%d' % (self.headnode, self.nn_rpc_port),
        'dfs.namenode.http-address': '%s:%d' % (self.headnode, self.nn_http_port),
        'dfs.namenode.rpc-address': '%s:%d' % (self.headnode, self.nn_rpc_port),
        'dfs.namenode.servicerpc-address': '%s:%d' % (self.headnode, self.nn_srpc_port),
        'yarn.resourcemanager.scheduler.class':
        'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler',
        'mapred.fairscheduler.allocation.file': os.path.join(self.cwd, 'fair-scheduler.xml'),
        'mapreduce.framework.name': 'yarn',
        'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
        
        'yarn.resourcemanager.address': '%s:%d' %(self.headnode, self.rm_rpc_port),
        'yarn.resourcemanager.resource-tracker.address': '%s:%d' %(self.headnode, self.rm_tracker_port),
        'yarn.resourcemanager.scheduler.address': '%s:%d' %(self.headnode, self.rm_sch_port),
        'yarn.resourcemanager.webapp.address': '%s:%d' %(self.headnode, self.rm_web_port),
        'yarn.resourcemanager.admin.address': '%s:%d' %(self.headnode, self.rm_admin_port),
        'mapreduce.jobhistory.address': '%s:%d' %(self.headnode, self.rm_hist_port),
        'mapreduce.jobhistory.webapp.address': '%s:%d' %(self.headnode, self.rm_histweb_port),
        
        'yarn.nodemanager.webapp.address': '%s:%d' % (self.hostname, nm_web_port),
        'yarn.nodemanager.address': '%s:%d' % (self.hostname, nm_main_port),
        'yarn.nodemanager.localizer.address': '%s:%d' % (self.hostname, nm_loc_port),
        'mapreduce.shuffle.port': "%d" % nm_shuffle_port,
        'dfs.datanode.http.address': '%s:%d' % (self.hostname, dn_web_port),
        'dfs.datanode.ipc.address': '%s:%d' % (self.hostname, dn_ipc_port),
        'dfs.datanode.address': '%s:%d' % (self.hostname, dn_rpc_port),
        
        'dfs.namenode.name.dir': os.path.join(self.cwd, 'nndata'),
        'dfs.datanode.data.dir': os.path.join(self.cwd, 'dndata'),
        'yarn.nodemanager.local-dirs': os.path.join(self.cwd, 'yarn_local'),
        'yarn.nodemanager.log-dirs': os.path.join(self.cwd, 'yarn_log'),
    }
    for key, value in hadoop_opts.items():
      self.hadoopxml.set(key, value)

    return self.hadoopxml.to_str()
