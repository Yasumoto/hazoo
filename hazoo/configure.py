import os
import socket
from textwrap import dedent

from config import HadoopXmlConf

class Configure(object):

  def __init__(self, property_list, **kwargs):
    self.hadoopxml = HadoopXmlConf()
    for prop in property_list:
      self.hadoopxml.set(prop + '.port', str(kwargs[prop].port))
      self.hadoopxml.set(prop + '.host', kwargs[prop].host)

  def generate_xml(self, preset_properties):
    with open(preset_properties,'r') as f:
      for line in f:
        k,v = line.split('=')
        # Don't override if already set
        if not self.hadoopxml.get(k, None):
          self.hadoopxml.set(k, v.strip())

    return self.hadoopxml.to_str()
