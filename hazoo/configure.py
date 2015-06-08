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
        self.hadoopxml.set(k, v)

    return self.hadoopxml.to_str()
