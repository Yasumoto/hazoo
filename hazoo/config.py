from __future__ import print_function

from contextlib import contextmanager
from xml.etree import ElementTree as ET
from xml.dom import minidom
import errno
import os
import pprint
import subprocess
import sys

from twitter.common.contextutil import temporary_dir


def _to_nice_xml(doc):
  rough_string = ET.tostring(doc, 'utf-8')
  reparsed = minidom.parseString(rough_string)
  return reparsed.toprettyxml(indent="  ")



class HadoopXmlConf(object):
  def __init__(self):
    self.props = {}

  def get(self, key, default_value=None):
    return self.props.get(key, default_value)

  def set(self, key, value):
    self.props[key] = value

  def to_str(self):
    hconf = ET.Element('configuration')
    for k, v in self.props.items():
      prop = ET.SubElement(hconf, 'property')
      name = ET.SubElement(prop, 'name')
      name.text = k
      value = ET.SubElement(prop, 'value')
      value.text = v
    return _to_nice_xml(hconf)


class HadoopConfig(object):
  @classmethod
  def from_path(cls, path):
    def iterate_files():
      for filename in os.listdir(path):
        try:
          with open(os.path.join(path, filename)) as fp:
            yield filename, fp.read()
        except (IOError, OSError) as e:
          if e.errno == errno.ENOENT:
            continue
          raise
    return cls(dict(iterate_files()))

  def __init__(self, content_map):
    self.__content = content_map.copy()

  @contextmanager
  def temporary(self):
    with temporary_dir() as td:
      for filename, content in self.__content.items():
        with open(os.path.join(td, filename), 'w') as fp:
          fp.write(content)
      yield td

  @contextmanager
  def override(self, xml_name):
    path = xml_name + '.xml'
    doc = HadoopXmlConf(self.__content[path])
    yield doc
    self.__content[path] = doc.to_str()


class Hadoop(object):
  def __init__(self, prefix, config_path=None):
    self.prefix = prefix
    self.config = HadoopConfig.from_path(config_path or os.path.join(prefix, 'etc', 'hadoop'))

  def execute(self, command, args):
    env_copy = os.environ.copy()
    with self.config.temporary() as conf_dir:
      env_copy['HADOOP_PREFIX'] = self.prefix
      env_copy['HADOOP_CONF_DIR'] = conf_dir
      env_copy['YARN_CONF_DIR'] = conf_dir

      if command in ('yarn', 'hadoop'):
        args = ['--config', conf_dir] + args

      command = os.path.join(self.prefix, 'bin', command)
      command_str = '%s %s' % (command, ' '.join(args))

      print('Executing command: %s' % command_str, file=sys.stderr)
      print('Environment:\n%s' % pprint.pformat(env_copy), file=sys.stderr)

      sys.stdout.flush()
      sys.stderr.flush()

      os.execve(command, [command] + args, env_copy)


class Twadoop(Hadoop):
  @classmethod
  def get_version(cls, path):
    po = subprocess.Popen('/usr/bin/hadoop-version %s' % path, shell=True, stdout=subprocess.PIPE)
    rc = po.wait()
    if rc != 0:
      raise RuntimeError('Could not get hadoop version of %s' % path)
    return po.stdout.read().strip()

  def __init__(self, config_path):
    version = self.get_version(config_path)
    prefix = os.path.join('/usr/local', version)
    super(Twadoop, self).__init__(prefix, config_path=config_path)
