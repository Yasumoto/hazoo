from __future__ import print_function

from contextlib import contextmanager
from xml.etree import ElementTree as ET
import errno
import os
import pprint
import subprocess
import sys

from twitter.common.contextutil import temporary_dir


class HadoopXmlConf(object):
  def __init__(self, content):
    self.__doc = ET.fromstring(content)

  def _new_element(self, key):
    prop = ET.Element('property')
    name = ET.SubElement(prop, 'name')
    name.text = key
    self.__doc.append(prop)
    return prop

  def _find_element(self, key, create=False):
    for prop in self.__doc.findall('property'):
      name = prop.find('name')
      if name is not None and name.text == key:
        # hit
        return prop
    # miss
    if create:
      return self._new_element(key)

  def get(self, key):
    return self._find_element(key)

  def set(self, key, value):
    prop = self._find_element(key, create=True)
    value_element = prop.find('value') or ET.SubElement(prop, 'value')
    value_element.text = value

  def to_str(self):
    return ET.tostring(self.__doc)


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
