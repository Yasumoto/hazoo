from setuptools import find_packages, setup

__version__ = '0.0.1'


setup(
  name='hazoo',
  version=__version__,
  description='Service Discovery and Configuration Provisioner for Apache Hadoop worker nodes',
  url='http://github.com/Yasumoto/hazoo',
  author='Joe Smith',
  author_email='yasumoto7@gmail.com',
  license='Apache License 2.0',
  packages=find_packages(exclude=['tests']),
  install_requires=[
    'twitter.common.app==0.3.3',
    'twitter.common.zookeeper==0.3.3',
  ],
  entry_points = {
    'console_scripts': [
      'hazoo = hazoo.bin.hazoo:proxy_main',
    ],
  },
  zip_safe=True
)
