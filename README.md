# Hadoop Service Discovery on Aurora

This generates a [pex](https://pex.readthedocs.org/en/latest/) file which will perform service
discovery into a local ZooKeeper ensemble to find the correct location of a headnode (which includes
both a Name Node and a Resource Manager). It will then update the local configuration file so the
workernode knows how to communicate with the headnode.
