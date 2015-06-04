Note- I decided to cheat and use this in the (super) short-term instead of [Dynamic Yarn](http://github.com/wickman/darn) but will come back around to merge them if/when we move forward with our approach.

# Hadoop Service Discovery on Aurora

This generates a [pex](https://pex.readthedocs.org/en/latest/) file which will perform service
discovery into a local ZooKeeper ensemble to find the correct location of a headnode (which includes
both a Name Node and a Resource Manager). It will then update the local configuration file so the
workernode knows how to communicate with the headnode.
