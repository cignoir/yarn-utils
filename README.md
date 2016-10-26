# yarn-utils

A script that configure the resources of Hadoop yarn, 

`yarn-utils.py` has been ported to Ruby.

See also: http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.4.3/bk_installing_manually_book/content/determine-hdp-memory-config.html

## Usage
### Command
```sh
$ ruby yarn-utils.rb --help
```

### Result
```
Usage: yarn-utils [options]
    -c, --cores VALUE                The number of cores on each host.
    -m, --memory VALUE               The amount of memory on each host in GB.
    -d, --disks VALUE                The number of disks on each host.
    -k, --hbase VALUE                "true" if HBase is installed, "false" if not.
```

## Example
### Command
```sh
$ ruby yarn-utils.rb -c 8 -m 61 -d 1 -k false
```

### Result
```
Using cores=8 memory=61GB disks=1 hbase=false
Profile: cores=8 memory=61440MB reservedMem=1GB usableMem=60GB disks=1
Num Container=3
Container Ram=20480 MB
Used Ram=60GB
Unused Ram=1GB
yarn.scheduler.minimum-allocation-mb=20480
yarn.scheduler.maximum-allocation-mb=61440
yarn.nodemanager.resource.memory-mb=61440
mapreduce.map.memory.mb=20480
mapreduce.map.java.opts=-Xmx16384m
mapreduce.reduce.memory.mb=20480
mapreduce.reduce.java.opts=-Xmx16384m
yarn.app.mapreduce.am.resource.mb=20480
yarn.app.mapreduce.am.command-opts=-Xmx16384m
mapreduce.task.io.sort.mb=8192
```
