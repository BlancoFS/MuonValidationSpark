# Instructions to run the validation code on Spark cluster


The Apache Spark cluster is a powerful set of machines that is used to run jobs on data stored in Hadoop. The Spark components work as a Cloud in the sense that you can connect them in an interactive way or via unix. The interactive way is the easiest one, [Swan Projects](https://swan002.cern.ch), so notebooks can be runned there to develope and test code. In the other option, a connection should be made via a lxplus machine.


In both cases it's necessary to request acces to the Spark cluster and Hadoop space system before starting. This is done in the [link](https://hadoop-user-guide.web.cern.ch/getstart/access.html).


# Start a session with Swan

It's trivial. Just have to select "Analytix" in the Spark cluster option when it's initillized.

# Start a session with lxplus

To connect to Spark and Hadoop via lxplus machine:

```
source /cvmfs/sft.cern.ch/lcg/views/LCG_99/x86_64-centos7-gcc8-opt/setup.sh
```

```
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh <cluster name> spark3
```

Here, the cluster name can be: **hadoop-qa, analytix, lxhadoop, nxcals-prod**. Now, the option to use is:

```
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix spark3
```


Then, use:

```
kinit
```

It will ask your password. Finally, if you have permision to use hadoop you can test:


```
hdfs dfs -ls /
```

or 

```
hdfs dfs -mkdir /hdfs/user/UserName/testFolder
```


# Connect with hadoop from lxplus

These commands are used to get connected directly to the /hdfs space, but problems can occur.

```
ssh it-hadoop-client

kinit
```


# Download of Muon-POG code and Spark connection from it

The optimal way to get a connection and to install the code:


```
git clone https://gitlab.cern.ch/cms-muonPOG/spark_tnp.git

cd spark_tnp

source env.sh

kinit
```








