# Instructions to run the validation code on Spark cluster


The Apache Spark cluster is a powerful framework that is used for big data processing of data stored in Hadoop. The Spark components work as a Cloud in the sense that you can connect to them in an interactive way via Swan (Jupyter notebook) or via shell. The interactive way is the easiest one, [Swan Projects](https://swan002.cern.ch), so notebooks can be runned there to develope and test code. In the other hand, a connection should be made via a lxplus machine.


In both cases it's necessary to request acces to the Spark cluster and Hadoop space system before starting. This can be done using the [link](https://hadoop-user-guide.web.cern.ch/getstart/access.html).


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


# Connection with the Muon validation code (This is our main way)

The optimal way to get a connection and to install the code:


```
git clone https://gitlab.cern.ch/cms-muonPOG/spark_tnp.git

cd spark_tnp

source env.sh

kinit
```


# How to generate distributions


The code is developed in the context of the Muon-POG spark code, so, as in the case of efficiencies, it reads the configuration file and plot the ratio and no ratio plots of the one dimentional variables initialized in the section "binVariables" of the configuration.json file.

To run the code, two different options:

## Produce Data/MC distributions for a full era:

```
./tnp_fitter.py compare particle probe resonance era configs/muon_example.json --baseDir ./example
```

For example:

```
./tnp_fitter.py compare muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example
```

## Produce distributions comparing two specific suberas:

Two options, compare Data or MC datasets from the same era or from different eras. In the first case:

```
./tnp_fitter.py compare particle probe resonance era configs/muon_example.json --baseDir ./example --subera1 SubEra1 --subera2 SubEra2
```

For example:

```
./tnp_fitter.py compare muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example --subera1 Run2018A --subera2 DY_madgraph
```

In the second case, from two different eras:

```
./tnp_fitter.py compare particle probe resonance era1 configs/muon_example.json --baseDir ./example --subera1 SubEra1 --subera2 SubEra2 --era2 Era2
```

For example:

```
./tnp_fitter.py compare muon generalTracks Z Run2018_UL configs/muon_example.json --baseDir ./example --subera1 Run2018A --subera2 DY_madgraph --era2 Run2016_UL
```





