from __future__ import print_function
import os
import glob
import numpy as np
import pandas as pd
import itertools

#import uproot

from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

from iminuit import Minuit, describe
from scipy.stats import expon
from scipy.special import wofz, erfc

from muon_definitions import *

from registry import registry

import importlib.util
import sys
import math
import json
from array import array
import ctypes
import ROOT
import tdrstyle
import CMS_lumi

from dataset_allowed_definitions import get_allowed_sub_eras, get_data_mc_sub_eras

from compare_one_job import *


def compare_multiple(particle, probe, resonance, era, config, **kwargs):
  
    _baseDir = kwargs.pop('baseDir', '')
    _subera1 = kwargs.pop('subera1', '')
    _subera2 = kwargs.pop('subera2', '')
    _era2    = kwargs.pop('era2', '')
  
    muon_IDs = []
    efficiencies = config.efficiencies()

    if len(efficiencies) == 1:
        muon_IDs.append(efficiencies[0][0])
    else:
        for eff_pair in efficiencies:
            if len(eff_pair) == 1:
                muon_IDs.append(eff_pair[0])
            else:
                muon_IDs.append(eff_pair[0])
                muon_IDs.append(eff_pair[1])

    muon_IDs = list(dict.fromkeys(muon_IDs))
    
    if not os.path.exists('OUTPUT'):
                    os.makedirs('OUTPUT')
    
    for muon_ID in muon_IDs:
      
      
        args = [particle, probe, resonance, era, config, muon_ID, _baseDir, _subera1, _subera2, _era2]
        
        files = ['env.sh', 'tdrstyle.py', 'CMS_lumi.py', 'dataset_allowed_definitions.py', 'registry.py', 'muon_definitions.py']

        
        arguments = './compare_one_job.py {}'.format(
            ' '.join([f'$({a})' for a in args]),)
        
        queue = 'queue {} from {}'.format(
            ','.join(args),
            joblist,
        )
        
        flavour = 'longlunch'

        
        output = 'OUTPUT/job_'+ muon_ID + '.$(ClusterId).$(ProcId).out'
        error = 'OUTPUT/job_'+ muon_ID + '.$(ClusterId).$(ProcId).err' 
        log = 'OUTPUT/job_'+ muon_ID + '.$(ClusterId).$(ProcId).log' 
        
        
        config = '''universe    = vanilla
                    executable  = condor_wrapper.sh
                    arguments   = {arguments}
                    transfer_input_files = {files}
                    output      = {output}
                    error       = {error}
                    log         = {log}
                    +JobFlavour = "{flavour}"
                    {queue}'''.format(
                                      arguments=arguments,
                                      files=','.join(files),
                                      output=output,
                                      error=error,
                                      log=log,
                                      flavour=flavour,
                                      queue=queue,)
        
        configpath = 'condor_' + muon_ID + '.sub'
        
        with open(configpath, 'w') as f:
            f.write(config)
            
        print('Condor submit script written to {}'.format(configpath))
        print('To submit:')
        print('    condor_submit {}'.format(configpath))
       
     
  


