#!/usr/bin/env python
from __future__ import print_function
import os
import glob
import numpy as np
import pandas as pd
import itertools
import subprocess
import sys

import uproot

import pyspark.sql

from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType


def run_convert(argv=None):

    if argv is None:
        argv = sys.argv[1:]

    resonance = argv[0]
    era = argv[1]
    subEra = argv[2]


    _useLocalSpark = False
    useParquet = True

    print("\n")
    print("\n")
    print("*************************************")
    print("******* Initializing Spark **********")
    print("*************************************")
    print("\n")
    print("\n")



    local_jars = ','.join([
        './laurelin-1.1.1.jar',
        './log4j-api-2.14.1.jar',
        './log4j-core-2.14.1.jar',
    ])

    #.config("spark.jars", local_jars)
    #.config("spark.jars.packages", "edu.vanderbilt.accre:laurelin-1.1.1.jar")
    #.config("spark.jars.packages", "org.apache.logging.log4j:log4j-api:2.14.1")
    #.config("spark.jars.packages", "org.apache.logging.log4j:log4j-core:2.14.1")
    
    spark = SparkSession\
        .builder\
        .appName("TnP")\
        .config("spark.jars", local_jars) \
        .config("spark.driver.extraClassPath", local_jars)\
        .config("spark.executor.extraClassPath", local_jars)\
        .config("spark.dynamicAllocation.maxExecutors", "100") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    

    #spark = pyspark.sql.SparkSession.builder \
    #                                .master("local[1]") \
    #                                .config('spark.jars.packages', 'edu.vanderbilt.accre:laurelin:1.1.1') \
    #                                .getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())


    print("\n")
    print("\n")

    ### DEFINITIONS #####

    baseDir = '/eos/cms/store/group/phys_higgs/cmshww/amassiro/HWWNano/'

    files_local = {
        'Z': {
            'Run2016_UL': {
                'Run2016B': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016B-ver2*.root',
                'Run2016C': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016C*.root',
                'Run2016D': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016D*.root',
                'Run2016E': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016E*.root',
                'Run2016F': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016F*.root',
                'Run2016G': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016G*.root',
                'Run2016H': 'Run2016_UL2016_nAODv8_Full2016v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2016H*.root',
                'DY_madgraph': 'Summer20UL16_106x_nAODv8_noHIPM_Full2016v8/MCTandP__addTnPMuon/nanoLatino_DYJetsToLL_M-50__part*.root', 
            },
        },
        'JPsi':{
        },
    }


    #####################

    print("Copying files to hdfs system")
    filein = os.path.join(baseDir, files_local.get(resonance,{}).get(era,{}).get(subEra,[]))    
    fileout = os.path.join("hdfs://analytix/user/sblancof/root/muon", resonance, era,'nanoAOD', subEra)

    print("hdfs dfs -cp root://eosuser/" + filein + " " + fileout)
    os.system("hdfs dfs -cp root://eosuser/" + filein + " " + fileout)


    #fnames = ['root://eosuser'+f for f in fnamesMap.get(resonance,{}).get(era,{}).get(subEra,[])]
    #fnames = [''+f for f in fnamesMap.get(resonance,{}).get(era,{}).get(subEra,[])]

    inDir = os.path.join('hdfs://analytix/user/sblancof/root/muon', resonance, era, 'nanoAOD', subEra)

    inDir = inDir.replace('/hdfs/analytix.cern.ch', 'hdfs://analytix')

    cmd = "hdfs dfs -find {} -name '*.root'".format(inDir)
    fnames = subprocess.check_output(cmd, shell=True).strip().split(b'\n')
    fnames = [fname.decode('ascii') for fname in fnames]

    outDir = os.path.join("hdfs://analytix/user/sblancof/parquet", resonance, era, subEra)
    outDir = outDir.replace('/hdfs/analytix.cern.ch', 'hdfs://analytix')

    outname = os.path.join(outDir,'tnp.parquet')
    outname_tmp = os.path.join(outDir,'tnp_tmp.parquet')

    print("\n")
    print("ROOT files path")
    print(inDir)
    print("\n")
    print("Parquet files path")
    print(outDir)
    print(outname)
    print("\n")

    treename = 'Events/'
    
    # process 1000 files at a time
    # this is about the limit that can be handled when writing
    batchsize = 1000
    new = True

    ### Variables used ###########

    variables_val = ['Muon_dxy', 'Muon_dxyErr', 'Muon_dxybs', 'Muon_dz', 'Muon_dzErr', 'Muon_eta', 'Muon_ip3d', 'Muon_jetPtRelv2', 'Muon_jetRelIso', 'Muon_mass', 'Muon_miniPFRelIso_all', 'Muon_miniPFRelIso_chg', 'Muon_pfRelIso03_all', 'Muon_pfRelIso03_chg', 'Muon_pfRelIso04_all', 'Muon_phi', 'Muon_pt', 'Muon_ptErr', 'Muon_segmentComp', 'Muon_sip3d', 'Muon_softMva', 'Muon_tkRelIso', 'Muon_tunepRelPt', 'Muon_mvaLowPt', 'Muon_mvaTTH', 'Muon_charge', 'Muon_jetIdx', 'Muon_nStations', 'Muon_nTrackerLayers', 'Muon_pdgId', 'Muon_tightCharge', 'Muon_fsrPhotonIdx', 'Muon_highPtId', 'Muon_highPurity', 'Muon_inTimeMuon', 'Muon_isGlobal', 'Muon_isPFcand', 'Muon_isTracker', 'Muon_jetNDauCharged', 'Muon_looseId', 'Muon_mediumId', 'Muon_mediumPromptId', 'Muon_miniIsoId', 'Muon_multiIsoId', 'Muon_mvaId', 'Muon_mvaLowPtId', 'Muon_pfIsoId', 'Muon_puppiIsoId', 'Muon_softId', 'Muon_softMvaId', 'Muon_tightId', 'Muon_tkIsoId', 'Muon_triggerIdLoose']

    if subEra == 'DY_madgraph':
        variables_tnp = ['run', 'luminosityBlock', 'event', 'PV_npvs', 'Pileup_nTrueInt', 'Pileup_nPU', 'puWeight', 'CaloMET_phi', 'CaloMET_pt', 'CaloMET_sumEt', 'nIsoTrack', 'MET_phi', 'MET_pt', 'MET_significance', 'MET_sumEt', 'MET_sumPtUnclustered', 'nMuon', 'PuppiMET_phi', 'PuppiMET_phiJERDown', 'PuppiMET_phiJERUp', 'PuppiMET_phiJESDown', 'PuppiMET_phiJESUp', 'PuppiMET_phiUnclusteredDown', 'PuppiMET_phiUnclusteredUp', 'PuppiMET_pt', 'PuppiMET_ptJERDown', 'PuppiMET_ptJERUp', 'PuppiMET_ptJESDown', 'PuppiMET_ptJESUp', 'PuppiMET_ptUnclusteredDown', 'PuppiMET_ptUnclusteredUp', 'PuppiMET_sumEt', 'RawMET_phi', 'RawMET_pt', 'RawMET_sumEt', 'RawPuppiMET_phi', 'RawPuppiMET_pt', 'RawPuppiMET_sumEt', 'fixedGridRhoFastjetAll', 'fixedGridRhoFastjetCentral', 'fixedGridRhoFastjetCentralCalo', 'fixedGridRhoFastjetCentralChargedPileUp', 'fixedGridRhoFastjetCentralNeutral', 'nSoftActivityJet', 'TkMET_phi', 'TkMET_pt', 'TkMET_sumEt', 'nTrigObj', 'run_period', 'Tag_dxy', 'Probe_dxy', 'Tag_dxyErr', 'Probe_dxyErr', 'Tag_dxybs', 'Probe_dxybs', 'Tag_dz', 'Probe_dz', 'Tag_dzErr', 'Probe_dzErr', 'Tag_eta', 'Probe_eta', 'Tag_ip3d', 'Probe_ip3d', 'Tag_jetPtRelv2', 'Probe_jetPtRelv2', 'Tag_jetRelIso', 'Probe_jetRelIso', 'Tag_mass', 'Probe_mass', 'Tag_miniPFRelIso_all', 'Probe_miniPFRelIso_all', 'Tag_miniPFRelIso_chg', 'Probe_miniPFRelIso_chg', 'Tag_pfRelIso03_all', 'Probe_pfRelIso03_all', 'Tag_pfRelIso03_chg', 'Probe_pfRelIso03_chg', 'Tag_pfRelIso04_all', 'Probe_pfRelIso04_all', 'Tag_phi', 'Probe_phi', 'Tag_pt', 'Probe_pt', 'Tag_ptErr', 'Probe_ptErr', 'Tag_segmentComp', 'Probe_segmentComp', 'Tag_sip3d', 'Probe_sip3d', 'Tag_softMva', 'Probe_softMva', 'Tag_tkRelIso', 'Probe_tkRelIso', 'Tag_tunepRelPt', 'Probe_tunepRelPt', 'Tag_mvaLowPt', 'Probe_mvaLowPt', 'Tag_mvaTTH', 'Probe_mvaTTH', 'Tag_charge', 'Probe_charge', 'Tag_jetIdx', 'Probe_jetIdx', 'Tag_nStations', 'Probe_nStations', 'Tag_nTrackerLayers', 'Probe_nTrackerLayers', 'Tag_pdgId', 'Probe_pdgId', 'Tag_tightCharge', 'Probe_tightCharge', 'Tag_fsrPhotonIdx', 'Probe_fsrPhotonIdx', 'Tag_highPtId', 'Probe_highPtId', 'Tag_highPurity', 'Probe_highPurity', 'Tag_inTimeMuon', 'Probe_inTimeMuon', 'Tag_isGlobal', 'Probe_isGlobal', 'Tag_isPFcand', 'Probe_isPFcand', 'Tag_isTracker', 'Probe_isTracker', 'Tag_jetNDauCharged', 'Probe_jetNDauCharged', 'Tag_looseId', 'Probe_looseId', 'Tag_mediumId', 'Probe_mediumId', 'Tag_mediumPromptId', 'Probe_mediumPromptId', 'Tag_miniIsoId', 'Probe_miniIsoId', 'Tag_multiIsoId', 'Probe_multiIsoId', 'Tag_mvaId', 'Probe_mvaId', 'Tag_mvaLowPtId', 'Probe_mvaLowPtId', 'Tag_pfIsoId', 'Probe_pfIsoId', 'Tag_puppiIsoId', 'Probe_puppiIsoId', 'Tag_softId', 'Probe_softId', 'Tag_softMvaId', 'Probe_softMvaId', 'Tag_tightId', 'Probe_tightId', 'Tag_tkIsoId', 'Probe_tkIsoId', 'Tag_triggerIdLoose', 'Probe_triggerIdLoose', 'Tag_cleanmask', 'Probe_cleanmask', 'Tag_isGenMatched', 'Tag_jetBTagDeepB', 'Tag_jetBTagDeepFlavB', 'Tag_jetBTagCSVV2', 'Tag_conept', 'Probe_isGenMatched', 'Probe_jetBTagDeepB', 'Probe_jetBTagDeepFlavB', 'Probe_jetBTagCSVV2', 'Probe_conept', 'TnP_mass', 'TnP_ht', 'TnP_met', 'TnP_trigger', 'TnP_npairs']
    
    else:
        variables_tnp = ['run', 'luminosityBlock', 'event', 'PV_npvs', 'CaloMET_phi', 'CaloMET_pt', 'CaloMET_sumEt', 'nIsoTrack', 'MET_phi', 'MET_pt', 'MET_significance', 'MET_sumEt', 'MET_sumPtUnclustered', 'nMuon', 'PuppiMET_phi', 'PuppiMET_phiJERDown', 'PuppiMET_phiJERUp', 'PuppiMET_phiJESDown', 'PuppiMET_phiJESUp', 'PuppiMET_phiUnclusteredDown', 'PuppiMET_phiUnclusteredUp', 'PuppiMET_pt', 'PuppiMET_ptJERDown', 'PuppiMET_ptJERUp', 'PuppiMET_ptJESDown', 'PuppiMET_ptJESUp', 'PuppiMET_ptUnclusteredDown', 'PuppiMET_ptUnclusteredUp', 'PuppiMET_sumEt', 'RawMET_phi', 'RawMET_pt', 'RawMET_sumEt', 'RawPuppiMET_phi', 'RawPuppiMET_pt', 'RawPuppiMET_sumEt', 'fixedGridRhoFastjetAll', 'fixedGridRhoFastjetCentral', 'fixedGridRhoFastjetCentralCalo', 'fixedGridRhoFastjetCentralChargedPileUp', 'fixedGridRhoFastjetCentralNeutral', 'nSoftActivityJet', 'TkMET_phi', 'TkMET_pt', 'TkMET_sumEt', 'nTrigObj', 'run_period', 'Tag_dxy', 'Probe_dxy', 'Tag_dxyErr', 'Probe_dxyErr', 'Tag_dxybs', 'Probe_dxybs', 'Tag_dz', 'Probe_dz', 'Tag_dzErr', 'Probe_dzErr', 'Tag_eta', 'Probe_eta', 'Tag_ip3d', 'Probe_ip3d', 'Tag_jetPtRelv2', 'Probe_jetPtRelv2', 'Tag_jetRelIso', 'Probe_jetRelIso', 'Tag_mass', 'Probe_mass', 'Tag_miniPFRelIso_all', 'Probe_miniPFRelIso_all', 'Tag_miniPFRelIso_chg', 'Probe_miniPFRelIso_chg', 'Tag_pfRelIso03_all', 'Probe_pfRelIso03_all', 'Tag_pfRelIso03_chg', 'Probe_pfRelIso03_chg', 'Tag_pfRelIso04_all', 'Probe_pfRelIso04_all', 'Tag_phi', 'Probe_phi', 'Tag_pt', 'Probe_pt', 'Tag_ptErr', 'Probe_ptErr', 'Tag_segmentComp', 'Probe_segmentComp', 'Tag_sip3d', 'Probe_sip3d', 'Tag_softMva', 'Probe_softMva', 'Tag_tkRelIso', 'Probe_tkRelIso', 'Tag_tunepRelPt', 'Probe_tunepRelPt', 'Tag_mvaLowPt', 'Probe_mvaLowPt', 'Tag_mvaTTH', 'Probe_mvaTTH', 'Tag_charge', 'Probe_charge', 'Tag_jetIdx', 'Probe_jetIdx', 'Tag_nStations', 'Probe_nStations', 'Tag_nTrackerLayers', 'Probe_nTrackerLayers', 'Tag_pdgId', 'Probe_pdgId', 'Tag_tightCharge', 'Probe_tightCharge', 'Tag_fsrPhotonIdx', 'Probe_fsrPhotonIdx', 'Tag_highPtId', 'Probe_highPtId', 'Tag_highPurity', 'Probe_highPurity', 'Tag_inTimeMuon', 'Probe_inTimeMuon', 'Tag_isGlobal', 'Probe_isGlobal', 'Tag_isPFcand', 'Probe_isPFcand', 'Tag_isTracker', 'Probe_isTracker', 'Tag_jetNDauCharged', 'Probe_jetNDauCharged', 'Tag_looseId', 'Probe_looseId', 'Tag_mediumId', 'Probe_mediumId', 'Tag_mediumPromptId', 'Probe_mediumPromptId', 'Tag_miniIsoId', 'Probe_miniIsoId', 'Tag_multiIsoId', 'Probe_multiIsoId', 'Tag_mvaId', 'Probe_mvaId', 'Tag_mvaLowPtId', 'Probe_mvaLowPtId', 'Tag_pfIsoId', 'Probe_pfIsoId', 'Tag_puppiIsoId', 'Probe_puppiIsoId', 'Tag_softId', 'Probe_softId', 'Tag_softMvaId', 'Probe_softMvaId', 'Tag_tightId', 'Probe_tightId', 'Tag_tkIsoId', 'Probe_tkIsoId', 'Tag_triggerIdLoose', 'Probe_triggerIdLoose', 'Tag_cleanmask', 'Probe_cleanmask', 'Tag_isGenMatched', 'Tag_jetBTagDeepB', 'Tag_jetBTagDeepFlavB', 'Tag_jetBTagCSVV2', 'Tag_conept', 'Probe_isGenMatched', 'Probe_jetBTagDeepB', 'Probe_jetBTagDeepFlavB', 'Probe_jetBTagCSVV2', 'Probe_conept', 'TnP_mass', 'TnP_ht', 'TnP_met', 'TnP_trigger', 'TnP_npairs']

    print("Start moving to parquet")

    os.system("hdfs dfs -rm -r -skipTrash " + outname)


    while fnames:
        current = fnames[:batchsize]
        fnames = fnames[batchsize:]
        
        rootfiles = spark.read.format("root").option('tree', treename).load(current)
        # merge rootfiles. chosen to make files of 8-32 MB (input) become at most 1 GB (parquet recommendation)
        # https://parquet.apache.org/documentation/latest/
        # .coalesce(int(len(current)/32)) \
        # but it is too slow for now, maybe try again later

        variables = variables_val + variables_tnp
        rootfiles = rootfiles.select(variables)
        
        if new:
            rootfiles.write.parquet(outname)
            new = False
        else:
            rootfiles.write.mode('append').parquet(outname)

    print("\n")
    
    print("Removing root files from Hadoop")

    os.system("hdfs dfs -rm -skipTrash " + fileout + "/*")
    
    print("\n")

    print("Final parquet file written at: ", outname)

    spark.stop()

if __name__ == "__main__":
    run_convert()
