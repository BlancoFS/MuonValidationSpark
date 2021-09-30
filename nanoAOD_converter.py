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
            'Run2017_UL': {
                'Run2017B': 'Run2017_UL2017_nAODv8_Full2017v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2017B-UL2017-v1__part*.root',
                'Run2017C': 'Run2017_UL2017_nAODv8_Full2017v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2017C-UL2017-v1__part*.root',
                'Run2017D': 'Run2017_UL2017_nAODv8_Full2017v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2017D-UL2017-v1__part*.root',
                'Run2017E': 'Run2017_UL2017_nAODv8_Full2017v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2017E-UL2017-v2__part*.root',
                'Run2017F': 'Run2017_UL2017_nAODv8_Full2017v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2017F-UL2017-v2__part*.root',
                'DY_madgraph': 'Summer20UL17_106x_nAODv8_Full2017v8/MCTandP__addTnPMuon/nanoLatino_DYJetsToLL_M-50*root',
            },
            'Run2018_UL': {
                'Run2018A': 'Run2018_UL2018_nAODv8_Full2018v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2018A-UL2018-v1__part*.root',
                'Run2018B': 'Run2018_UL2018_nAODv8_Full2018v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2018B-UL2018-v1__part*.root',
                'Run2018C': 'Run2018_UL2018_nAODv8_Full2018v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2018C-UL2018-v1__part*.root',
                'Run2018D': 'Run2018_UL2018_nAODv8_Full2018v8/DataTandP__addTnPMuon/nanoLatino_SingleMuon_Run2018D-UL2018-v1__part*.root',
                'DY_madgraph': 'Summer20UL18_106x_nAODv8_Full2018v8/MCTandP__addTnPMuon/nanoLatino_DYJetsToLL_M-50*.root',
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

    variables_HLT = ['HLTriggerFirstPath', 'HLT_AK8PFJet360_TrimMass30', 'HLT_AK8PFHT700_TrimR0p1PT0p03Mass50', 'HLT_AK8PFHT650_TrimR0p1PT0p03Mass50', 'HLT_AK8PFHT600_TrimR0p1PT0p03Mass50_BTagCSV_p20', 'HLT_CaloJet500_NoJetID', 'HLT_Dimuon13_PsiPrime', 'HLT_Dimuon13_Upsilon', 'HLT_Dimuon20_Jpsi', 'HLT_DoubleEle24_22_eta2p1_WPLoose_Gsf', 'HLT_DoubleEle33_CaloIdL', 'HLT_DoubleEle33_CaloIdL_MW', 'HLT_DoubleEle33_CaloIdL_GsfTrkIdVL_MW', 'HLT_DoubleEle33_CaloIdL_GsfTrkIdVL', 'HLT_DoubleEle37_Ele27_CaloIdL_GsfTrkIdVL', 'HLT_DoubleMediumIsoPFTau32_Trk1_eta2p1_Reg', 'HLT_DoubleMediumIsoPFTau35_Trk1_eta2p1_Reg', 'HLT_DoubleMediumIsoPFTau40_Trk1_eta2p1_Reg', 'HLT_DoubleMu33NoFiltersNoVtx', 'HLT_DoubleMu38NoFiltersNoVtx', 'HLT_DoubleMu23NoFiltersNoVtxDisplaced', 'HLT_DoubleMu28NoFiltersNoVtxDisplaced', 'HLT_DoubleMu4_3_Bs', 'HLT_DoubleMu4_3_Jpsi_Displaced', 'HLT_DoubleMu4_JpsiTrk_Displaced', 'HLT_DoubleMu4_LowMassNonResonantTrk_Displaced', 'HLT_DoubleMu3_Trk_Tau3mu', 'HLT_DoubleMu4_PsiPrimeTrk_Displaced', 'HLT_Mu7p5_L2Mu2_Jpsi', 'HLT_Mu7p5_L2Mu2_Upsilon', 'HLT_Mu7p5_Track2_Jpsi', 'HLT_Mu7p5_Track3p5_Jpsi', 'HLT_Mu7p5_Track7_Jpsi', 'HLT_Mu7p5_Track2_Upsilon', 'HLT_Mu7p5_Track3p5_Upsilon', 'HLT_Mu7p5_Track7_Upsilon', 'HLT_Dimuon0er16_Jpsi_NoOS_NoVertexing', 'HLT_Dimuon0er16_Jpsi_NoVertexing', 'HLT_Dimuon6_Jpsi_NoVertexing', 'HLT_DoublePhoton60', 'HLT_DoublePhoton85', 'HLT_Ele22_eta2p1_WPLoose_Gsf', 'HLT_Ele22_eta2p1_WPLoose_Gsf_LooseIsoPFTau20_SingleL1', 'HLT_Ele23_WPLoose_Gsf', 'HLT_Ele23_WPLoose_Gsf_WHbbBoost', 'HLT_Ele24_eta2p1_WPLoose_Gsf', 'HLT_Ele24_eta2p1_WPLoose_Gsf_LooseIsoPFTau20', 'HLT_Ele24_eta2p1_WPLoose_Gsf_LooseIsoPFTau20_SingleL1', 'HLT_Ele25_WPTight_Gsf', 'HLT_Ele25_eta2p1_WPLoose_Gsf', 'HLT_Ele25_eta2p1_WPTight_Gsf', 'HLT_Ele27_WPLoose_Gsf', 'HLT_Ele27_WPLoose_Gsf_WHbbBoost', 'HLT_Ele27_WPTight_Gsf', 'HLT_Ele27_eta2p1_WPLoose_Gsf', 'HLT_Ele27_eta2p1_WPLoose_Gsf_LooseIsoPFTau20_SingleL1', 'HLT_Ele27_eta2p1_WPLoose_Gsf_DoubleMediumIsoPFTau32_Trk1_eta2p1_Reg', 'HLT_Ele27_eta2p1_WPLoose_Gsf_DoubleMediumIsoPFTau35_Trk1_eta2p1_Reg', 'HLT_Ele27_eta2p1_WPLoose_Gsf_DoubleMediumIsoPFTau40_Trk1_eta2p1_Reg', 'HLT_Ele27_eta2p1_WPTight_Gsf', 'HLT_Ele32_eta2p1_WPLoose_Gsf_LooseIsoPFTau20_SingleL1', 'HLT_Ele32_eta2p1_WPTight_Gsf', 'HLT_Ele35_WPLoose_Gsf', 'HLT_Ele35_CaloIdVT_GsfTrkIdT_PFJet150_PFJet50', 'HLT_Ele45_WPLoose_Gsf', 'HLT_Ele45_CaloIdVT_GsfTrkIdT_PFJet200_PFJet50', 'HLT_Ele105_CaloIdVT_GsfTrkIdT', 'HLT_Ele30WP60_SC4_Mass55', 'HLT_Ele30WP60_Ele8_Mass55', 'HLT_HT200', 'HLT_HT275', 'HLT_HT325', 'HLT_HT425', 'HLT_HT575', 'HLT_HT410to430', 'HLT_HT430to450', 'HLT_HT450to470', 'HLT_HT470to500', 'HLT_HT500to550', 'HLT_HT550to650', 'HLT_HT650', 'HLT_Mu16_eta2p1_MET30', 'HLT_IsoMu16_eta2p1_MET30', 'HLT_IsoMu16_eta2p1_MET30_LooseIsoPFTau50_Trk30_eta2p1', 'HLT_IsoMu17_eta2p1_LooseIsoPFTau20', 'HLT_IsoMu17_eta2p1_LooseIsoPFTau20_SingleL1', 'HLT_DoubleIsoMu17_eta2p1', 'HLT_DoubleIsoMu17_eta2p1_noDzCut', 'HLT_IsoMu18', 'HLT_IsoMu19_eta2p1_LooseIsoPFTau20', 'HLT_IsoMu19_eta2p1_LooseIsoPFTau20_SingleL1', 'HLT_IsoMu19_eta2p1_MediumIsoPFTau32_Trk1_eta2p1_Reg', 'HLT_IsoMu20', 'HLT_IsoMu21_eta2p1_LooseIsoPFTau20_SingleL1', 'HLT_IsoMu21_eta2p1_MediumIsoPFTau32_Trk1_eta2p1_Reg', 'HLT_IsoMu22', 'HLT_IsoMu22_eta2p1', 'HLT_IsoMu24', 'HLT_IsoMu27', 'HLT_IsoTkMu18', 'HLT_IsoTkMu20', 'HLT_IsoTkMu22', 'HLT_IsoTkMu22_eta2p1', 'HLT_IsoTkMu24', 'HLT_IsoTkMu27', 'HLT_JetE30_NoBPTX3BX', 'HLT_JetE30_NoBPTX', 'HLT_JetE50_NoBPTX3BX', 'HLT_JetE70_NoBPTX3BX', 'HLT_L1SingleMu18', 'HLT_L2Mu10', 'HLT_L1SingleMuOpen', 'HLT_L1SingleMuOpen_DT', 'HLT_L2DoubleMu23_NoVertex', 'HLT_L2DoubleMu28_NoVertex_2Cha_Angle2p5_Mass10', 'HLT_L2DoubleMu38_NoVertex_2Cha_Angle2p5_Mass10', 'HLT_L2Mu10_NoVertex_NoBPTX3BX', 'HLT_L2Mu10_NoVertex_NoBPTX', 'HLT_L2Mu35_NoVertex_3Sta_NoBPTX3BX', 'HLT_L2Mu40_NoVertex_3Sta_NoBPTX3BX', 'HLT_LooseIsoPFTau50_Trk30_eta2p1', 'HLT_LooseIsoPFTau50_Trk30_eta2p1_MET80', 'HLT_LooseIsoPFTau50_Trk30_eta2p1_MET90', 'HLT_LooseIsoPFTau50_Trk30_eta2p1_MET110', 'HLT_LooseIsoPFTau50_Trk30_eta2p1_MET120', 'HLT_PFTau120_eta2p1', 'HLT_VLooseIsoPFTau120_Trk50_eta2p1', 'HLT_VLooseIsoPFTau140_Trk50_eta2p1', 'HLT_Mu17_Mu8', 'HLT_Mu17_Mu8_DZ', 'HLT_Mu17_Mu8_SameSign', 'HLT_Mu17_Mu8_SameSign_DZ', 'HLT_Mu20_Mu10', 'HLT_Mu20_Mu10_DZ', 'HLT_Mu20_Mu10_SameSign', 'HLT_Mu20_Mu10_SameSign_DZ', 'HLT_Mu17_TkMu8_DZ', 'HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL', 'HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ', 'HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL', 'HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ', 'HLT_Mu25_TkMu0_dEta18_Onia', 'HLT_Mu27_TkMu8', 'HLT_Mu30_TkMu11', 'HLT_Mu30_eta2p1_PFJet150_PFJet50', 'HLT_Mu40_TkMu11', 'HLT_Mu40_eta2p1_PFJet200_PFJet50', 'HLT_Mu20', 'HLT_TkMu20', 'HLT_Mu24_eta2p1', 'HLT_TkMu24_eta2p1', 'HLT_Mu27', 'HLT_TkMu27', 'HLT_Mu45_eta2p1', 'HLT_Mu50', 'HLT_TkMu50', 'HLT_Mu38NoFiltersNoVtx_Photon38_CaloIdL', 'HLT_Mu42NoFiltersNoVtx_Photon42_CaloIdL', 'HLT_Mu28NoFiltersNoVtxDisplaced_Photon28_CaloIdL', 'HLT_Mu33NoFiltersNoVtxDisplaced_Photon33_CaloIdL', 'HLT_Mu23NoFiltersNoVtx_Photon23_CaloIdL', 'HLT_DoubleMu18NoFiltersNoVtx', 'HLT_Mu33NoFiltersNoVtxDisplaced_DisplacedJet50_Tight', 'HLT_Mu33NoFiltersNoVtxDisplaced_DisplacedJet50_Loose', 'HLT_Mu28NoFiltersNoVtx_DisplacedJet40_Loose', 'HLT_Mu38NoFiltersNoVtxDisplaced_DisplacedJet60_Tight', 'HLT_Mu38NoFiltersNoVtxDisplaced_DisplacedJet60_Loose', 'HLT_Mu38NoFiltersNoVtx_DisplacedJet60_Loose', 'HLT_Mu28NoFiltersNoVtx_CentralCaloJet40', 'HLT_PFHT300_PFMET100', 'HLT_PFHT300_PFMET110', 'HLT_PFHT550_4JetPt50', 'HLT_PFHT650_4JetPt50', 'HLT_PFHT750_4JetPt50', 'HLT_PFJet15_NoCaloMatched', 'HLT_PFJet25_NoCaloMatched', 'HLT_DiPFJet15_NoCaloMatched', 'HLT_DiPFJet25_NoCaloMatched', 'HLT_DiPFJet15_FBEta3_NoCaloMatched', 'HLT_DiPFJet25_FBEta3_NoCaloMatched', 'HLT_DiPFJetAve15_HFJEC', 'HLT_DiPFJetAve25_HFJEC', 'HLT_DiPFJetAve35_HFJEC', 'HLT_AK8PFJet40', 'HLT_AK8PFJet60', 'HLT_AK8PFJet80', 'HLT_AK8PFJet140', 'HLT_AK8PFJet200', 'HLT_AK8PFJet260', 'HLT_AK8PFJet320', 'HLT_AK8PFJet400', 'HLT_AK8PFJet450', 'HLT_AK8PFJet500', 'HLT_PFJet40', 'HLT_PFJet60', 'HLT_PFJet80', 'HLT_PFJet140', 'HLT_PFJet200', 'HLT_PFJet260', 'HLT_PFJet320', 'HLT_PFJet400', 'HLT_PFJet450', 'HLT_PFJet500', 'HLT_DiPFJetAve40', 'HLT_DiPFJetAve60', 'HLT_DiPFJetAve80', 'HLT_DiPFJetAve140', 'HLT_DiPFJetAve200', 'HLT_DiPFJetAve260', 'HLT_DiPFJetAve320', 'HLT_DiPFJetAve400', 'HLT_DiPFJetAve500', 'HLT_DiPFJetAve60_HFJEC', 'HLT_DiPFJetAve80_HFJEC', 'HLT_DiPFJetAve100_HFJEC', 'HLT_DiPFJetAve160_HFJEC', 'HLT_DiPFJetAve220_HFJEC', 'HLT_DiPFJetAve300_HFJEC', 'HLT_DiPFJet40_DEta3p5_MJJ600_PFMETNoMu140', 'HLT_DiPFJet40_DEta3p5_MJJ600_PFMETNoMu80', 'HLT_DiCentralPFJet55_PFMET110', 'HLT_DiCentralPFJet170', 'HLT_SingleCentralPFJet170_CFMax0p1', 'HLT_DiCentralPFJet170_CFMax0p1', 'HLT_DiCentralPFJet220_CFMax0p3', 'HLT_DiCentralPFJet330_CFMax0p5', 'HLT_DiCentralPFJet430', 'HLT_PFHT125', 'HLT_PFHT200', 'HLT_PFHT250', 'HLT_PFHT300', 'HLT_PFHT350', 'HLT_PFHT400', 'HLT_PFHT475', 'HLT_PFHT600', 'HLT_PFHT650', 'HLT_PFHT800', 'HLT_PFHT900', 'HLT_PFHT200_PFAlphaT0p51', 'HLT_PFHT200_DiPFJetAve90_PFAlphaT0p57', 'HLT_PFHT200_DiPFJetAve90_PFAlphaT0p63', 'HLT_PFHT250_DiPFJetAve90_PFAlphaT0p55', 'HLT_PFHT250_DiPFJetAve90_PFAlphaT0p58', 'HLT_PFHT300_DiPFJetAve90_PFAlphaT0p53', 'HLT_PFHT300_DiPFJetAve90_PFAlphaT0p54', 'HLT_PFHT350_DiPFJetAve90_PFAlphaT0p52', 'HLT_PFHT350_DiPFJetAve90_PFAlphaT0p53', 'HLT_PFHT400_DiPFJetAve90_PFAlphaT0p51', 'HLT_PFHT400_DiPFJetAve90_PFAlphaT0p52', 'HLT_MET60_IsoTrk35_Loose', 'HLT_MET75_IsoTrk50', 'HLT_MET90_IsoTrk50', 'HLT_PFMET120_BTagCSV_p067', 'HLT_PFMET120_Mu5', 'HLT_PFMET170_NoiseCleaned', 'HLT_PFMET170_HBHECleaned', 'HLT_PFMET170_JetIdCleaned', 'HLT_PFMET170_NotCleaned', 'HLT_PFMET170_BeamHaloCleaned', 'HLT_PFMET90_PFMHT90_IDTight', 'HLT_PFMET100_PFMHT100_IDTight', 'HLT_PFMET110_PFMHT110_IDTight', 'HLT_PFMET120_PFMHT120_IDTight', 'HLT_CaloMHTNoPU90_PFMET90_PFMHT90_IDTight_BTagCSV_p067', 'HLT_CaloMHTNoPU90_PFMET90_PFMHT90_IDTight', 'HLT_QuadPFJet_BTagCSV_p016_p11_VBF_Mqq200', 'HLT_QuadPFJet_BTagCSV_p016_VBF_Mqq460', 'HLT_QuadPFJet_BTagCSV_p016_p11_VBF_Mqq240', 'HLT_QuadPFJet_BTagCSV_p016_VBF_Mqq500', 'HLT_QuadPFJet_VBF', 'HLT_L1_TripleJet_VBF', 'HLT_QuadJet45_TripleBTagCSV_p087', 'HLT_QuadJet45_DoubleBTagCSV_p087', 'HLT_DoubleJet90_Double30_TripleBTagCSV_p087', 'HLT_DoubleJet90_Double30_DoubleBTagCSV_p087', 'HLT_DoubleJetsC100_DoubleBTagCSV_p026_DoublePFJetsC160', 'HLT_DoubleJetsC100_DoubleBTagCSV_p014_DoublePFJetsC100MaxDeta1p6', 'HLT_DoubleJetsC112_DoubleBTagCSV_p026_DoublePFJetsC172', 'HLT_DoubleJetsC112_DoubleBTagCSV_p014_DoublePFJetsC112MaxDeta1p6', 'HLT_DoubleJetsC100_SingleBTagCSV_p026', 'HLT_DoubleJetsC100_SingleBTagCSV_p014', 'HLT_DoubleJetsC100_SingleBTagCSV_p026_SinglePFJetC350', 'HLT_DoubleJetsC100_SingleBTagCSV_p014_SinglePFJetC350', 'HLT_Photon135_PFMET100', 'HLT_Photon22_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon22_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Photon250_NoHE', 'HLT_Photon300_NoHE', 'HLT_Photon26_R9Id85_OR_CaloId24b40e_Iso50T80L_Photon16_AND_HE10_R9Id65_Eta2_Mass60', 'HLT_Photon36_R9Id85_OR_CaloId24b40e_Iso50T80L_Photon22_AND_HE10_R9Id65_Eta2_Mass15', 'HLT_Photon36_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon36_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Photon50_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon50_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Photon75_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon75_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Photon90_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon90_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Photon120_R9Id90_HE10_Iso40_EBOnly_PFMET40', 'HLT_Photon120_R9Id90_HE10_Iso40_EBOnly_VBF', 'HLT_Mu8_TrkIsoVVL', 'HLT_Mu17_TrkIsoVVL', 'HLT_Ele8_CaloIdL_TrackIdL_IsoVL_PFJet30', 'HLT_Ele12_CaloIdL_TrackIdL_IsoVL_PFJet30', 'HLT_Ele17_CaloIdL_TrackIdL_IsoVL_PFJet30', 'HLT_Ele23_CaloIdL_TrackIdL_IsoVL_PFJet30', 'HLT_BTagMu_DiJet20_Mu5', 'HLT_BTagMu_DiJet40_Mu5', 'HLT_BTagMu_DiJet70_Mu5', 'HLT_BTagMu_DiJet110_Mu5', 'HLT_BTagMu_Jet300_Mu5', 'HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ', 'HLT_Ele17_Ele12_CaloIdL_TrackIdL_IsoVL_DZ', 'HLT_Ele16_Ele12_Ele8_CaloIdL_TrackIdL', 'HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL', 'HLT_Mu8_TrkIsoVVL_Ele17_CaloIdL_TrackIdL_IsoVL', 'HLT_Mu23_TrkIsoVVL_Ele8_CaloIdL_TrackIdL_IsoVL', 'HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL', 'HLT_Mu17_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL', 'HLT_Mu30_Ele30_CaloIdL_GsfTrkIdVL', 'HLT_Mu37_Ele27_CaloIdL_GsfTrkIdVL', 'HLT_Mu27_Ele37_CaloIdL_GsfTrkIdVL', 'HLT_Mu8_DiEle12_CaloIdL_TrackIdL', 'HLT_Mu12_Photon25_CaloIdL', 'HLT_Mu12_Photon25_CaloIdL_L1ISO', 'HLT_Mu12_Photon25_CaloIdL_L1OR', 'HLT_Mu17_Photon22_CaloIdL_L1ISO', 'HLT_Mu17_Photon30_CaloIdL_L1ISO', 'HLT_Mu17_Photon35_CaloIdL_L1ISO', 'HLT_DiMu9_Ele9_CaloIdL_TrackIdL', 'HLT_TripleMu_5_3_3', 'HLT_TripleMu_12_10_5', 'HLT_Mu3er_PFHT140_PFMET125', 'HLT_Mu6_PFHT200_PFMET80_BTagCSV_p067', 'HLT_Mu6_PFHT200_PFMET100', 'HLT_Mu14er_PFMET100', 'HLT_Ele17_Ele12_CaloIdL_TrackIdL_IsoVL', 'HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL', 'HLT_Ele12_CaloIdL_TrackIdL_IsoVL', 'HLT_Ele17_CaloIdL_GsfTrkIdVL', 'HLT_Ele17_CaloIdL_TrackIdL_IsoVL', 'HLT_Ele23_CaloIdL_TrackIdL_IsoVL', 'HLT_AK8DiPFJet280_200_TrimMass30', 'HLT_AK8DiPFJet250_200_TrimMass30', 'HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV_p20', 'HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV_p20', 'HLT_PFHT650_WideJetMJJ900DEtaJJ1p5', 'HLT_PFHT650_WideJetMJJ950DEtaJJ1p5', 'HLT_Photon22', 'HLT_Photon30', 'HLT_Photon36', 'HLT_Photon50', 'HLT_Photon75', 'HLT_Photon90', 'HLT_Photon120', 'HLT_Photon175', 'HLT_Photon165_HE10', 'HLT_Photon22_R9Id90_HE10_IsoM', 'HLT_Photon30_R9Id90_HE10_IsoM', 'HLT_Photon36_R9Id90_HE10_IsoM', 'HLT_Photon50_R9Id90_HE10_IsoM', 'HLT_Photon75_R9Id90_HE10_IsoM', 'HLT_Photon90_R9Id90_HE10_IsoM', 'HLT_Photon120_R9Id90_HE10_IsoM', 'HLT_Photon165_R9Id90_HE10_IsoM', 'HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90', 'HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelSeedMatch_Mass70', 'HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55', 'HLT_Diphoton30_18_Solid_R9Id_AND_IsoCaloId_AND_HE_R9Id_Mass55', 'HLT_Diphoton30EB_18EB_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55', 'HLT_Dimuon0_Jpsi_Muon', 'HLT_Dimuon0_Upsilon_Muon', 'HLT_QuadMuon0_Dimuon0_Jpsi', 'HLT_QuadMuon0_Dimuon0_Upsilon', 'HLT_Rsq0p25', 'HLT_Rsq0p30', 'HLT_RsqMR240_Rsq0p09_MR200', 'HLT_RsqMR240_Rsq0p09_MR200_4jet', 'HLT_RsqMR270_Rsq0p09_MR200', 'HLT_RsqMR270_Rsq0p09_MR200_4jet', 'HLT_Rsq0p02_MR300_TriPFJet80_60_40_BTagCSV_p063_p20_Mbb60_200', 'HLT_Rsq0p02_MR300_TriPFJet80_60_40_DoubleBTagCSV_p063_Mbb60_200', 'HLT_HT200_DisplacedDijet40_DisplacedTrack', 'HLT_HT250_DisplacedDijet40_DisplacedTrack', 'HLT_HT350_DisplacedDijet40_DisplacedTrack', 'HLT_HT350_DisplacedDijet80_DisplacedTrack', 'HLT_HT350_DisplacedDijet80_Tight_DisplacedTrack', 'HLT_HT350_DisplacedDijet40_Inclusive', 'HLT_HT400_DisplacedDijet40_Inclusive', 'HLT_HT500_DisplacedDijet40_Inclusive', 'HLT_HT550_DisplacedDijet40_Inclusive', 'HLT_HT650_DisplacedDijet80_Inclusive', 'HLT_HT750_DisplacedDijet80_Inclusive', 'HLT_VBF_DisplacedJet40_DisplacedTrack', 'HLT_VBF_DisplacedJet40_DisplacedTrack_2TrackIP2DSig5', 'HLT_VBF_DisplacedJet40_TightID_DisplacedTrack', 'HLT_VBF_DisplacedJet40_Hadronic', 'HLT_VBF_DisplacedJet40_Hadronic_2PromptTrack', 'HLT_VBF_DisplacedJet40_TightID_Hadronic', 'HLT_VBF_DisplacedJet40_VTightID_Hadronic', 'HLT_VBF_DisplacedJet40_VVTightID_Hadronic', 'HLT_VBF_DisplacedJet40_VTightID_DisplacedTrack', 'HLT_VBF_DisplacedJet40_VVTightID_DisplacedTrack', 'HLT_PFMETNoMu90_PFMHTNoMu90_IDTight', 'HLT_PFMETNoMu100_PFMHTNoMu100_IDTight', 'HLT_PFMETNoMu110_PFMHTNoMu110_IDTight', 'HLT_PFMETNoMu120_PFMHTNoMu120_IDTight', 'HLT_MonoCentralPFJet80_PFMETNoMu90_PFMHTNoMu90_IDTight', 'HLT_MonoCentralPFJet80_PFMETNoMu100_PFMHTNoMu100_IDTight', 'HLT_MonoCentralPFJet80_PFMETNoMu110_PFMHTNoMu110_IDTight', 'HLT_MonoCentralPFJet80_PFMETNoMu120_PFMHTNoMu120_IDTight', 'HLT_Ele27_eta2p1_WPLoose_Gsf_HT200', 'HLT_Photon90_CaloIdL_PFHT500', 'HLT_DoubleMu8_Mass8_PFHT250', 'HLT_Mu8_Ele8_CaloIdM_TrackIdM_Mass8_PFHT250', 'HLT_DoubleEle8_CaloIdM_TrackIdM_Mass8_PFHT250', 'HLT_DoubleMu8_Mass8_PFHT300', 'HLT_Mu8_Ele8_CaloIdM_TrackIdM_Mass8_PFHT300', 'HLT_DoubleEle8_CaloIdM_TrackIdM_Mass8_PFHT300', 'HLT_Mu10_CentralPFJet30_BTagCSV_p13', 'HLT_DoubleMu3_PFMET50', 'HLT_Ele10_CaloIdM_TrackIdM_CentralPFJet30_BTagCSV_p13', 'HLT_Ele15_IsoVVVL_BTagCSV_p067_PFHT400', 'HLT_Ele15_IsoVVVL_PFHT350_PFMET50', 'HLT_Ele15_IsoVVVL_PFHT600', 'HLT_Ele15_IsoVVVL_PFHT350', 'HLT_Ele15_IsoVVVL_PFHT400_PFMET50', 'HLT_Ele15_IsoVVVL_PFHT400', 'HLT_Ele50_IsoVVVL_PFHT400', 'HLT_Mu8_TrkIsoVVL_DiPFJet40_DEta3p5_MJJ750_HTT300_PFMETNoMu60', 'HLT_Mu10_TrkIsoVVL_DiPFJet40_DEta3p5_MJJ750_HTT350_PFMETNoMu60', 'HLT_Mu15_IsoVVVL_BTagCSV_p067_PFHT400', 'HLT_Mu15_IsoVVVL_PFHT350_PFMET50', 'HLT_Mu15_IsoVVVL_PFHT600', 'HLT_Mu15_IsoVVVL_PFHT350', 'HLT_Mu15_IsoVVVL_PFHT400_PFMET50', 'HLT_Mu15_IsoVVVL_PFHT400', 'HLT_Mu50_IsoVVVL_PFHT400', 'HLT_Dimuon16_Jpsi', 'HLT_Dimuon10_Jpsi_Barrel', 'HLT_Dimuon8_PsiPrime_Barrel', 'HLT_Dimuon8_Upsilon_Barrel', 'HLT_Dimuon0_Phi_Barrel', 'HLT_Mu16_TkMu0_dEta18_Onia', 'HLT_Mu16_TkMu0_dEta18_Phi', 'HLT_TrkMu15_DoubleTrkMu5NoFiltersNoVtx', 'HLT_TrkMu17_DoubleTrkMu8NoFiltersNoVtx', 'HLT_Mu8', 'HLT_Mu17', 'HLT_Mu3_PFJet40', 'HLT_Ele8_CaloIdM_TrackIdM_PFJet30', 'HLT_Ele12_CaloIdM_TrackIdM_PFJet30', 'HLT_Ele17_CaloIdM_TrackIdM_PFJet30', 'HLT_Ele23_CaloIdM_TrackIdM_PFJet30', 'HLT_Ele50_CaloIdVT_GsfTrkIdT_PFJet140', 'HLT_Ele50_CaloIdVT_GsfTrkIdT_PFJet165', 'HLT_PFHT400_SixJet30_DoubleBTagCSV_p056', 'HLT_PFHT450_SixJet40_BTagCSV_p056', 'HLT_PFHT400_SixJet30', 'HLT_PFHT450_SixJet40', 'HLT_Ele115_CaloIdVT_GsfTrkIdT', 'HLT_Mu55', 'HLT_Photon42_R9Id85_OR_CaloId24b40e_Iso50T80L_Photon25_AND_HE10_R9Id65_Eta2_Mass15', 'HLT_Photon90_CaloIdL_PFHT600', 'HLT_PixelTracks_Multiplicity60ForEndOfFill', 'HLT_PixelTracks_Multiplicity85ForEndOfFill', 'HLT_PixelTracks_Multiplicity110ForEndOfFill', 'HLT_PixelTracks_Multiplicity135ForEndOfFill', 'HLT_PixelTracks_Multiplicity160ForEndOfFill', 'HLT_FullTracks_Multiplicity80', 'HLT_FullTracks_Multiplicity100', 'HLT_FullTracks_Multiplicity130', 'HLT_FullTracks_Multiplicity150', 'HLT_ECALHT800', 'HLT_DiSC30_18_EIso_AND_HE_Mass70', 'HLT_MET200', 'HLT_Ele27_HighEta_Ele20_Mass55', 'HLT_L1FatEvents', 'HLT_Physics', 'HLT_Physics_part0', 'HLT_Physics_part1', 'HLT_Physics_part2', 'HLT_Physics_part3', 'HLT_Random', 'HLT_ZeroBias', 'HLT_ZeroBias_part0', 'HLT_ZeroBias_part1', 'HLT_ZeroBias_part2', 'HLT_ZeroBias_part3', 'HLT_AK4CaloJet30', 'HLT_AK4CaloJet40', 'HLT_AK4CaloJet50', 'HLT_AK4CaloJet80', 'HLT_AK4CaloJet100', 'HLT_AK4PFJet30', 'HLT_AK4PFJet50', 'HLT_AK4PFJet80', 'HLT_AK4PFJet100', 'HLT_HISinglePhoton10', 'HLT_HISinglePhoton15', 'HLT_HISinglePhoton20', 'HLT_HISinglePhoton40', 'HLT_HISinglePhoton60', 'HLT_EcalCalibration', 'HLT_HcalCalibration', 'HLT_GlobalRunHPDNoise', 'HLT_L1BptxMinus', 'HLT_L1BptxPlus', 'HLT_L1NotBptxOR', 'HLT_L1BeamGasMinus', 'HLT_L1BeamGasPlus', 'HLT_L1BptxXOR', 'HLT_L1MinimumBiasHF_OR', 'HLT_L1MinimumBiasHF_AND', 'HLT_HcalNZS', 'HLT_HcalPhiSym', 'HLT_ZeroBias_FirstCollisionAfterAbortGap', 'HLT_ZeroBias_FirstCollisionAfterAbortGap_TCDS', 'HLT_ZeroBias_IsolatedBunches', 'HLT_Photon500', 'HLT_Photon600', 'HLT_Mu300', 'HLT_Mu350', 'HLT_MET250', 'HLT_MET300', 'HLT_MET600', 'HLT_MET700', 'HLT_PFMET300', 'HLT_PFMET400', 'HLT_PFMET500', 'HLT_PFMET600', 'HLT_HT2000', 'HLT_HT2500', 'HLT_IsoTrackHE', 'HLT_IsoTrackHB', 'HLTriggerFinalPath', 'HLT_Ele27_WPTight_Gsf_L1JetTauSeeded', 'HLT_Ele45_WPLoose_Gsf_L1JetTauSeeded', 'HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ_L1JetTauSeeded']
        
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

        variables = variables_HLT + variables_tnp
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
