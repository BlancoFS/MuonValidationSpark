from __future__ import print_function
import os
import glob
import numpy as np
import pandas as pd
import itertools
import matplotlib.pyplot as plt

import uproot
#from uproot_methods.classes import TH1

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
import itertools
import json
from array import array
import ctypes
import ROOT
import tdrstyle
import CMS_lumi

from dataset_allowed_definitions import get_allowed_sub_eras, get_data_mc_sub_eras




def run_files(particle, probe, resonance, era, subEra, config, spark):
    
    useParquet = True
    
    # Select parquet files
    
    if useParquet:
        fnames = list(registry.parquet(
            particle, probe, resonance, era, subEra))
    else:
        fnames = registry.root(particle, probe, resonance, era, subEra)
        # Assume path in registry is already correct, no need for redirector
        # fnames = ['root://eoscms.cern.ch/'+f for f in fnames]
        fnames = [f for f in fnames]

    
    # Load parquet files (or root)
    print('Loading parquet files:', fnames)
    if isinstance(fnames, list):
        baseDF = spark.read.parquet(*fnames)
    else:
        baseDF = spark.read.parquet(fnames)
    
    # Load definitions and filter events
    
    doGen = subEra in ['DY_madgraph', 'DY_powheg', 'JPsi_pythia8']
    
    #definitions = config['definitions']
    definitions = config.definitions()

    defDF = baseDF
    for d in definitions:
        defDF = defDF.withColumn(d, F.expr(definitions[d]))

    
    #tagsDF = defDF.filter(config['selection'])
    tagsDF = defDF.filter(config.selection())
    
    if doGen:
        if 'mc_selection' in config:
            tagsDF = tagsDF.filter(config.mc_selection())
    else:
        if 'data_selection' in config:
            tagsDF = tagsDF.filter(config.data_selection())

            
        
    # Weight data and MC with the PileUp
    weightedDF = get_weighted_dataframe(tagsDF, doGen, resonance, era, subEra, shift='Nominal')
    
    
    #if doGen:
    #    print("")
    #else:
    #    new_def = {}
    #    new_def['genWeight'] = '1.0'
    #    for d in new_def:
    #        tagsDF = tagsDF.withColumn(d, F.expr(new_def[d]))
    
    
    
    #binning = config['binning']
    #variables = config['variables']
    #binVariables = config['binVariables']
    
    binning = config.binning()
    variables = config.variables()
    binVariables = config.binVariables()

    binningSet = set()

    for bvs in binVariables:
        binningSet = binningSet.union(set(bvs))

    binnedDF = weightedDF
        
    for bName in binningSet:
        binnedDF = get_binned_dataframe(
            binnedDF, bName+"Bin",
            variables[bName]['variable'],
            binning[bName])                
        
    
    yields = {}
    for binVars in binningSet:
        key = binVars
        yields[key] = binnedDF.groupBy(key+'Bin', *[key+'Bin']).agg({'weight': 'sum'})                                            

    realized = {}
    for binVars in yields:
        realized[binVars] = yields[binVars].toPandas()
        
    return realized


def compare(particle, probe, resonance, era, config, **kwargs):
    

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
        './laurelin-1.0.0.jar',
        './log4j-api-2.13.0.jar',
        './log4j-core-2.13.0.jar',
    ])
    
    spark = SparkSession\
        .builder\
        .appName("TnP")
    
    if useParquet == False:
        spark = spark\
        .config("spark.jars", local_jars)\
        .config("spark.driver.extraClassPath", local_jars)\
        .config("spark.executor.extraClassPath", local_jars)\
        .config("spark.dynamicAllocation.maxExecutors", "100")\
        .config("spark.driver.memory", "6g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.executor.cores", "2")

    if _useLocalSpark == True:
        spark = spark.master("local")

    spark = spark.getOrCreate()

    sc = spark.sparkContext
    print(sc.getConf().toDebugString())

    
    _baseDir = kwargs.pop('baseDir', '')
    _subera1 = kwargs.pop('subera1', '')
    _subera2 = kwargs.pop('subera2', '')
    _era2    = kwargs.pop('era2', '')


    if _era2 == '':
        _era2 = era


    _FullEra = False

    subEra_dic = get_allowed_sub_eras(resonance, era)
    subEra2_dic = get_allowed_sub_eras(resonance, _era2)

    if (_subera1 in subEra_dic) and (_subera2 in subEra2_dic):
        subEras = [_subera1, _subera2]
        print("\n")
        print("-------------------------------------")
        print("-------------------------------------")
        print("Procesing " + str(_subera1) + " vs " + str(_subera2) + " datasets")
        print("-------------------------------------")
        print("-------------------------------------")
        print("\n")
    elif _subera1 == '' and _subera2 == '':
        subEras = subEra_dic
        _FullEra = True
        print("\n")
        print("-------------------------------------")
        print("-------------------------------------")
        print("Procesing complete " + str(era) + " dataset")
        print("-------------------------------------")
        print("-------------------------------------")
        print("\n")
    else:
        print("\n")
        print("\n")
        print("The subEras selected are not availible")
        print("\n")
        print("\n")
        return 0
    
        

    realized = {}
    

    if _FullEra:
        
        use_Data = False
        use_MC = False
        
        Z_peak = False
        JPsi_peak = False

        for subEra in subEras:
            if (era.split('_')[0] in subEra) or (era in subEra):
                use_Data = True
            elif 'DY_madgraph' in subEra:
                use_MC = True
                Z_peak = True
            elif 'DY_powheg' in subEra:
                use_MC = True
                Z_peak = True
            elif 'JPsi_pythia8' in subEra:
                use_MC = True
                JPsi_peak = True
                
        #if subEra == 'ALL':
        #    
        #    use_Data = True
        #    use_MC   = True
        #    
        #    if bySubEraAlso:
        #        subEras = get_allowed_sub_eras(resonance, era)
        #    else:
        #        subEras = get_data_mc_sub_eras(resonance, era)
        #else:
        #    if subEra in ['DY_madgraph', 'DY_powheg', 'JPsi_pythia8']:
        #        use_MC = True
        #    else:
        #        use_Data = True
        #    
        #    subEras = []
        #    subEras.append(subEra)
        
        
        
        for subEra in subEras:
            realized[subEra] = run_files(particle, probe, resonance, era, subEra, config, spark)
            
            
            
        #binning = config['binning']
        #variables = config['variables']
        #binVariables = config['binVariables']
        
        binning = config.binning()
        variables = config.variables()
        binVariables = config.binVariables()

        ROOT.gROOT.SetBatch()
        ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 1001;")
        tdrstyle.setTDRStyle()
                                

        for binVar in binVariables:
            
            if len(binVar) == 1:
                
                # Data array
                fill_array = np.zeros(len(binning[binVar[0]])+1) 
                
                # MC array
                fill_array_mc = np.zeros(len(binning[binVar[0]])+1) 
                
                var_name = binVar[0]
                
                # Fill the arrays for each subEra
                for subEra in subEras:
                    
                    df = realized[subEra][var_name]
                    df = df.T.drop_duplicates().T
                    df = df.sort_values(by=[var_name+'Bin'])
                    df = df.reset_index()
                    
                    values = pd.Series(np.zeros(len(binning[var_name])+1))
                    values[df.index] = df['sum(weight)']
                    
                    if (subEra in ['DY_madgraph', 'DY_powheg', 'JPsi_pythia8']):
                        for i in df.index:
                            fill_array_mc[i] = fill_array_mc[i] + float(values[i])
                    else:
                        for i in df.index:
                            fill_array[i] = fill_array[i] + float(values[i])
                            
                            
                lumi = 0
                if use_Data:
                    for subEra in subEras:
                        if (era.split('_')[0] not in subEra) and (era not in subEra):
                            continue
                        else:
                            lumi = lumi + registry.luminosity(particle, probe, resonance, era, subEra)
                else:
                    lumi = -1
                    
                    
                #
                # Initialize histogram
                #
            
                bins = np.array(binning[var_name])
                bins = bins.astype(np.float32)

                print("\n")
                print("Binning for " + var_name + " is ", bins)
                print("\n")

                hist = ROOT.TH1F(var_name, var_name, len(bins)-1, bins) #ROOT histogram
                hist_mc = ROOT.TH1F(var_name+'_mc', var_name+'_mc', len(bins)-1, bins) #ROOT histogram
                
                
                # Axis titles and options
                
                hist_mc.GetYaxis().SetTitle('Events')
                hist_mc.GetYaxis().SetTitleOffset(1.33)
                hist_mc.GetXaxis().SetTitle(variables[var_name]['pretty'])
                
                
                # Fill histogram
                
                for i in range(0, len(fill_array)):
                    hist.SetBinContent(i, fill_array[i])
                    hist_mc.SetBinContent(i, fill_array_mc[i])
                    

                # Normalize histograms

                hist_mc.Scale(hist.Integral()/hist_mc.Integral())
   
                # Canvas initialization    
                
                cName = var_name        
                canvas = ROOT.TCanvas(cName, cName, 900, 800)
                #ROOT.gStyle.SetPaintTextFormat("5.3f")
                ROOT.gStyle.SetPaintTextFormat("4.1f")
                canvas.SetRightMargin(0.24)
                
                
                # Make up 
                
                hist.SetMarkerStyle(ROOT.kFullCircle)
                
                hist_mc.SetLineWidth(3)
                hist_mc.SetLineColor(ROOT.kAzure-4)
                hist_mc.SetFillColor(ROOT.kAzure-4)

                
                    
                if use_MC:
                    hist_mc.Draw("hist")
                
                if use_Data:
                    hist.Draw("e1 same")
                else:
                    hist.Draw("e1")


                
                #plotPath = os.path.join(plotDir, h)
                #canvas.SetLogy()
                #canvas.SetGrid()
                
                max1 = hist.GetMaximum()
                max2 = hist_mc.GetMaximum()
                
                if max1 > max2:
                    hist.SetMaximum(max1*0.2 + max1)
                    hist_mc.SetMaximum(max1*0.2 + max1)
                else:
                    hist.SetMaximum(max2*0.2 + max2)
                    hist_mc.SetMaximum(max2*0.2 + max2)
            
                    
                legend = ROOT.TLegend(0.9, 0.9, 0.7, 0.78)
                #legend = ROOT.TLegend(0.5, 0.70, 0.92, 0.92)
                
                if use_Data:
                    legend.AddEntry(hist, "Data")
                if use_MC:
                    legend.AddEntry(hist_mc, "Simulation", "l")
                    
                legend.SetTextFont(42)
                legend.SetBorderSize(0)
                legend.SetFillColor(0)
                legend.Draw()
                
                canvas.Modified()
                canvas.Update()
                
                # CMS title and lumi
                
                CMS_lumi.cmsText = 'CMS'
                CMS_lumi.writeExtraText = True
                CMS_lumi.extraText = 'Preliminary'
                if lumi!=-1:
                    CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
                else:
                    CMS_lumi.lumi_13TeV = ""
                CMS_lumi.CMS_lumi(canvas, 4, 11)
                

                #
                # Draw and save no ratio plots    
                #

                directory = _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/"

                if not os.path.exists(directory):
                    os.makedirs(directory)

                canvas.Draw()        
                canvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                              era + "/" + "c_" + var_name + "_muon_val.png")  #Save .png file 
                
                
                print("\n")
                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "c_" + var_name + "_muon_val.png")
            

                canvas.SetLogy()
                canvas.Update()
                canvas.Draw()
                canvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                              era + "/" + "log_c_" + var_name + "_muon_val.png")  #Save .png file 

                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "log_c_" + var_name + "_muon_val.png")

                print("\n")

                #
                # Produce ratio plots
                #

                
                rcanvas = ROOT.TCanvas("r"+cName, "r"+cName, 900, 800)
                rcanvas.SetRightMargin(0.24)

                rcanvas.Divide(1,2)
                rcanvas.cd(1)
                
                plotPad = rcanvas.GetPad(1)
                plotPad.SetPad(0.,0.2,1.,1.)

                hist.GetXaxis().SetLabelSize(0.)
                hist.GetXaxis().SetTitleSize(0.)

                hist_mc.GetXaxis().SetLabelSize(0.)
                hist_mc.GetXaxis().SetTitleSize(0.)
                
                if use_MC:
                    hist_mc.Draw("HIST")

                if use_Data:
                    hist.Draw("E1 SAME")
                else:
                    hist.Draw("E1")


                #plotPath = os.path.join(plotDir, h)                                                                                                                                                       
                #rcanvas.SetLogy()                                                                                                                                                                         
                #rcanvas.SetGrid()                                                                                                                                                                         

                max1 = hist.GetMaximum()
                max2 = hist_mc.GetMaximum()

                if max1 > max2:
                    hist.SetMaximum(max1*0.2 + max1)
                    hist_mc.SetMaximum(max1*0.2 + max1)
                    hist_mc.GetYaxis().SetRangeUser(1, max1*0.2 + max1)
                    hist.GetYaxis().SetRangeUser(1, max1*0.2 + max1)
                else:
                    hist.SetMaximum(max2*0.2 + max2)
                    hist_mc.SetMaximum(max2*0.2 + max2)
                    hist_mc.GetYaxis().SetRangeUser(1, max2*0.2 + max2)
                    hist.GetYaxis().SetRangeUser(1, max2*0.2 + max2)


                legend = ROOT.TLegend(0.95, 0.9, 0.75, 0.78)
                #legend = ROOT.TLegend(0.5, 0.70, 0.92, 0.92)                                                                                                                                              

                if use_Data:
                    legend.AddEntry(hist, "Data")
                if use_MC:
                    legend.AddEntry(hist_mc, "Simulation", "l")

                legend.SetTextFont(42)
                legend.SetBorderSize(0)
                legend.SetFillColor(0)
                legend.Draw()

                CMS_lumi.cmsText = 'CMS'
                CMS_lumi.writeExtraText = True
                CMS_lumi.extraText = 'Preliminary'
                if lumi!=-1:
                    CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
                else:
                    CMS_lumi.lumi_13TeV = ""
                CMS_lumi.CMS_lumi(plotPad, 4, 11)


                #
                # Ratio pad
                #

                rcanvas.cd(2)

                ratioPad = rcanvas.GetPad(2)
                ratioPad.SetPad(0.,0.,1.,0.31)
                
                ratioPad.SetFillStyle(4000)
                ratioPad.SetBottomMargin(0.2)
                
                hRatio = hist.Clone()
                hRatio.SetTitle(" ")
                
                hRatio.GetXaxis().SetLabelSize(0.1)
                hRatio.GetXaxis().SetTitleSize(0.1)
                hRatio.GetXaxis().SetTitleOffset(.85)
                hRatio.GetXaxis().SetTitle(variables[var_name]['pretty'])
                
                hRatio.GetYaxis().SetLabelSize(0.07)
                hRatio.GetYaxis().SetTitleSize(0.1)
                hRatio.GetYaxis().SetTitleOffset(0.5)
                hRatio.GetYaxis().SetTitle("Data/MC")
                hRatio.GetYaxis().SetRangeUser(0.,2.)
                
                hRatio.Divide(hist_mc)
                hRatio.Draw()

                Xmax = hRatio.GetXaxis().GetXmax()
                Xmin = hRatio.GetXaxis().GetXmin()

                l = ROOT.TLine(Xmin, 1, Xmax, 1)
                l.SetLineColor(1) 
                l.Draw("same") 

                
                rcanvas.Draw()
                rcanvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/" + "c_ratio_" + var_name + "_muon_val.png")  #Save .png file           

                print("\n")
                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "c_ratio" + var_name + "_muon_val.png")


                rcanvas.cd(1)
                
                plotPad.SetLogy()
                
                hist_mc.GetYaxis().SetDefaults()
                hist.GetYaxis().SetDefaults()

                hist_mc.GetYaxis().SetTitle('Events')
                hist_mc.GetYaxis().SetTitleOffset(1.33)

                rcanvas.Update()
                rcanvas.Draw()

                rcanvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/" + "log_c_ratio_" + var_name + "_muon_val.png")  #Save .png file                           

                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "log_c_ratio" + var_name + "_muon_val.png")

                print("\n")

            if len(binVar) == 2:
                continue
                
    else:

        realized[_subera1] = run_files(particle, probe, resonance, era, _subera1, config, spark)
        realized[_subera2] = run_files(particle, probe, resonance, _era2, _subera2, config, spark)


        #binning = config['binning']
        #variables = config['variables']
        #binVariables = config['binVariables']

        binning = config.binning()
        variables = config.variables()
        binVariables = config.binVariables()

        ROOT.gROOT.SetBatch()
        ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 1001;")
        tdrstyle.setTDRStyle()


        for binVar in binVariables:
        
            if len(binVar) == 1:
                
                # subera1 array
                fill_array = np.zeros(len(binning[binVar[0]])+1) 
                
                # subera2 array
                fill_array_2 = np.zeros(len(binning[binVar[0]])+1) 
                
                var_name = binVar[0]
                
                # Fill the arrays for each subEra
                for subEra in subEras:
                    
                    df = realized[subEra][var_name]
                    df = df.T.drop_duplicates().T
                    df = df.sort_values(by=[var_name+'Bin'])
                    df = df.reset_index()
                    
                    values = pd.Series(np.zeros(len(binning[var_name])+1))
                    values[df.index] = df['sum(weight)']
                
                    if (_subera1 == subEra):
                        for i in df.index:
                            fill_array[i] = fill_array[i] + float(values[i])
                    else:
                        for i in df.index:
                            fill_array_2[i] = fill_array_2[i] + float(values[i])
            
                
                # Luminosity

                lumi = 0
                
                subera1_isMC = _subera1 in ['DY_madgraph', 'DY_powheg', 'JPsi_pythia8']
                subera2_isMC = _subera2 in ['DY_madgraph', 'DY_powheg', 'JPsi_pythia8']
                
                if subera1_isMC and subera2_isMC:
                    lumi = -1
                elif subera1_isMC:
                    lumi = registry.luminosity(particle, probe, resonance, _era2, _subera2)
                elif subera2_isMC:
                    lumi = registry.luminosity(particle, probe, resonance, era, _subera1)
                else:
                    lumi = registry.luminosity(particle, probe, resonance, _era2, _subera2) + registry.luminosity(particle, probe, resonance, era, _subera1)
                    

                #
                # Initialize histogram
                #
                
                bins = np.array(binning[var_name])
                bins = bins.astype(np.float32)
                
                hist = ROOT.TH1F(var_name, var_name, len(bins)-1, bins) #ROOT histogram
                hist_2 = ROOT.TH1F(var_name+'_2', var_name+'_2', len(bins)-1, bins) #ROOT histogram
                
                
                # Axis titles and options
                
                hist.GetYaxis().SetTitle('Events')
                hist.GetYaxis().SetTitleOffset(1.33)
                hist.GetXaxis().SetTitle(variables[var_name]['pretty'])
                
                
                # Fill histogram
                
                for i in range(0, len(fill_array)):
                    hist.SetBinContent(i, fill_array[i])
                    hist_2.SetBinContent(i, fill_array_2[i])

                    
                # Canvas initialization


                cName = var_name
                canvas = ROOT.TCanvas(cName, cName, 900, 800)
                ROOT.gStyle.SetPaintTextFormat("5.3f")
                canvas.SetRightMargin(0.24)

    
                # Normalize histograms

                if subera1_isMC and not subera2_isMC:
                    hist_2.SetMarkerStyle(ROOT.kFullCircle)

                    hist.SetLineWidth(3)
                    hist.SetLineColor(ROOT.kAzure-2)
                    hist.SetFillColor(ROOT.kAzure-2)

                    his.Scale(hist_2.Integral()/hist.Integral())
                    hist.Draw("HIST")
                    hist_2.Draw("E1 SAME")
                else:
                    hist.SetMarkerStyle(ROOT.kFullCircle)

                    hist_2.SetLineWidth(3)
                    hist_2.SetLineColor(ROOT.kAzure-4)
                    hist_2.SetFillColor(ROOT.kAzure-4)

                    hist_2.Scale(hist.Integral()/hist_2.Integral())
                    hist_2.Draw("HIST")
                    hist.Draw("E1 SAME")
                
    
                #plotPath = os.path.join(plotDir, h)
                #canvas.SetLogy()
                #canvas.SetGrid()
                
                max1 = hist.GetMaximum()
                max2 = hist_2.GetMaximum()
                
                if max1 > max2:
                    hist.SetMaximum(max1*0.2 + max1)
                    hist_2.SetMaximum(max1*0.2 + max1)
                else:
                    hist.SetMaximum(max2*0.2 + max2)
                    hist_2.SetMaximum(max2*0.2 + max2)
                    
                    
                legend = ROOT.TLegend(0.9, 0.9, 0.7, 0.78)
                
                
                if subera1_isMC and not subera2_isMC:
                    legend.AddEntry(hist, _subera1, "l")
                    legend.AddEntry(hist_2, _subera2)
                else:
                    legend.AddEntry(hist, _subera1)
                    legend.AddEntry(hist_2, _subera2, "l")
                
                legend.SetTextFont(42)
                legend.SetBorderSize(0)
                legend.SetFillColor(0)
                legend.Draw()
                
                canvas.Modified()
                canvas.Update()
                
                # CMS title and lumi
                
                CMS_lumi.cmsText = 'CMS'
                CMS_lumi.writeExtraText = True
                CMS_lumi.extraText = 'Preliminary'
                if lumi!=-1:
                    CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
                else:
                    CMS_lumi.lumi_13TeV = ""
                CMS_lumi.CMS_lumi(canvas, 4, 11)
                
                # Draw    
                # Saved as file: ./baseDir/plots/muon/generalTracks/Z/Run2018_UL/muon_pt_Run2018A_vs_Run2018B.png 
                
                directory = _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/"

                if not os.path.exists(directory):
                    os.makedirs(directory)

                canvas.Draw()        
                canvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                              era + "/" + "c_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")  #Save .png file 
                
                print("\n")
                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                      era + "/" + "c_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")

                
                canvas.SetLogy()
                canvas.Update()
                canvas.Draw()
                
                canvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                              era + "/" + "log_c_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")

                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                      era + "/" + "log_c_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")

                print("\n")


                #                                                                                                                                                                                          
                # Produce ratio plots                                                                                                                                                                      
                #                                                                                                                                                                                          


                rcanvas = ROOT.TCanvas("r"+cName, "r"+cName, 900, 800)
                rcanvas.SetRightMargin(0.24)

                rcanvas.Divide(1,2)
                rcanvas.cd(1)

                plotPad = rcanvas.GetPad(1)
                plotPad.SetPad(0.,0.2,1.,1.)

                hist.GetXaxis().SetLabelSize(0.)
                hist.GetXaxis().SetTitleSize(0.)

                hist_2.GetXaxis().SetLabelSize(0.)
                hist_2.GetXaxis().SetTitleSize(0.)


                if subera1_isMC and not subera2_isMC:
                    hist_2.SetMarkerStyle(ROOT.kFullCircle)

                    hist.SetLineWidth(3)
                    hist.SetLineColor(ROOT.kAzure-2)
                    hist.SetFillColor(ROOT.kAzure-2)

                    his.Scale(hist_2.Integral()/hist.Integral())
                    hist.Draw("HIST")
                    hist_2.Draw("E1 SAME")
                else:
                    hist.SetMarkerStyle(ROOT.kFullCircle)

                    hist_2.SetLineWidth(3)
                    hist_2.SetLineColor(ROOT.kAzure-4)
                    hist_2.SetFillColor(ROOT.kAzure-4)

                    hist_2.Scale(hist.Integral()/hist_2.Integral())
                    hist_2.Draw("HIST")
                    hist.Draw("E1 SAME")

                #plotPath = os.path.join(plotDir, h)                                                                                                                                                       
                #rcanvas.SetLogy()                                                                                                                                                                         
                #rcanvas.SetGrid()                                                                                                                                                                         

                max1 = hist.GetMaximum()
                max2 = hist_2.GetMaximum()

                if max1 > max2:
                    hist.SetMaximum(max1*0.2 + max1)
                    hist_2.SetMaximum(max1*0.2 + max1)
                    hist_2.GetYaxis().SetRangeUser(1, max1*0.2 + max1)
                    hist.GetYaxis().SetRangeUser(1, max1*0.2 + max1)
                else:
                    hist.SetMaximum(max2*0.2 + max2)
                    hist_2.SetMaximum(max2*0.2 + max2)
                    hist_2.GetYaxis().SetRangeUser(1, max2*0.2 + max2)
                    hist.GetYaxis().SetRangeUser(1, max2*0.2 + max2)


                legend.Draw()


                CMS_lumi.cmsText = 'CMS'
                CMS_lumi.writeExtraText = True
                CMS_lumi.extraText = 'Preliminary'
                if lumi!=-1:
                    CMS_lumi.lumi_13TeV = "%0.1f fb^{-1}" % (lumi)
                else:
                    CMS_lumi.lumi_13TeV = ""
                CMS_lumi.CMS_lumi(plotPad, 4, 11)


                #                                                                                                                                                                                          
                # Ratio pad                                                                                                                                                                                
                #                                                                                                                                                                                          


                rcanvas.cd(2)

                ratioPad = rcanvas.GetPad(2)
                ratioPad.SetPad(0.,0.,1.,0.31)

                ratioPad.SetFillStyle(4000)
                ratioPad.SetBottomMargin(0.2)

                if subera1_isMC and not subera2_isMC:
                    hRatio = hist_2.Clone()
                    hRatio.Divide(hist)
                else:
                    hRatio = hist.Clone()
                    hRatio.Divide(hist_2)


                hRatio.SetTitle(" ")

                hRatio.GetXaxis().SetLabelSize(0.1)
                hRatio.GetXaxis().SetTitleSize(0.1)
                hRatio.GetXaxis().SetTitleOffset(.85)
                hRatio.GetXaxis().SetTitle(variables[var_name]['pretty'])

                hRatio.GetYaxis().SetLabelSize(0.07)
                hRatio.GetYaxis().SetTitleSize(0.1)
                hRatio.GetYaxis().SetTitleOffset(0.5)
                
                if subera1_isMC or subera2_isMC:
                    hRatio.GetYaxis().SetTitle("Data/MC")
                else:
                    hRatio.GetYaxis().SetTitle("Data 1/Data 2")
                hRatio.GetYaxis().SetRangeUser(0.,2.)

                hRatio.Draw()

                Xmax = hRatio.GetXaxis().GetXmax()
                Xmin = hRatio.GetXaxis().GetXmin()

                l = ROOT.TLine(Xmin, 1, Xmax, 1)
                l.SetLineColor(1)
                l.Draw("same")


                rcanvas.Draw()
                rcanvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/" + "c_ratio_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")  #Save .png file

                print("\n")
                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "c_ratio_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")


                rcanvas.cd(1)

                hist_2.GetYaxis().SetDefaults()
                hist.GetYaxis().SetDefaults()

                hist.GetYaxis().SetTitle('Events')
                hist.GetYaxis().SetTitleOffset(1.33)

                plotPad.SetLogy()
                rcanvas.Update()
                rcanvas.Draw()

                rcanvas.SaveAs(_baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" + era + "/" + "log_c_ratio_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")
                
                print(str(var_name) + " distribution saved at: " + _baseDir + "/plots/" + particle + "/" + probe + "/" + resonance + "/" +
                             era + "/" + "log_c_ratio_" + var_name + "_" + _subera1 + "_vs_" + _subera2 + "_muon_val.png")

                print("\n")

                
                
                
            if len(binVar) == 2:
                pass


                
    spark.stop()

