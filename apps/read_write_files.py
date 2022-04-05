# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 16:55:09 2022

@author: davi_fr
"""
import numpy as np
import pandas as pd
# from datetime import datetime
# import matplotlib.pyplot as plt
import os
import json
from error_log import log

os.environ["CDF_LIB"] = "C:\cdf3.8.0_64bit_VS2015\lib"
from spacepy import pycdf
from pathlib import Path
# Read hdf files. 
# file name should contain complete path to the file
def readH5File(fileName):
    df = pd.DataFrame()
    try:
        df = pd.read_hdf(fileName)
        return df
    except:
        error = 'Can not read file: ' + fileName 
        log(error)
    return df
        
# Read txt files. 
# file name should contain complete path to the file       
def readTextFile(fileName):
    try:
        dataFile = open(fileName, 'r')
        txtData = dataFile.read()
        return txtData
    except:
        error = 'Can not read file: ' + fileName 
        log(error)
    
    
# Read cdf (common data format). Refer https://cdf.gsfc.nasa.gov/
# Make sure cdf library is installed and CDF_LIB path is defined
# Link : https://spdf.gsfc.nasa.gov/pub/software/cdf/dist/cdf38_0/windows/
# os.environ["CDF_LIB"] = "C:\cdf3.8.0_64bit_VS2015\lib"
# Make sure pycdf libray is installed
def readCDFFile(fileName):
    try:
        cdfFile = pycdf.CDF(fileName)
        return cdfFile
    except:
        error = 'Can not read file: ' + fileName 
        log(error)


def prepareLocationForSaving(metadata, file):
    try:
        dataSaveLocation = metadata['saveLocation'] if 'saveLocation' in metadata else ''
        os.makedirs(dataSaveLocation, exist_ok = True)
        dataSaveLocation = os.path.join(dataSaveLocation, file)
        pre, ext = os.path.splitext(dataSaveLocation)
        print('file split: ', pre, ext )
        dataSaveLocation = pre + '.h5'
        return dataSaveLocation
    except:
        print('Failed to save data file')
        return None
    
    return None
        
def saveToHDFFile(dataframe, filePath):
    try:
        dataframe.to_hdf(filePath, key='df')
    except:
        error = 'Failed to save dataframe: ' + filePath 
        log(error)
        
def readJsonFile(path):
    input_file = open(path)
    json_array = json.load(input_file)
    return json_array