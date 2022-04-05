# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 18:20:36 2022

@author: davi_fr
"""
import os
import json
os.environ["CDF_LIB"] = "C:\cdf3.8.0_64bit_VS2015\lib"
from spacepy import pycdf

def getFileType(path):
    fileExt = ''
    filePath = ''

    if os.path.exists(path):
        print('path exists')
    else:
        return fileExt
    
    for root, dirs, files in os.walk(path):
    	for file in files:
            try:
                fileExt = os.path.splitext(file)[1]
                filePath = os.path.join(root, file)
                break
            except:
                fileExt = ''
                break
    
    return (fileExt, filePath)


def readCDFFile(fileName):
    print(fileName)
    try:
        cdfFile = pycdf.CDF(fileName)
        return cdfFile
    except:
        print('data could not read')
        error = 'Can not read file: ' + fileName 
        
    return None

def getCDFAttributes(cdfFile):
    attributes = []
    for att in cdfFile:
        attributes.append(att)
    return attributes

def checkFolderPathIsValid(path):
    return os.path.exists(path)

def writeToJsonFile(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
        
        