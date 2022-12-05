# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 20:08:52 2022

@author: davi_fr
"""
import sys
import os
import json
import read_write_files as fop
import pandas as pd
import numpy as np
from datetime import datetime


projectFolder = ''
projectName = ''



# getting Product 
def prod(val) : 
    res = 1 
    for ele in val: 
        res *= ele 
    return res

# extract tec data from cdf files
# input as raw data and satellites as priority wise
def extractTecData(data, metadata):
    
    attributes = metadata['attributes'] if 'attributes' in metadata else []
    indexAttributes = metadata['indexAttributes'] if 'indexAttributes' in metadata else []
    
    tempIndexData = []
    tecValueShape = 1
    for index in indexAttributes:
        
        iData = data[index][:] if index in data else []
        tempIndexData.append(iData)
        tecValueShape = tecValueShape * len(iData)
    indexData = []
    try:
        indexData = pd.MultiIndex.from_product(tempIndexData, 
                                               names=indexAttributes)
    except Exception as e:
        print('failed to create multi-indexing')
    
    cdfDataFrame = pd.DataFrame()
    try:
        cdfDataFrame = pd.DataFrame(index=indexData)
    except:
        print('Failed to initiale data frame with multi-indexing')
        
    for attribute in attributes:
        if attribute not in indexAttributes:
            tempData = data[attribute][:] if attribute in data else []
            totalDataLength = prod(list(tempData.shape))
            if totalDataLength == tecValueShape:
                tempData = np.reshape(tempData, (tecValueShape))
                cdfDataFrame[attribute] = tempData
        cdfDataFrame[cdfDataFrame <= 0] = np.nan       
    return cdfDataFrame
    

# An empty dataframe is initialised with the given metadata
# attribute name in metadata is used to create the column names
def initDataFrame(metadata):
    columnNames = []
    indexColumns = []
    for element in metadata:
        attribute = element['attrName'] if 'attrName' in element else ''
        isIndex = element['isIndex'] if 'isIndex' in element else ''
        if isIndex:
            indexColumns.append(attribute)
            
        columnNames.append(attribute)   
    df = pd.DataFrame(columns=columnNames)
    return df


def getIndexColumns(metadata):
    indexColumns = []
    for element in metadata:
        attribute = element['attrName'] if 'attrName' in element else ''
        isIndex = element['isIndex'] if 'isIndex' in element else ''
        if isIndex:
            indexColumns.append(attribute)
        
    return indexColumns

# extract data values from text lines based on the given meta data
# metadata comprises what is the data, type of the data and positions need to be extracted
# Threshold value: if the attribute value goes beyond thresold value then it is considered as NaN
def extractDataFromTxtLine(txt, metadata):
    value = {}
    for element in metadata:
        attribute = element['attrName'] if 'attrName' in element else ''
        startPosition = element['startPosition'] if 'startPosition' in element else 0
        endPosition = element['endPosition'] if 'endPosition' in element else 0
        thresholdValue = element['maxThresholdValue'] if 'maxThresholdValue' in element else 0
        dataDateFormat = element['dateFormat'] if 'dateFormat' in element else None
        isIndex = element['isIndex'] if 'isIndex' in element else False
        try:
            attrValue = txt[startPosition:endPosition]
            if attrValue == '':
                break
            if thresholdValue != 0:
                try:
                    attrValue = float(attrValue)
                    attrValue = float('NaN') if attrValue <= 0 else attrValue
                    # checks attribute value goes above threshold value. if goes beyod invalidate
                    if attrValue > thresholdValue:
                        attrValue = float('NaN')
                    
                except:
                    attrValue = float('NaN')
                    error = 'Value cannot be converted to float'
                    print(error)
                    
            if dataDateFormat != '' and dataDateFormat is not None:
                attrValue = datetime.strptime(attrValue, dataDateFormat).strftime('%Y-%m-%d')
                
            if isIndex and attrValue == float('NaN'):
                value = {}
                break
                
            value[attribute] = attrValue
        except:
            error = 'No data available at this position'
            print(error)
    return value

def extractDataFromTextFile(data, metadata):
    attrMetaData = metadata['attributes'] if 'attributes' in metadata else []
    dataFrame = initDataFrame(attrMetaData)
    indexColumns = getIndexColumns(attrMetaData)
    print(indexColumns)
    for dataLine in data.splitlines():
        if dataLine[:1] != '#':
            values = extractDataFromTxtLine(dataLine, attrMetaData)
            try:
                dataFrame = dataFrame.append(values, ignore_index=True)
                print(dataFrame)
            except:
                error = 'cannot append data into dataframe'
                print(error)
                
    dataFrame = dataFrame.set_index(indexColumns)
    print(dataFrame)
    return dataFrame
        

def readFilesFromDirectoryRecursively(metadata):
    fileType = getFileType(metadata)
    dataLocation = getDataLocation(metadata)
    try:
        dataLocation = os.path.normpath(dataLocation)
    except:
        print('File Location does not exist')
    isFilesAvailable = False
    for subdir, dirs, files in os.walk(dataLocation):
        for file in files:
            if file.endswith(fileType):
                dataFile = os.path.join(subdir, file)
                isFilesAvailable = True
                if fileType == '.cdf':
                    data = fop.readCDFFile(dataFile)
                    cdfData = extractTecData(data, metadata)
                    dataSaveLocation = fop.prepareLocationForSaving(metadata, file)
                    print('Saving Location: ', dataSaveLocation)
                    fop.saveToHDFFile(cdfData, dataSaveLocation)
                    
                elif fileType == '.txt':
                    data = fop.readTextFile(dataFile)
                    txtData = extractDataFromTextFile(data, metadata)
                    fileName = getAttributeName(metadata)
                    dataSaveLocation = fop.prepareLocationForSaving(metadata, fileName)
                    print('Saving Location: ', dataSaveLocation)
                    fop.saveToHDFFile(txtData, dataSaveLocation)
    if isFilesAvailable == False:
        print('No files avaialble in the directory', dataLocation)


def getDataConfig(loadConfig):
    dataConfig = loadConfig['dataConfig'] if 'dataConfig' in loadConfig else []
    return dataConfig

def getProjectPath(loadConfig):
    projectFolder = loadConfig['projectPath'] if 'projectPath' in loadConfig else ''
    return projectFolder

def getProjectName(loadConfig):
    projectName = loadConfig['projectName'] if 'projectName' in loadConfig else ''
    return projectName


def getAttributeName(metadata):
    name = metadata['name'] if 'name' in metadata else ''
    return name
        
def getDataLocation(metadata):
    dataLocation = metadata['fileLocation'] if 'fileLocation' in metadata else None
    return dataLocation

def getProcessSaveLocation(projectFolder, folderName):
    loadDataSaveLocation = os.path.join(projectFolder, 'load_data', folderName)
    return loadDataSaveLocation

def getFileType(metadata):
    fileType = metadata['fileType'] if 'fileType' in metadata else ''
    return fileType

def getDataAttributes(metadata):
    attributes = metadata['attributes'] if 'attributes' in metadata else []
    indexAttributes = metadata['indexAttributes'] if 'indexAttributes' in metadata else []
    return (attributes, indexAttributes)
    

if __name__ == "__main__":
    configFilePath = sys.argv[1]
    configFilePath = os.path.normpath(configFilePath)
    loadConfig = fop.readJsonFile(configFilePath)
    
    
    dataConfig = getDataConfig(loadConfig) # loadConfig['dataConfig'] if 'dataConfig' in loadConfig else []
    projectFolder = getProjectPath(loadConfig) # loadConfig['projectPath'] if 'projectPath' in loadConfig else ''
    projectName = getProjectName(loadConfig) # loadConfig['projectName'] if 'projectName' in loadConfig else ''
    
    
    
    print('Begin Execution: Load Data')
    for metadata in dataConfig:
        name = getAttributeName(metadata) # metadata['name'] if 'name' in metadata else ''
        dataLocation = getDataLocation(metadata) #metadata['fileLocation'] if 'fileLocation' in metadata else None
        loadDataSaveLocation = getProcessSaveLocation(projectFolder, name) # os.path.join(projectFolder, 'load_data', name)
        metadata['saveLocation'] = loadDataSaveLocation
        try:
            dataLocation = os.path.normpath(dataLocation)
        except:
            print('File Location does not exist')
        fileType = getFileType(metadata)#metadata['fileType'] if 'fileType' in metadata else ''
        (attributes, indexAttributes) = getDataAttributes(metadata)
        # attributes = metadata['attributes'] if 'attributes' in metadata else []
        # indexAttributes = metadata['indexAttributes'] if 'indexAttributes' in metadata else []
        
        if os.path.exists(dataLocation):
            readFilesFromDirectoryRecursively(metadata)
        else:
            print('File Location does not exist')
    print('End Execution: Load Data')