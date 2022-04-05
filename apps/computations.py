# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 18:54:05 2022

@author: davi_fr
"""

import sys
import os
import json
import read_write_files as fop
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
from ast import literal_eval

process = 'computations'
startDate = ''
endDate = ''
startYear = 0
endYear = 0
analysisPeriod = np.arange(startYear, endYear+1)
projectFolder = ''
projectName = ''


def getStartDate(formatConfig):
    startDate = formatConfig['analysisStartDate'] if 'analysisStartDate' in formatConfig else ''
    return startDate

def getEndDate(formatConfig):
    startDate = formatConfig['analysisEndDate'] if 'analysisEndDate' in formatConfig else ''
    return startDate

def getProjectPath(formatConfig):
    projectFolder = formatConfig['projectPath'] if 'projectPath' in formatConfig else ''
    return projectFolder

def getProjectName(formatConfig):
    projectName = formatConfig['projectName'] if 'projectName' in formatConfig else ''
    return projectName

def getDataConfig(dataConfig):
    dataFiles = dataConfig['dataConfig'] if 'dataConfig' in dataConfig else []
    return dataFiles

def getYearFromDate(date):
    year = datetime.strptime(date, '%Y-%m-%d').strftime('%Y')
    year = int(year)
    return year

def getDataFileLocation(dataConfig):
    fileLocation = dataConfig['fileLocation'] if 'fileLocation' in dataConfig else ''
    return fileLocation

def getFileFrequency(metadata):
    fileFrequency = metadata['fileFrequency'] if 'fileFrequency' in metadata else ''
    return fileFrequency

def getOperation(metadata):
    operation = metadata['operation'] if 'operation' in metadata else ''
    return operation

def getGroupByArgument(metadata):
    groupBy = metadata['groupBy'] if 'groupBy' in metadata else ''
    return groupBy

def getWindowSize(metadata):
    windowSize = metadata['windowSize'] if 'windowSize' in metadata else 1
    return windowSize

def getMinPeriod(metadata):
    minimumPeriod = metadata['minimumPeriod'] if 'minimumPeriod' in metadata else 1
    return minimumPeriod

def getDataName(metadata):
    name = metadata['name'] if 'name' in metadata else ''
    return name

def getProcessSaveLocation(operation, folderName):
    loadDataSaveLocation = os.path.join(projectFolder, process, operation, folderName)
    return loadDataSaveLocation

def getNewValueFilePath(metadata):
    fileLocation = metadata['newValueFile'] if 'newValueFile' in metadata else ''
    return fileLocation

def getReferenceValueFilePath(metadata):
    fileLocation = metadata['referenceValueFile'] if 'referenceValueFile' in metadata else ''
    return fileLocation

def getValueFromDict(metadata, key):
    val = metadata[key] if key in metadata else ''
    return val

def getDateTimeIndexColumn(df):
    indexDTypes = []
    dateIndexColumn = None
    if len(df.index.names) > 1:
        indexDTypes = df.index.dtypes
        for i in range(len(indexDTypes)):
            indexColumn = df.index.names[i]
            if indexDTypes[i] == 'datetime64[ns]':
                dateIndexColumn = indexColumn
                break
    elif len(df.index.names) == 1:
        indexDTypes = df.index.dtype
        if indexDTypes == 'datetime64[ns]':
            dateIndexColumn = df.index.name   
            
    return dateIndexColumn

def getTimeIndexColumn(df):
    indexValue = None
    timeIndexColumn = None
    indexNames = []
    try:
        indexValue = df.index[0]
        indexNames = df.index.names
    except:
        print('No index values found')
    if isinstance(indexValue, (list, tuple)) and len(indexValue) > 1:
        for i in range(len(indexValue)):
            value = indexValue[i]
            if isinstance(value, dt.time):
                timeIndexColumn = indexNames[i]
                break
    else:
        print('--------')
        if isinstance(indexValue, dt.time):
            timeIndexColumn = indexNames
            
    return timeIndexColumn


                
def getDateIndexColumn(df):
    indexValue = None
    dateIndexColumn = None
    indexNames = []
    try:
        indexValue = df.index[0]
        indexNames = df.index.names
    except:
        print('No index values found')
    print(indexValue)
    print(indexNames)
    if isinstance(indexValue, (list, tuple)) and len(indexValue) > 1:
        for i in range(len(indexValue)):
            value = indexValue[i]
            if isinstance(value, dt.date):
                dateIndexColumn = indexNames[i]
                break
    else:
        if isinstance(indexValue, dt.date):
            if isinstance(indexNames, (list, tuple)) and len(indexNames) > 0:
                dateIndexColumn = indexNames[0]
            else:
                dateIndexColumn = indexNames
            
    return dateIndexColumn
    
def getConsecutiveFilesCombined(filePath, year):
    fileYear = [year-1, year, year+1]
    data = pd.DataFrame()
    for year in fileYear:
        fileName = str(year) + '.h5'
        yearlyFile = os.path.join(filePath, fileName)
        fileData = fop.readH5File(yearlyFile)
        data = pd.concat([data, fileData])
    return data

    
def redirectToOperation(metadata):
    operation = getOperation(metadata)
    if operation == 'moving_average':
        findMovingAverage(metadata)
        print(operation)
    elif operation == 'relative_difference':
        findRelativeDifference(metadata)
        print(operation)
    elif operation == 'mean':
        findMean(metadata)
        print(operation)
    elif operation == 'correlation':
        findCorrelation(metadata)
        print(operation)
    elif operation == 'normalised_correlation':
        findNormalisedCorrelation(metadata)
        print(operation)
    elif operation == 'confidence_interval':
        findConfidenceInterval(metadata)
        print(operation)
    elif operation == 'append':
        appendDataFiles(metadata)
        print(operation)
    elif operation == 'extract_column':
        extractColumnData(metadata)
    elif operation == 'interpolate':
        interpolateData(metadata)
        
def fillNaNValueWithInterpolation(data):
    data = data.interpolate().bfill()
    data.head(4)
 
    return data  

def interpolateData(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    windowSize = getWindowSize(metadata)
    minimumPeriod = getMinPeriod(metadata)
    operation = getOperation(metadata)
    name = getDataName(metadata)
    groupBy = getGroupByArgument(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)    
    data = pd.DataFrame()
    extractedData = pd.DataFrame()
    if fileFrequency == 'yearly':
        
        for year in analysisPeriod:
            data = getConsecutiveFilesCombined(filePath, year)
            dateTimeIndexColumn = getDateIndexColumn(data)
            print(dateTimeIndexColumn)
            print(data)
            data = data[pd.to_datetime(data.index.get_level_values(dateTimeIndexColumn)).year == year]
            extractedData = pd.concat([extractedData, data])   
            
        fileName = name +'.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(extractedData, saveFileLocation)
        print('saved', saveFileLocation)
    elif fileFrequency == 'single':
        data = fop.readH5File(filePath)
        dateTimeIndexColumn = getDateIndexColumn(data)
        print(dateTimeIndexColumn)
        data = fillNaNValueWithInterpolation(data)
                    
        fileName = name+'.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(data, saveFileLocation)
        
        
def extractColumnData(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    windowSize = getWindowSize(metadata)
    minimumPeriod = getMinPeriod(metadata)
    operation = getOperation(metadata)
    name = getDataName(metadata)
    groupBy = getGroupByArgument(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    extractColumns = getValueFromDict(metadata, 'columns')
    extractColumns = literal_eval(extractColumns)
    os.makedirs(saveLocation, exist_ok = True)    
    data = pd.DataFrame()
    extractedData = pd.DataFrame()
    if fileFrequency == 'yearly':
        
        for year in analysisPeriod:
            data = getConsecutiveFilesCombined(filePath, year)
            dateTimeIndexColumn = getDateIndexColumn(data)
            data = data[[extractColumns]]
            # data = data[data.columns[extractColumns]]
            data = data[pd.to_datetime(data.index.get_level_values(dateTimeIndexColumn)).year == year]
            extractedData = pd.concat([extractedData, data]) 
            print(extractedData)
            
        fileName = name +'.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(extractedData, saveFileLocation)
        print('saved', saveFileLocation)
    elif fileFrequency == 'single':
        data = fop.readH5File(filePath)
        dateTimeIndexColumn = getDateIndexColumn(data)
        print(dateTimeIndexColumn)
        data = data[extractColumns]
                    
        fileName = name+'.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(data, saveFileLocation)
        print('saved', saveFileLocation)
    

        
def movingAverage(data, dateTimeIndexColumn, groupBy=None, minPeriod=1, windowSize = 27):
    movingAvg = pd.DataFrame()
    timeIndexColumn = getTimeIndexColumn(data)
    if groupBy == 'hour':
        movingAvg = data.groupby([timeIndexColumn], 
                                 as_index=False).rolling(window=windowSize, 
                                                         min_periods=minPeriod).mean().drop(timeIndexColumn, 
                                                                                            axis=1)
                                                                                                  
    else:
        print(data)
        movingAvg = data.rolling(window=windowSize, 
                                 min_periods=minPeriod).mean()
      
    return movingAvg

def relativeDifference(referenceValue, newValue):
    diffRel = ((referenceValue - newValue)/newValue)*100
    return diffRel


def findRelativeDifference(metadata):
    fileFrequency = getFileFrequency(metadata)
    newValueFile = getNewValueFilePath(metadata)
    print(newValueFile)
    referenceValueFile = getReferenceValueFilePath(metadata)
    print(referenceValueFile)
    operation = getOperation(metadata)
    name = getDataName(metadata)
    groupBy = getGroupByArgument(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)    
    if fileFrequency == 'yearly':
        for year in analysisPeriod:
            fileName = str(year) + '.h5'
            newValueFileLocation = os.path.join(newValueFile, fileName)
            referenceValueFileLocation = os.path.join(referenceValueFile, fileName)
            print(newValueFileLocation)
            print(referenceValueFileLocation)
            newValueData = fop.readH5File(newValueFileLocation)
            referenceValueData = fop.readH5File(referenceValueFileLocation)
            print('----------New Value----') 
            print(newValueData)
            print('----------Reference Value----') 
            print(referenceValueData)
            relativeDifferenceData = relativeDifference(referenceValueData, newValueData)
            
            saveFileLocation = os.path.join(saveLocation, fileName)
            fop.saveToHDFFile(relativeDifferenceData, saveFileLocation)
            print('saved', saveFileLocation)
            print(relativeDifferenceData)
    elif fileFrequency == 'single':
        newValueData = fop.readH5File(newValueFile)
        referenceValueData = fop.readH5File(referenceValueFile)
        relativeDifferenceData = relativeDifference(referenceValueData, newValueData)
        fileName = name + '.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(relativeDifferenceData, saveFileLocation)
        print('saved', saveFileLocation)
        print(relativeDifferenceData)
            
def findMovingAverage(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    windowSize = getWindowSize(metadata)
    minimumPeriod = getMinPeriod(metadata)
    operation = getOperation(metadata)
    name = getDataName(metadata)
    groupBy = getGroupByArgument(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)    
    data = pd.DataFrame()
    if fileFrequency == 'yearly':
        for year in analysisPeriod:
            data = getConsecutiveFilesCombined(filePath, year)
            dateTimeIndexColumn = getDateIndexColumn(data)
            print(dateTimeIndexColumn)
            movingAvg = movingAverage(data, dateTimeIndexColumn,  groupBy=groupBy, minPeriod=minimumPeriod, windowSize=windowSize)
            movingAvg = movingAvg[pd.to_datetime(movingAvg.index.get_level_values(dateTimeIndexColumn)).year == year]
                        
            fileName = str(year)+'.h5'
            saveFileLocation = os.path.join(saveLocation, fileName)
            fop.saveToHDFFile(movingAvg, saveFileLocation)
            print('saved', saveFileLocation)
    elif fileFrequency == 'single':
        data = fop.readH5File(filePath)
        dateTimeIndexColumn = getDateIndexColumn(data)
        print(dateTimeIndexColumn)
        movingAvg = movingAverage(data, dateTimeIndexColumn,  groupBy=groupBy, minPeriod=minimumPeriod, windowSize=windowSize)
                    
        fileName = name+'.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(movingAvg, saveFileLocation)
        print('saved', saveFileLocation)
        print(movingAvg)

def getColumnAxis(metadata):
    groupBy = getGroupByArgument(metadata)
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    data = pd.DataFrame()
    if fileFrequency == 'yearly':
        for year in analysisPeriod:
            fileName = str(year) + '.h5'
            dataFileLocation = os.path.join(filePath, fileName)
            data = fop.readH5File(dataFileLocation)
            break
    elif fileFrequency == 'single':
        data = fop.readH5File(filePath)
    print(data)    
    axis = None
    columnName = None
    if groupBy == 'date' or groupBy == 'season':
        columnName = getDateIndexColumn(data)
        axis = 0
    elif groupBy == 'time':
        columnName = getTimeIndexColumn(data)
        axis = 0
    else:
        if groupBy in data.columns.names:
            columnName = groupBy
            axis = 1
            
    return (columnName, axis)    
        
    
def findMean(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    name = getDataName(metadata)
    groupBy = getGroupByArgument(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)
    (columnName, axis) = getColumnAxis(metadata)  
    print(columnName, axis)
    if fileFrequency == 'yearly':
        for year in analysisPeriod:
            fileName = str(year) + '.h5'
            dataFileLocation = os.path.join(filePath, fileName)
            data = fop.readH5File(dataFileLocation)
            print(data)
            meanData = data.groupby(level = columnName, axis = axis).mean()
            print(meanData)
            
            fileName = str(year)+'.h5'
            saveFileLocation = os.path.join(saveLocation, fileName)
            fop.saveToHDFFile(meanData, saveFileLocation)
            print('saved', saveFileLocation)
    elif fileFrequency == 'single':
        print(filePath)
        data = fop.readH5File(filePath)
        meanData = pd.DataFrame()
        if groupBy == 'season':
            meanData = computeSeasonalMean(data, columnName)
        else:
            meanData = data.groupby(level = columnName, axis = axis).mean()
            
        fileName = name + '.h5'
        saveFileLocation = os.path.join(saveLocation, fileName)
        fop.saveToHDFFile(meanData, saveFileLocation)
        print('saved', saveFileLocation)
        print(meanData)   
        
        
        
def appendDataFiles(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    name = getDataName(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)
    combinedData = pd.DataFrame()
    if fileFrequency == 'yearly':
        for year in analysisPeriod:
            fileName = str(year) + '.h5'
            dataFileLocation = os.path.join(filePath, fileName)
            data = fop.readH5File(dataFileLocation)
            combinedData = pd.concat([combinedData, data])
        
    fileName = name + '.h5'
    saveFileLocation = os.path.join(saveLocation, fileName)
    fop.saveToHDFFile(combinedData, saveFileLocation)
    print('saved', saveFileLocation) 
    print(combinedData)

def adjustData(data, threshold=-9999):
    data = data.fillna(0)
    if threshold!= None and threshold > 0:
        data = data.where(data < threshold, other=0)
    return data

def findCorrelation(metadata):
    name = getDataName(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)
    
    data_1Loc = getValueFromDict(metadata, 'data_1') 
    data_2Loc = getValueFromDict(metadata, 'data_2') 
    
    data_1Max = getValueFromDict(metadata, 'data_1Max') 
    data_2Max = getValueFromDict(metadata, 'data_2Max') 
    
    data_1Attributes = getValueFromDict(metadata, 'data_1Attributes') 
    data_2Attributes = getValueFromDict(metadata, 'data_2Attributes') 
    
    data_1 = fop.readH5File(data_1Loc)
    data_1 = adjustData(data_1, data_1Max)
    if data_1Attributes != None:
        data_1 =data_1[data_1Attributes]
    print(data_1)
    data_2 = fop.readH5File(data_2Loc)
    data_2 = adjustData(data_2, data_2Max)
    if data_2Attributes != None:
        data_2 =data_2[data_2Attributes]
    print(data_2)
    
    windowSize = getValueFromDict(metadata, 'windowSize')
    print(windowSize)
    crossCorr = data_1.rolling(windowSize).corr(data_2)
    print(crossCorr)
    fileName = name + '.h5'
    saveFileLocation = os.path.join(saveLocation, fileName)
    fop.saveToHDFFile(crossCorr, saveFileLocation)
    print('saved', saveFileLocation) 

    
def gaussianWindowNormalisation(parameter_1, parameter_2, windowSize):
    
    wsize = windowSize
    
    norm = parameter_1.copy()
    norm[:] = np.NaN
    
    for shift in np.arange(wsize / 2, len(parameter_1)-wsize/2+1):
        gseries = np.exp(- 0.5 * (((np.arange(1,len(parameter_1)+1)) - shift) / wsize * 4) ** 2)

        param_1_norm = parameter_1.multiply(gseries, axis=0, fill_value=0)
        param_2_norm = parameter_2.multiply(gseries, axis=0, fill_value=0)

        corrNorm = param_1_norm.corrwith(param_2_norm.shift(0))
        i_shift = int(shift) + int(windowSize/2)
        if i_shift < len(norm):
            print(i_shift)
            norm.iloc[i_shift] = corrNorm
        
    return norm


def findNormalisedCorrelation(metadata):
    name = getDataName(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)
    
    data_1Loc = getValueFromDict(metadata, 'data_1') 
    data_2Loc = getValueFromDict(metadata, 'data_2') 
    
    data_1Max = getValueFromDict(metadata, 'data_1Max') 
    data_2Max = getValueFromDict(metadata, 'data_2Max') 
    
    data_1Attributes = getValueFromDict(metadata, 'data_1Attributes') 
    data_2Attributes = getValueFromDict(metadata, 'data_2Attributes') 
    
    data_1 = fop.readH5File(data_1Loc)
    data_1 = adjustData(data_1, data_1Max)
    if data_1Attributes != None:
        data_1 =data_1[data_1Attributes]
    print(data_1)
    data_2 = fop.readH5File(data_2Loc)
    data_2 = adjustData(data_2, data_2Max)
    if data_2Attributes != None:
        data_2 =data_2[data_2Attributes]
    print(data_2)
    
    windowSize = getValueFromDict(metadata, 'windowSize')
    print(windowSize)
    
    normCorrelation = gaussianWindowNormalisation(data_1, data_2, windowSize)
    
    print(normCorrelation)
    fileName = name + '.h5'
    saveFileLocation = os.path.join(saveLocation, fileName)
    fop.saveToHDFFile(normCorrelation, saveFileLocation)
    print('saved', saveFileLocation)


def confidenceInterval(data):
    XCorr = data
    z = np.arctanh(XCorr)
    sig2 = 1 /90
    upperConfInterval = np.tanh(z + 1.96 * np.sqrt(sig2))
    lowerConfInterval = np.tanh(z - 1.96 * np.sqrt(sig2))
    
    return (lowerConfInterval, upperConfInterval)

def findConfidenceInterval(metadata):
    fileFrequency = getFileFrequency(metadata)
    filePath = getDataFileLocation(metadata)
    name = getDataName(metadata)
    saveLocation = getProcessSaveLocation(operation, name)
    os.makedirs(saveLocation, exist_ok = True)
    
    data = fop.readH5File(filePath)
    print('-------------Conf interval data-----------------------')
    print(data)
    (lowerConfInterval, upperConfInterval) = confidenceInterval(data)
    print('----------------upper and lower---------------')
    print(lowerConfInterval, upperConfInterval)
    lowerConfIntervalFile = name + '_' + 'lowerConfInterval' + '.h5'
    upperConfIntervalFile = name + '_' + 'upperConfInterval' + '.h5'
    lowerLimitFile = os.path.join(saveLocation, lowerConfIntervalFile)
    upperLimitFile = os.path.join(saveLocation, upperConfIntervalFile)
    fop.saveToHDFFile(lowerConfInterval, lowerLimitFile)
    fop.saveToHDFFile(upperConfInterval, upperLimitFile)
    print('confidence interval files saved')
    
def computeSeasonalMean(data, indexColumn):
    mean = data.groupby([pd.to_datetime(data.index.get_level_values(indexColumn)).month, 
                         pd.to_datetime(data.index.get_level_values(indexColumn)).day]).mean()
    mean.index.names = ['month', 'day']
    return mean

          
    
if __name__ == "__main__":
    configFilePath = sys.argv[1]
    configFilePath = os.path.normpath(configFilePath)
    formatConfig = fop.readJsonFile(configFilePath)
    
    dataConfig = getDataConfig(formatConfig)
    projectFolder = getProjectPath(formatConfig) # loadConfig['projectPath'] if 'projectPath' in loadConfig else ''
    projectName = getProjectName(formatConfig)
    
    startDate = getStartDate(formatConfig)
    endDate = getEndDate(formatConfig)
    startYear = getYearFromDate(startDate)
    endYear = getYearFromDate(endDate)
    analysisPeriod = np.arange(startYear, endYear+1)
    
    for metadata in dataConfig:
        operation = getOperation(metadata)
        redirectToOperation(metadata)