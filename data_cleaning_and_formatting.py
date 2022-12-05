# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 14:31:53 2022

@author: davi_fr
"""
import sys
import os
import json
import read_write_files as fop
import pandas as pd
import numpy as np
from datetime import datetime


def getDataConfig(formatConfig):
    dataConfig = formatConfig['dataConfig'] if 'dataConfig' in formatConfig else []
    return dataConfig

def getProjectPath(formatConfig):
    projectFolder = formatConfig['projectPath'] if 'projectPath' in formatConfig else ''
    return projectFolder

def getProjectName(formatConfig):
    projectName = formatConfig['projectName'] if 'projectName' in formatConfig else ''
    return projectName

def getStartDate(formatConfig):
    startDate = formatConfig['analysisStartDate'] if 'analysisStartDate' in formatConfig else ''
    return startDate

def getEndDate(formatConfig):
    startDate = formatConfig['analysisEndDate'] if 'analysisEndDate' in formatConfig else ''
    return startDate

def getDataFiles(dataConfig):
    dataFiles = dataConfig['dataFiles'] if 'dataFiles' in dataConfig else []
    return dataFiles

def getDataColumns(dataConfig):
    dataColumns = dataConfig['mergeColumns'] if 'mergeColumns' in dataConfig else []
    return dataColumns

def getDataFileLocation(dataConfig):
    fileLocation = dataConfig['location'] if 'location' in dataConfig else ''
    return fileLocation

def getDataName(metadata):
    name = metadata['name'] if 'name' in metadata else ''
    return name

def getFileFrequency(metadata):
    fileFrequency = metadata['fileFrequency'] if 'fileFrequency' in metadata else ''
    return fileFrequency

def getIndexFrequency(metadata):
    indexFrequency = metadata['indexFrequency'] if 'indexFrequency' in metadata else ''
    return indexFrequency

def getDataColumns(metadata):
    dataColumns = metadata['dataColumns'] if 'dataColumns' in metadata else []
    return dataColumns

def getYearFromDate(date):
    year = datetime.strptime(date, '%Y-%m-%d').strftime('%Y')
    year = int(year)
    return year

# calculate number of period in the start and end date in the given frequncy
# if it is hourly frequncy then number of day in the start and end date is multipled with 24
def getTimeSeriesPeriod(startDate, endDate, frequency):
    startDate = datetime.strptime(startDate, "%Y-%m-%d")
    endDate = datetime.strptime(endDate, "%Y-%m-%d")
    multiplier = 1
    if frequency == 'H':
        multiplier = 24
    timeSeriesperiod = (abs((endDate - startDate).days) + 1) * multiplier
    return timeSeriesperiod
    
    
# generate a series of date array of given frequency 
# data generated in this way to avoid missing dates in the downlaoded data set
# frequency 'H' defines hourly, 'D' defines daily
def generateTimeSeries(strStartDate, strEndDate, frequency):
    period = getTimeSeriesPeriod(strStartDate, strEndDate, frequency)
    timeSeries = pd.date_range(strStartDate, periods=period, freq=frequency)
    return timeSeries

def createInitialDataFrame(df, startDate, endDate, metadata):
    
    fileFrequency = getFileFrequency(metadata)
    indexFrequency = getIndexFrequency(metadata)
    dataColumns = getDataColumns(metadata)
    otherIndexColumns = []
    dateIndex = []
    if indexFrequency == 'hourly':
        dateIndex = generateTimeSeries(startDate, endDate, 'H')
    elif indexFrequency == 'daily':
        dateIndex = generateTimeSeries(startDate, endDate, 'D')
    
    otherIndexColumns.append(dateIndex)
    indexDTypes = []
    if len(df.index.names) > 1:
        indexDTypes = df.index.dtypes
    elif len(df.index.names) == 1:
        indexDTypes = df.index.dtype
    # indexDTypes = df.index.dtypes
    print('--------------')
    print(indexDTypes)
    print('--------------')
    dateIndexColumn = ''
    
    for i in range(len(indexDTypes)):
        indexColumn = df.index.names[i]
        if indexDTypes[i] == 'datetime64[ns]':
            dateIndexColumn = indexColumn
        else:
            otherIndexColumns.append(sorted(set(list(df.index.get_level_values(indexColumn)))))
    print(otherIndexColumns)
    timeIndex = pd.MultiIndex.from_product(otherIndexColumns, 
                                          names=df.index.names)
    tempDf = pd.DataFrame(index=timeIndex)
    for column in dataColumns:
        tempDf[column] = np.nan
    
    return tempDf

def getProcessSaveLocation(projectFolder, folderName):
    loadDataSaveLocation = os.path.join(projectFolder, 'data_formatting', folderName)
    return loadDataSaveLocation


def getYearFromDataFrame(df):
    try:
        dataSet = df.index[0]
        print(dataSet)
        for data in dataSet:
            try:
                year = data.year
                return year
            except:
                error = 'cannot extract year from the data'
    except:
        print('no data available in dataframe')
    return None
        
def mergeData(metadata):
    dataFiles = getDataFiles(metadata)
    fileFrequency = getFileFrequency(metadata)
    dataName = getDataName(metadata)
    cleanDataLocation = metadata['cleanDataLocation']
    for dataFile in dataFiles:
        dataLocation = getDataFileLocation(dataFile)
        dataColumns = getDataColumns(dataFile)
        if dataLocation != '':
            try:
                dataLocation = os.path.normpath(dataLocation)
            except:
                print('File Location does not exist')
            for subdir, dirs, files in os.walk(dataLocation):
                for file in files:
                    fileName = os.path.join(subdir, file)
                    fileData = fop.readH5File(fileName)
                    if fileFrequency == 'yearly':
                        year = getYearFromDataFrame(fileData)
                        if year is not None:
                            cleanFileName = str(year)+'.h5'
                            fileLocation = os.path.join(cleanDataLocation, cleanFileName)
                            cleanData = fop.readH5File(fileLocation)
                            for dataColumn in dataColumns:
                                cleanData = np.where( cleanData == float('NaN'), fileData[dataColumn], cleanData)
                                
                            fop.saveToHDFFile(cleanData, fileLocation)
                            print('file saved: ', cleanFileName)
                    elif fileFrequency == 'single':
                        cleanFileName = dataName+'.h5'
                        fileLocation = os.path.join(cleanDataLocation, cleanFileName)
                        cleanData = fop.readH5File(fileLocation)
                        for dataColumn in dataColumns:
                            cleanData = np.where( cleanData == float('NaN'), fileData[dataColumn], cleanData)
                            
                        fop.saveToHDFFile(cleanData, fileLocation)
                        print('file saved: ', cleanFileName)
        else:
            print('Invalid File Location')
            
    
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
         
    print('Begin Execution: Clean and Format Data')
    for metadata in dataConfig:
        dataFiles = getDataFiles(metadata)
        fileData = pd.DataFrame()
        for dataFile in dataFiles:
            dataLocation = getDataFileLocation(dataFile)
            if dataLocation != '':
                try:
                    dataLocation = os.path.normpath(dataLocation)
                except:
                    print('File Location does not exist')
                for subdir, dirs, files in os.walk(dataLocation):
                    for file in files:
                        fileName = os.path.join(subdir, file)
                        fileData = fop.readH5File(fileName)
                        break
                    break
            else:
                print('Invalid File Location')
                
            break
        
        dataName = getDataName(metadata)
        fileFrequency = getFileFrequency(metadata)
        indexFrequency = getIndexFrequency(metadata)
        dataColumns = getDataColumns(metadata)
        dataSaveLocation = getProcessSaveLocation(projectFolder, dataName)
        os.makedirs(dataSaveLocation, exist_ok = True)
        metadata['cleanDataLocation'] = dataSaveLocation
        if fileFrequency == 'yearly':
            for year in analysisPeriod:
                startDateYearly = str(year)+'-01-01'
                endDateYearly = str(year)+'-12-31'
                initDataFrame = createInitialDataFrame(fileData, startDateYearly, endDateYearly, metadata)
                fileName = str(year)+'.h5'
                saveFileLocation = os.path.join(dataSaveLocation, fileName)
                fop.saveToHDFFile(initDataFrame, saveFileLocation)
                print('save location', saveFileLocation, initDataFrame)
                
            mergeData(metadata)
        elif fileFrequency == 'single':
            print('single file')
            initDataFrame = createInitialDataFrame(fileData, startDateYearly, endDateYearly, metadata)
            fileName = dataName + '.h5'
            saveFileLocation = os.path.join(dataSaveLocation, fileName)
            fop.saveToHDFFile(initDataFrame, saveFileLocation)
            print('save location', saveFileLocation, initDataFrame)
            mergeData(metadata)
        # print(metadata)
    
    