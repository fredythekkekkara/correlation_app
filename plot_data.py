# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 09:28:01 2022

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
import matplotlib.pyplot as plt
import matplotlib as mpl
from ast import literal_eval
from math import ceil, floor

process = 'plot_data'
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
    fileLocation = dataConfig['filePath'] if 'filePath' in dataConfig else ''
    return fileLocation

def getDataName(metadata):
    name = metadata['name'] if 'name' in metadata else ''
    return name

def getProcessSaveLocation(folderName):
    loadDataSaveLocation = os.path.join(projectFolder, process, folderName)
    return loadDataSaveLocation

def getDataFiles(metadata):
    dataFiles = metadata['dataFiles'] if 'dataFiles' in metadata else []
    return dataFiles

def getPlotType(metadata):
    plotType = metadata['type'] if 'type' in metadata else ''
    return plotType

def getFigureHeight(metadata):
    height = metadata['height'] if 'height' in metadata else 0
    return height

def getFigureWidth(metadata):
    width = metadata['width'] if 'width' in metadata else 0
    return width

def getXAxis(metadata):
    xaxis = metadata['xAxis'] if 'xAxis' in metadata else ''
    try:
        xaxis = literal_eval(xaxis)
    except:
        print('_____')
        
    return xaxis

def getYAxis(metadata):
    yaxis = metadata['yAxis'] if 'yAxis' in metadata else ''
    try:
        yaxis = literal_eval(yaxis)
    except:
        print('_____')
        
    return yaxis

def getColorValue(metadata):
    color = metadata['color'] if 'color' in metadata else ''
    return color

def getLowerFile(metadata):
    file = metadata['lowerLimitFile'] if 'lowerLimitFile' in metadata else ''
    return file


def getUpperFile(metadata):
    file = metadata['upperLimitFile'] if 'upperLimitFile' in metadata else ''
    return file

def getLabel(metadata):
    label = metadata['label'] if 'label' in metadata else ''
    return label

def getXAxisLabel(metadata):
    xaxis = metadata['xLabel'] if 'xLabel' in metadata else ''
    return xaxis

def getYAxisLabel(metadata):
    yaxis = metadata['yLabel'] if 'yLabel' in metadata else ''
    return yaxis

def getZAxisLabel(metadata):
    zaxis = metadata['zLabel'] if 'zLabel' in metadata else ''
    return zaxis

def getTitle(metadata):
    title = metadata['title'] if 'title' in metadata else ''
    return title
    
def getNumberOfColors(metadata):
    numberOfColors = metadata['numberOfColors'] if 'numberOfColors' in metadata else 0
    return numberOfColors

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


def clipDataByDate(data, startDate, endDate):
    startDate = pd.to_datetime(startDate).date()
    endDate = pd.to_datetime(endDate).date()
    dateIndexColumn = getDateIndexColumn(data)
    # data = data.loc[startDate:endDate]
    data = data[(data.index.get_level_values(dateIndexColumn) >= startDate) & (data.index.get_level_values(dateIndexColumn) <= endDate)]
    # data = data[(pd.to_datetime(data.index.get_level_values(dateIndexColumn)).date >= startDate) & (pd.to_datetime(data.index.get_level_values(dateIndexColumn)).date <= endDate)]
    return data

def findBounds(a, b):
    boundValue = a if a > b else b
    if a < 0:
        a = a * -1
    if b < 0:
        b = b* -1
    boundValue = a if a > b else b
    bounds = np.linspace(-boundValue,boundValue,10)
    print(boundValue)
    return bounds

def getCorrelationBounds(maxValue, minValue):
    print(maxValue, minValue)
    maxValue = maxValue + 0.1
    minValue = minValue - 0.1
    maxValue = round(maxValue, 1)
    minValue = round(minValue, 1)
    # print(maxValue, minValue)
    bounds = findBounds(minValue, maxValue)
    # bounds = np.linspace(minValue,maxValue,10)
    # bounds = np.linspace(-1,+1,20)
    
    return bounds

def getDataClippingEnabled(metadata):
    isDataClippingEnabled = metadata['clipDataByDate'] if 'clipDataByDate' in metadata else False
    if isDataClippingEnabled == 1:
        isDataClippingEnabled = True
    else:
        isDataClippingEnabled = False
    return isDataClippingEnabled


def getColorMap(metadata):
    colorMap = metadata['colorMap'] if 'colorMap' in metadata else 'seismic'
    return colorMap


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
        name = getDataName(metadata)
        title = getTitle(metadata)
        dataFiles = getDataFiles(metadata)
        plotType = getPlotType(metadata)
        figHeight = getFigureHeight(metadata)
        figWidth = getFigureWidth(metadata)
        xAxisLabel = getXAxisLabel(metadata)
        yAxisLabel = getYAxisLabel(metadata)
        zAxisLabel = getZAxisLabel(metadata)
        isDataClippingEnabled = getDataClippingEnabled(metadata)
        colorMap = getColorMap(metadata)
        numberOfColors = getNumberOfColors(metadata)
        saveLocation = getProcessSaveLocation(name)
        os.makedirs(saveLocation, exist_ok = True)
        fig = graph = None 
        for dataFile in dataFiles:
            label = getLabel(dataFile)
            dataPlotType = getPlotType(dataFile)
            if dataPlotType == 'line':
                if fig == None and graph == None:
                    fig, graph = plt.subplots(figsize=(figWidth, figHeight))
                dataFileLocation = getDataFileLocation(dataFile)
                xAxisColumn = getXAxis(dataFile)
                yAxisColumn = getYAxis(dataFile)
                data = fop.readH5File(dataFileLocation)
                data = clipDataByDate(data, startDate, endDate)
                data = data.reset_index()
                print(data)
                xAxis = data[xAxisColumn]
                yAxis = data[yAxisColumn]
                color = getColorValue(dataFile)
                graph.plot(xAxis, yAxis, linewidth=1, color= color, label=label)
                plt.grid(True)
                plt.legend(loc="upper left")
            elif dataPlotType == 'fillBetween':
                if fig == None and graph == None:
                    fig, graph = plt.subplots(figsize=(figWidth, figHeight))
                lowerFile = getLowerFile(dataFile)
                upperFile = getUpperFile(dataFile)
                xAxisColumn = getXAxis(dataFile)
                yAxisColumn = getYAxis(dataFile)
                lowerData = fop.readH5File(lowerFile)
                lowerData = clipDataByDate(lowerData, startDate, endDate)
                upperData = fop.readH5File(upperFile)
                upperData = clipDataByDate(upperData, startDate, endDate)
                lowerData = lowerData.reset_index()
                upperData = upperData.reset_index()
                
                xAxis = lowerData[xAxisColumn]
                
                yAxisLower = lowerData[yAxisColumn]
                print(yAxisLower)
                yAxisUpper = upperData[yAxisColumn]
                color = getColorValue(dataFile)
                
                graph.fill_between(xAxis, yAxisLower, yAxisUpper, alpha=.5, linewidth=0, color='gray', label=label)
                plt.grid(True)
                plt.legend(loc="upper left")
            elif dataPlotType == 'colorBar':
                yAxisColumn = getYAxis(dataFile)
                if fig == None:
                    fig = plt.figure(figsize=(figWidth, figHeight), dpi=200)
                dataFileLocation = getDataFileLocation(dataFile)
                data = fop.readH5File(dataFileLocation)
                if isDataClippingEnabled:
                    data = clipDataByDate(data, startDate, endDate)
                print('________________________RAW DATA___________________', title)
                print(data)   
                print('________________________END___________________')
                data = data.T
                print('________________________ DATA T ___________________')
                print(data)   
                print('________________________END___________________')
                data = data.sort_index(axis=0, ascending=False)
                indexValues = data.index.values 
                #print(indexValues)
                if yAxisColumn != "":
                    indexValues = data.index.get_level_values(yAxisColumn)
                tiks = range(0,len(indexValues),2)
                #tiks = range(0,len(indexValues),8)
                lbl = indexValues[tiks]
                bounds = getCorrelationBounds(data.max().max(), data.min().min()) 
                
                x_tiks = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]#range(0, 366, 30)
                x_bl = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 
                        'August', 'September', 'October', 'November', 'December']
                cmaps = mpl.cm.get_cmap(colorMap)   
                if numberOfColors > 0:
                    cmaps = mpl.cm.get_cmap(colorMap, numberOfColors)
    
                if graph == None:
                    graph = plt.imshow(data, cmap = cmaps, vmin = bounds.min(), vmax=bounds.max(), aspect='auto')
                    #graph = plt.imshow(data, cmap = colorMap, vmin = bounds.min(), vmax=bounds.max())
                    
                
                cb = plt.colorbar(graph, shrink = 0.5, boundaries=bounds)
                cb.set_label(zAxisLabel)
                plt.yticks(ticks=tiks, labels=lbl)
                plt.xticks(ticks=x_tiks, labels=x_bl, rotation=45)
                ##print(dataPlotType)
                
            elif dataPlotType == 'contour':
                if fig == None:
                    fig = plt.figure(figsize=(figWidth, figHeight), dpi=200)
                dataFileLocation = getDataFileLocation(dataFile)
                data = fop.readH5File(dataFileLocation)
                if isDataClippingEnabled:
                    data = clipDataByDate(data, startDate, endDate)
                
                data = data.T
                
                data = data.sort_index(axis=0, ascending=False)
                indexValues = data.index.values
                if yAxisColumn != "":
                    indexValues = data.index.get_level_values(yAxisColumn)
                if graph == None:
                    graph = plt.imshow(data, cmap = colorMap, vmin = bounds.min(), vmax=bounds.max(), aspect='auto')
                else:
                    contour = plt.contour(data, levels= 1, colors='black')
                    plt.clabel(contour, inline=True, fontsize=8)
                    
                
                
        # graph.set_xlabel(xAxisLabel)
        # graph.set_ylabel(yAxisLabel)
        # graph.set_title(name)
        
        plt.xlabel(xAxisLabel)
        plt.ylabel(yAxisLabel)
        plt.title(title)
            
        fileName = name
        saveFileLocation = os.path.join(saveLocation, fileName) 
        fig.savefig(saveFileLocation, bbox_inches='tight', dpi=150)
        plt.clf()
           
            