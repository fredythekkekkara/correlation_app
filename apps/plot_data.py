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
from ast import literal_eval

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

    
# def plotGraph():
    
#     fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(12, 8))

    
#     ax1.fill_between(x, y1_corr_f10, y2_corr_f10, alpha=.5, linewidth=0, color='gray', label='95% confidence interval')
#     ax1.plot(x, corr_f10, linewidth=1, label='cross correlation tec x f10.7')
#     ax1.plot(x, g_corr_f10, linewidth=1, color='r', label='guassian window normalisation')
#     plt.grid(True)
#     ax1.set(ylim=(-1, 1))
#     ax1.set_title('cross correlation tec X f10.7')
#     ax1.set_xlabel('day of year')
#     ax1.legend(loc="lower left")
    
#     ax2.fill_between(x, y1_corr_sws, y2_corr_sws, alpha=.5, linewidth=0, color='gray', label='95% confidence interval')
#     ax2.plot(x, corr_sws, linewidth=1, label='cross correlation tec x solar wind')
#     ax2.plot(x, g_corr_sws, linewidth=1, color='r', label='guassian window normalisation')
#     ax2.set(ylim=(-1, 1))
#     ax2.set_title('cross correlation tec X solar wind speed')
#     ax2.set_xlabel('day of year')
#     fig.tight_layout()
    
#     plt.legend(loc="upper left")
#     plt.show()
#     return True


# latitude = 60


# corr_tec_f10_7 = readH5File(_corr_tec_x_f10_7_file_path)
# norm_corr_tec_f10_7 = readH5File(_corr_norm_tec_x_f10_7_file_path)
# corr_up_limit_tec_f10_7 = readH5File(_corr_tec_f10_7_upper_c_of_interval)
# corr_low_limit_tec_f10_7 = readH5File(_corr_tec_f10_7_lower_cof_interval)

# corr_tec_f10_7 = corr_tec_f10_7[latitude]
# norm_corr_tec_f10_7 = norm_corr_tec_f10_7[latitude]
# corr_up_limit_tec_f10_7 = corr_up_limit_tec_f10_7[latitude]
# corr_low_limit_tec_f10_7 = corr_low_limit_tec_f10_7[latitude]

# df = pd.DataFrame()
# df['original'] = corr_tec_f10_7
# df['normalised'] = norm_corr_tec_f10_7
# df['upper_limit'] = corr_up_limit_tec_f10_7
# df['lower_limit'] = corr_low_limit_tec_f10_7
# plotGraph(df)



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
                if fig == None:
                    fig = plt.figure(figsize=(figWidth, figHeight), dpi=200)
                dataFileLocation = getDataFileLocation(dataFile)
                data = fop.readH5File(dataFileLocation)
                data = data.T
                print(data)
                
                indexValues = data.index.values
                print(indexValues)
                tiks = range(0,len(indexValues),10)
                lbl = indexValues[tiks]
                
                x_tiks = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]#range(0, 366, 30)
                x_bl = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 
                        'August', 'September', 'October', 'November', 'December']
    
                if graph == None:
                    graph = plt.imshow(data, cmap='hot')
                cb = plt.colorbar(graph, shrink = 0.5)
                cb.set_label(zAxisLabel)
                plt.yticks(ticks=tiks, labels=lbl)
                plt.xticks(ticks=x_tiks, labels=x_bl, rotation=45)
                print(dataPlotType)
                
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
           
            