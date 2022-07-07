import streamlit as st
from multiapp import MultiApp
from apps import home, data, model # import your app modules here
import sys
from config.config import processSteps, fileResolution, fileTypes, plotType
import os
from general_functions import files, metadata
from general_functions import constants as c

from datetime import datetime

import numpy as np
app = MultiApp()

# st.markdown("""
# # Multi-Page App

# This multi-page app is using the [streamlit-multiapps](https://github.com/upraneelnihar/streamlit-multiapps) framework developed by [Praneel Nihar](https://medium.com/@u.praneel.nihar). Also check out his [Medium article](https://medium.com/@u.praneel.nihar/building-multi-page-web-app-using-streamlit-7a40d55fa5b4).

# """)

# # Add all your application here
# app.add_app("Home", home.app)
# app.add_app("Data", data.app)
# app.add_app("Model", model.app)
# # The main app
# app.run()
processOptions = [step.value for step in processSteps]
plotTypes = [plot_type.value for plot_type in plotType]
st.sidebar.title('Project')
projectName = st.sidebar.text_input('Project Name', value='', key='projectName')
projectlocation = st.sidebar.text_input('Project Location', value='', key='projectlocation')
# createProject = st.button('Create Project', key='createProject')
# if createProject:
#     st.caption('Project Created')
st.sidebar.title('Process Line')
processLine = st.sidebar.selectbox('', processOptions)


loadMetadata = []
fileExt = ''
if processLine == processSteps.loadData.value:
    st.title(processLine)
    numberOfFileDirectories = st.number_input('Enter number of file directories', 
                                         min_value=1, 
                                         max_value=10, 
                                         step=1)
    loadMetadata = [metadata.metadata for n in range(numberOfFileDirectories)]
    for fileDir in range(numberOfFileDirectories):
        dirNum = fileDir + 1
        with st.expander("Parameter " + str(dirNum)):
            name = st.text_input('Name of parameter', value='', key='name'+str(dirNum))
            
            folderpath = st.text_input('File location', value='', key='location'+str(dirNum))
            
            # filelist = files.listFiles(location)
            # st.caption(filelist)
            
            
            # folderpath = r"C:\Users\davi_fr\Documents\Project_1\data\tec_1hr" # make sure to put the 'r' in front
            # filepaths  = [os.path.join(root,f) for root,dirs,files in os.walk(folderpath) for f in files]
            if folderpath != '':
                (fileExt, sampleFilePath) = files.getFileType(folderpath)
                
                     
                if fileExt == '':
                    st.caption('No files found in the directory')
                st.caption(fileExt)
                fileAttributes = []
                indexAttributes = []
                if fileExt == fileTypes.cdf.value:
                    sampleData = files.readCDFFile(sampleFilePath)
                    attributes = files.getCDFAttributes(sampleData)
                    fileAttributes = st.multiselect('Select data attributes to be extacted', 
                                                    attributes, 
                                                    key='fileAttributes'+str(dirNum))
                    
                    indexAttributes = st.multiselect('Select index attributes from selected data attributes', 
                                                    fileAttributes, 
                                                    key='indexAttributes'+str(dirNum))
                    
                    
            
                    # store user data into a variable
                    loadMetadata[fileDir][c._name] = name
                    loadMetadata[fileDir][c._fileLocation] = folderpath
                    loadMetadata[fileDir][c._fileType] = fileExt
                    loadMetadata[fileDir][c._attributes] = fileAttributes
                    loadMetadata[fileDir][c._indexAttributes] = indexAttributes
                elif fileExt == fileTypes.text.value:
                    numberOfAttributes = st.number_input('Enter number of attributes from data file to be extarcted', 
                                                         min_value=1, 
                                                         max_value=10, 
                                                         step=1)
                    attributeMetadata = [metadata.attributeMetadata for n in range(numberOfAttributes)]
                    
                    st.caption(attributeMetadata)
                    attrColumn = st.columns(numberOfAttributes)
                    attNum = 0
                    columnIndex = 0
                    for clm in attrColumn:
                        attNum = attNum + 1
                        
                        
                        clm.caption('Data Attribute ' + str(attNum))
                      
                        attributeName = clm.text_input('Name of attribute', value='', key='attName'+str(attNum))
                        attrStartPosition = clm.text_input('Start Position of the attribute', value='', key='attStartPos'+str(attNum))
                        attrEndPosition = clm.text_input('End Position of the attribute', value='', key='attEndPos'+str(attNum))
                        indexAttribute = clm.checkbox('Set as index', key='attIndex'+str(attNum))
                        
                        attributeMetadata[columnIndex][c._attributeName] = attributeName
                        attributeMetadata[columnIndex][c._startPosition] = attrStartPosition
                        attributeMetadata[columnIndex][c._endPosition] = attrEndPosition
                        attributeMetadata[columnIndex][c._isIndex] = indexAttribute
                         
                        st.caption(attributeMetadata)
                            
                            
                        columnIndex = columnIndex + 1
                     
                    # for i in range(numberOfAttributes):
                    #     ind = i+1
                    #     if used_widget_key == 'attName'+str(attNum):
                    #         attributeMetadata[columnIndex][c._attributeName] = attributeName
                            
                    loadMetadata[fileDir][c._name] = name
                    loadMetadata[fileDir][c._fileLocation] = folderpath
                    loadMetadata[fileDir][c._fileType] = fileExt
                    loadMetadata[fileDir][c._attributes] = attributeMetadata
                 
            
            
    loadFiles = st.button('Create Project & Load Files', key='loadFile')
    stopExec = st.button('Stop Execution', key='stopExec')
    if stopExec:
        exit()
    if loadFiles:
        # check project name and location are not empty
        if projectName == '' or projectlocation == '':
            st.error('Project name or project location cannot be empty')
        else:
            #check project location is valid
            if files.checkFolderPathIsValid(projectlocation):
                #folder path exist
                projectPath = os.path.join(projectlocation, projectName)
                projectStepPath = os.path.join(projectPath, 'load_data')
                projectLoadConfigFile = os.path.join(projectStepPath, 'load_data_config.json')
                try:
                    # create project folder in the specified project location
                    # os.mkdir(projectPath)
                    os.makedirs(projectStepPath, exist_ok = True)
                    
                    configData = {'projectName': projectName,
                                  'projectPath': projectPath,
                                  'dataConfig': loadMetadata}
                    
                    files.writeToJsonFile(projectLoadConfigFile, configData)
                    st.success('Project ' + projectName + ' Created')
                    os.system("python apps/load_data_files.py " + f'"{projectLoadConfigFile}"') 
                    
                except OSError as error:
                    # if error.errno == 17:
                    #     # Project Folder Already Exist
                    #     st.caption('Project ' + projectName + ' already exist')
                    st.error(error)
            else:
                st.error('Project location not available. Try a valid project location')
        
                   
            
elif processLine == processSteps.dataConfiguration.value:
    st.title(processLine)
    startDate = st.date_input('Start date of anlysis', min_value=datetime(1995, 1, 1))
    endDate = st.date_input('End date of anlysis', min_value=datetime(1995, 1, 1))
    numberOfParameters = st.number_input('Enter number of solar forcing parameters', 
                                         min_value=2, 
                                         max_value=10, 
                                         step=1)
    for params in range(numberOfParameters):
        with st.expander("Parameter " + str(params + 1)):
            name = st.text_input('Name of parameter', value='', key='name'+str(params+1))
            location = st.text_input('File location', value='', key='location'+str(params+1))
            # filelist = files.listFiles(location)
            # st.caption(filelist)
            fileResolutions = [resolution.value for resolution in fileResolution]
            selectedFileResolution = st.radio('Temporal resolution of file', 
                                              fileResolutions, 
                                              key='resolution'+str(params+1))
            
elif processLine == processSteps.dataVisualisation.value:
    st.title(processLine)   
    startDate = st.date_input('Start date of anlysis', min_value=datetime(1995, 1, 1))
    endDate = st.date_input('End date of anlysis', min_value=datetime(1995, 1, 1))
    plotNums = 1
    for params in range(plotNums):
        with st.container():
            st.header('Plot Configuration'+str(params+1))
            name = st.text_input('Plot Name', value='', key='plot'+str(params+1))
            location = st.file_uploader('Select a File')
            with st.expander("Plot " + str(params + 1)):
                plotTypeSelector = st.selectbox('', plotTypes)
                if plotTypeSelector == plotType.line.value:
                    location = st.text_input('File location', value='', key='plotFileLoc'+str(params+1))
    
if processLine == processSteps.dataForamtting.value:
    st.error('Wrong choice')