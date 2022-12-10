# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 08:54:27 2022

@author: davi_fr
"""
import sys
import os
import json

if __name__ == "__main__":

    if len(sys.argv) != 3:
        sys.exit("input error.\n init.py project_name project_location")
    
    projectName =  sys.argv[1]
    projectLocation =  sys.argv[2]
    
    
    initProject ={ 
      "projectName": projectName,
      "projectPath": projectLocation,
    } 
    
    path = os.path.join(projectLocation, projectName)
    os.mkdir(path)
    
    
    loadDataPath = os.path.join(path, 'load_data')
    os.mkdir(loadDataPath)
    loadDataConfigPath = os.path.join(loadDataPath, 'load_data_config.json')
    with open(loadDataConfigPath, 'w') as f:
            json_string=json.dumps(initProject, indent=4)
            f.write(json_string)
        
    
    dataFormattingPath = os.path.join(path, 'data_formatting')
    os.mkdir(dataFormattingPath)
    dataFormattingConfigPath = os.path.join(dataFormattingPath, 'data_formatting_config.json')
    with open(dataFormattingConfigPath, 'w') as f:
            json_string=json.dumps(initProject, indent=4)
            f.write(json_string)
            
    
    computationsPath = os.path.join(path, 'computations')
    os.mkdir(computationsPath)
    computationsConfigPath = os.path.join(computationsPath, 'computations_config.json')
    with open(computationsConfigPath, 'w') as f:
            json_string=json.dumps(initProject, indent=4)
            f.write(json_string)
    
    plotDataPath = os.path.join(path, 'plot_data')
    os.mkdir(plotDataPath)
    plotDataConfigPath = os.path.join(plotDataPath, 'plot_config.json')
    with open(plotDataConfigPath, 'w') as f:
            json_string=json.dumps(initProject, indent=4)
            f.write(json_string)
    
    
    
    print("Project {} initialised".format(projectName))