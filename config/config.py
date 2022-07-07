# Available configurations of the project

from enum import Enum

class processSteps(Enum):
    loadData = 'Load Data'
    dataConfiguration = 'Data Configuration'
    dataForamtting = "Data Cleaning & Formatting"
    computations = "Computations"
    dataVisualisation = "Data Visualisation"

class fileTypes(Enum):
    text = '.txt'
    csv = '.csv'
    hdf = '.hdf'
    cdf = '.cdf'
    wdc = '.wdc'
    
    
class dateFormat(Enum):
    yyyymmdd = '%Y-%m-%d'
    yyyymmdd_2 = '%Y%m%d'
    yyyyj = '%Y  %j'
   
    
class fileResolution(Enum):
    singleFile = 'Single File'
    dailyFile = 'Daily File'
    yearlyFile = 'Yearly File'
    
class plotType(Enum):
    line = 'Line'
    fillBetween = 'Fill Between'
    colorBar = 'Color Bar'

    