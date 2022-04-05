# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 16:59:15 2022

@author: davi_fr
"""

from datetime import datetime
import os


def writeToLogFile(log):
    dt = datetime.now()
    log = str(dt) + '\t' + log + '\n' 
    file_name = 'log_file.txt'
    f = open(file_name, 'a+')  # open file in append mode
    f.write(log)
    f.close()

def log(message):
    writeToLogFile(message)
#     print(message)