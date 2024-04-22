import derive
import sys
import re
import sqlite3
import updateDatabase
import utils

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au

infile = open(sys.argv[1], "r")
citation_id = sys.argv[2]
delim = sys.argv[3]
commit = sys.argv[4] 
if(commit == "1"):
    commit = True
else:
    commit = False

con = utils.connect_db()

for line in infile:
    if(re.search("GROUP",line)):
        splitted = line.split(delim)
        if(len(splitted)!=3):
            print("Input file format not correct")
            exit()
        linkedSetDesc = (splitted[1]).strip()
        linkedSetType = (splitted[2]).strip()
        linkedSet_id = None

        if(int(linkedSetType) == 1):
            print("Adding new human data set for 1 pulsar")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Human input timing model"
            updateDatabase.addTimingModel(sys.argv[1],citation_id,linkedSetDesc,delim=delim,commit=commit)
        elif(int(linkedSetType) == 2):
            print("Adding Par File")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Par file input"
            print(linkedSetDesc)
            updateDatabase.addParFile(sys.argv[1],citation_id,linkedSetDesc,commit=commit)
        elif(int(linkedSetType) == 3):
            print("Adding Glitch")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Glitch input"
        elif(int(linkedSetType) == 4):
            print("Column Based Tabular Input")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Tabular input"
            updateDatabase.addTabular(sys.argv[1],citation_id,delim=delim,commit=commit)
        else:
            print("Group Type Unknown")
            print("---")
            exit()
    else:
        exit()

