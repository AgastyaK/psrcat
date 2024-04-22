#!/usr/bin/env python

import utils
import numpy as np
import pandas as pd
import sqlite3
import sys
import argparse
import warnings
import derive
warnings.simplefilter(action='ignore', category=FutureWarning)

def getTimingModel(con,psrname):
    cur = con.execute("PRAGMA foreign_keys = 1")
    pulsarIDtemp = cur.execute("SELECT DISTINCT pulsar_id FROM pulsar WHERE jname LIKE ?",(psrname,))
    pulsarIDinDB = pulsarIDtemp.fetchone()
    if pulsarIDinDB:
        pulsar_id = pulsarIDinDB[0]
    else:
        print("No pulsar with name %s in the database" %(psrname))
        return None 

    paramquery = "SELECT label,value,uncertainty,referenceTime,timeDerivative,companionNumber,parameter_id,fitParameters_id,units,ephemeris,clock FROM viewParameter WHERE (pulsar_id=%s AND timingFlag=1 AND value IS NOT NULL)" % (pulsar_id)
    rmquery = "SELECT label,value,uncertainty,referenceTime,timeDerivative,companionNumber,parameter_id FROM viewParameter WHERE (pulsar_id=%s AND label='RM' AND value IS NOT NULL)" % (pulsar_id)

    param = pd.read_sql_query(paramquery,con)
    rm = pd.read_sql_query(rmquery,con)
    lookup = pd.read_csv('lookupParameters',sep=';')
    
    params = pd.concat([param,rm],ignore_index=True,sort=False)

    params = params.replace({np.nan: ' '})
    lookup = lookup.replace({np.nan: ' '})

    for index, row in params.iterrows():

        oldlabel = params.loc[ (params['label']==row['label']) &
                               (params['timeDerivative']==row['timeDerivative']) &
                               (params['companionNumber']==row['companionNumber'])
        ]

        if(len(oldlabel.index.values) > 1):
            newlist = oldlabel.sort_values(by=['parameter_id'],ascending=False)
            mylist = newlist.index.values
            params.drop(mylist[1:],inplace=True)
            
    outstr = ""
    outstr = outstr+'{:<16s}{:}\n'.format("PSRJ",psrname)

    for index, row in params.iterrows():

        oldlabel = lookup.loc[ (lookup['newlabel']==row['label']) &
                               (lookup['timeDerivative']==row['timeDerivative']) &
                               (lookup['companionNumber']==row['companionNumber'])
                              ]['oldlabel'].values
        if(len(oldlabel)==0):
            continue

	# Fix uncertainty conversion for RAJ and DECJ from degrees to uncertainty in hh/mm/ss
        outstr = outstr+'{:<16s}{:<30s}{:}\n'.format(oldlabel[0],row['value'],row['uncertainty'])    

        if(oldlabel[0] == 'F0' or oldlabel[0] == 'P0'):
            if(row['units'] != 'unknown' and row['units'] != ' '):
                outstr = outstr+'{:<16s}{:}\n'.format("UNITS",row['units'])
                if((row['ephemeris'] != 'unknown') and (row['ephemeris'] != ' ')):
                    outstr = outstr+'{:<16s}{:}\n'.format("EPHEM",row['ephemeris'])
                if(row['clock'] != 'unknown' and row['clock'] != ' '):
                    outstr = outstr+'{:<16s}{:}\n'.format("CLK",row['clock'])
                if(row['referenceTime'] != ' '):
                    outstr = outstr+'{:<16s}{:}\n'.format("PEPOCH",row['referenceTime'])
        if(oldlabel[0] == 'DM'):
            if(row['referenceTime'] != ' '):
                outstr = outstr+'{:<16s}{:}\n'.format("DMEPOCH",row['referenceTime'])        
        if(oldlabel[0] == 'DECJ' or oldlabel[0] == 'ELAT'):
            if(row['referenceTime'] != ' '):
                outstr = outstr+'{:<16s}{:}\n'.format("POSPOCH",row['referenceTime'])      

    print(outstr.strip())

if __name__ == "__main__":
    warnings.simplefilter(action='ignore', category=FutureWarning)
    parser = argparse.ArgumentParser(description="Run psrcat2.db python interface")
    parser.add_argument("-c", "--parameters", type=str,dest="params", help = "Parameters that you want. Default is P0,DM")
    parser.add_argument("-E", "--ephemeris",action='store_true',dest="wantephem", help = "Get timing models for pulsars. Default is all pulsars")
    parser.add_argument("-db", "--database",type=str,dest="database", help = "Choose database to use. Default is psrcat2.db")
    parser.add_argument("-psrname", "--pulsars",type=str,dest="pulsarlist", help ="List of pulsars. Default is all pulsars")
    args, unknown = parser.parse_known_args()

    if args.pulsarlist:
        psrlist = unknown + args.pulsarlist.split(' ')
    else:
        psrlist = unknown

    if args.database is None:
        db = "psrcat2.db"
    else:
        db = args.database
                       
    con = utils.connect_db(db)

    if args.wantephem:
        if(len(psrlist)==0):
            dataframe = pd.read_sql_query("select * from pulsar",con)
            psrlist = dataframe['jname'].values
        for idx, psrname in enumerate(psrlist):
            if(1 <= idx < len(psrlist)):
                print("@-----------------------------------------------------------------")
            getTimingModel(con,psrname)
        exit()
