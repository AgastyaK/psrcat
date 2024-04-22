import utils
import derive
import sqlite3
import pandas as pd
import numpy as np
import pygedm
from functools import reduce

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au

con = utils.connect_db()

mydb = pd.read_sql_query

ymw_citation_id = 4 #Change this to not be hard coded
ne_citation_id = 5 #Change this to not be hard coded

cur = con.cursor()

# Remove derived parameters from the parameter table
cur.execute("DELETE FROM parameter where parameter_id IN (SELECT parameter_id FROM derived)")

# Clear derived and derived to parameter table
cur.execute("DELETE FROM derived")
cur.execute("DELETE FROM derivedFromParameter")

# Clear distance table
cur.execute("DELETE FROM distance WHERE label IN ('DIST_YMW16','DIST_NE2001')")

# Commit deletions
con.commit()

# Derivation methods

methodVersion = "1.0"

p0_to_f0_method = "f0=1/p0, f0_err=p0_err*f0*f0"
f0_to_p0_method = "p0=1/f0, p0_err=f0_err*p0*p0"
p1_to_f1_method = "f1=-1*f0*f0*p1, f1_err=((f0*f0*p1_err)**2+(2*f0*f0*p1*p1_err)**2)**(0.5)"
f1_to_p1_method = "p1=-1*p0*p0*f1, p1_err=((p0*p0*f1_err)**2+(2*p0*p0*f1*f1_err)**2)**(0.5)"


dm_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'DM',uncertainty as 'DM_ERR',max(parameter_id) AS DM_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='DM' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
raj_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'RAJ',uncertainty as 'RAJ_ERR',max(parameter_id) AS RAJ_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='RAJ' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
decj_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'DECJ',uncertainty as 'DECJ_ERR',max(parameter_id) AS DECJ_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='DECJ' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
elong_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'ELONG',uncertainty as 'ELONG_ERR',max(parameter_id) AS ELONG_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='ELONG' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
elat_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'ELAT',uncertainty as 'ELAT_ERR',max(parameter_id) AS ELAT_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='ELAT' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
assoc_query = "SELECT association.pulsar_id AS pulsar_id,name as 'ASSOC' FROM association WHERE name IN ('SMC','LMC') GROUP BY association.pulsar_id"

p0_query = "SELECT viewParameter.pulsar_id AS pulsar_id,viewParameter.[parameter.citation_id] AS P0_citation_id,fitParameters_id AS P0_fitParameters_id,observingSystem_id AS P0_observingSystem_id,timeDerivative AS P0_timeDerivative,companionNumber AS P0_companionNumber,value as 'P0',uncertainty as 'P0_ERR',referenceTime AS P0_MJD,max(parameter_id) AS P0_parameter_id FROM viewParameter WHERE (viewParameter.label='P' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id ORDER BY viewParameter.pulsar_id ASC"

f0_query = "SELECT viewParameter.pulsar_id AS pulsar_id,viewParameter.[parameter.citation_id] AS F0_citation_id,fitParameters_id AS F0_fitParameters_id,observingSystem_id AS F0_observingSystem_id,timeDerivative AS F0_timeDerivative,companionNumber AS F0_companionNumber,value as 'F0',uncertainty as 'F0_ERR',referenceTime AS F0_MJD,max(parameter_id) AS F0_parameter_id FROM viewParameter WHERE (viewParameter.label='F' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id ORDER BY viewParameter.pulsar_id ASC"

#f0_query = "SELECT jname AS F0_JNAME,pulsar.pulsar_id AS pulsar_id,value as 'F0',uncertainty as 'F0_ERR',referenceTime AS F0_MJD,max(parameter_id) AS F0_parameter_id, viewParameter.[parameter.citation_id] AS F0_citation_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='F' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"

flux_query = "SELECT pulsar.pulsar_id AS pulsar_id,value as 'FLUX',max(parameter_id) AS FLUX_parameter_id FROM pulsar LEFT JOIN viewParameter ON pulsar.pulsar_id=viewParameter.pulsar_id WHERE (viewParameter.label='FLUX' and timeDerivative=0 AND value IS NOT NULL) GROUP BY pulsar.pulsar_id"
#dist_query =

dm = pd.read_sql_query(dm_query,con)
raj = pd.read_sql_query(raj_query,con)
decj = pd.read_sql_query(decj_query,con)
elong = pd.read_sql_query(elong_query,con)
elat = pd.read_sql_query(elat_query,con)
assoc = pd.read_sql_query(assoc_query,con)

p0 = pd.read_sql_query(p0_query,con)
f0 = pd.read_sql_query(f0_query,con)


#Merging dataframe code adapted from https://stackoverflow.com/questions/44327999/how-to-merge-multiple-dataframes
dmdist_data_frames = [dm,raj,decj,elong,elat,assoc]
dmdist_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pulsar_id'],how='outer'), dmdist_data_frames)


### Derive DIST_DMs

# for index, row in dmdist_merged.iterrows():
#     pulsar_id = row['pulsar_id']
#     dm = float(row['DM'])
#     raj = row['RAJ']
#     decj = row['DECJ']
#     elong = row['ELONG']
#     elat = row['ELAT']
    
#     if(pd.isna(dm)):
#         continue

#     if(pd.notna(raj) and pd.notna(decj)):

#         rajd = derive.hhmmss_to_degrees(raj)
#         decjd = derive.ddmmss_to_degrees(decj)
#         galactic = derive.equatorial_to_galactic(rajd,decjd)
#         gl = float(galactic.l.value)
#         gb = float(galactic.b.value)

#         if(pd.notna(row['ASSOC'])):
#             dist_ymw,ymw_tau = derive.dm_to_dist(gl,gb,dm,method='ymw16',mode='MC')
#         else:
#             dist_ymw,ymw_tau = derive.dm_to_dist(gl,gb,dm,method='ymw16')
#         dist_ne,ne_tau = derive.dm_to_dist(gl,gb,dm,method='ne2001')

#         dist_ymw16 = "{:.3f}".format(dist_ymw.value/1000) #Convert to kpc with 3dp
#         dist_ne2001 = "{:.3f}".format(dist_ne.value/1000) #Convert to kpc
        
#         ymw_id = utils.addDistance(con,int(pulsar_id),int(ymw_citation_id),dist_ymw16,uncertainty=None,label='DIST_YMW16')
#         ne_id = utils.addDistance(con,pulsar_id,ne_citation_id,dist_ne2001,uncertainty=None,label='DIST_NE2001')
#     elif(pd.notna(elong) and pd.notna(elat)):

#         galactic = derive.ecliptic_to_galactic(elong,elat)
    
#         gl = float(galactic.l.value)
#         gb = float(galactic.b.value)
        
#         if(pd.notna(row['ASSOC'])):
#             dist_ymw,ymw_tau = derive.dm_to_dist(gl,gb,dm,method='ymw16',mode='MC')
#         else:
#             dist_ymw,ymw_tau = derive.dm_to_dist(gl,gb,dm,method='ymw16')
        
#         dist_ne,ne_tau = derive.dm_to_dist(gl,gb,dm,method='ne2001')

#         dist_ymw16 = "{:.3f}".format(dist_ymw.value/1000) #Convert to kpc with 3dp
#         dist_ne2001 = "{:.3f}".format(dist_ne.value/1000) #Convert to kpc
        
#         ymw_id = utils.addDistance(con,int(pulsar_id),int(ymw_citation_id),dist_ymw16,uncertainty=None,label='DIST_YMW16')
#         ne_id = utils.addDistance(con,pulsar_id,ne_citation_id,dist_ne2001,uncertainty=None,label='DIST_NE2001')


### Derive Period/Frequencies
spin_data_frames = [p0,f0]
spin_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pulsar_id'],how='outer'), spin_data_frames)
spin_merged = spin_merged.sort_values('pulsar_id')


for index, row in spin_merged.iterrows():
    pulsar_id = row['pulsar_id']
    print(pulsar_id)
    P0 = row['P0']
    F0 = row['F0']
    P0_err = row['P0_ERR']
    F0_err = row['F0_ERR']
    P0_id = row['P0_parameter_id']
    F0_id = row['F0_parameter_id']
    # Setting NULLs for next case
    P1 = None
    P1_err = None
    P1_id = None
    F1 = None
    F1_err = None
    F1_id = None
    P2 = None
    P2_err = None
    P2_id = None
    F2 = None
    F2_err = None
    P2_id = None

    parameterType_id = None

    
    # Both F0 and P0 are NULL
    if(pd.isna(F0) and pd.isna(P0)):
        continue

    if(pd.notna(F0) and pd.notna(P0)):
        # Always derive from F0 ?? Pick which parameter is better ?? ( more decimals ? more recent ? )
        # Going with more recent for now
        if(P0_id < F0_id):
            useF = True
        elif(P0_id > F0_id):
            useF = False
    elif(pd.notna(F0)):
        useF = True
    elif(pd.notna(P0)):
        useF = False

    if useF:
        # Deriving Ps from Fs
        #F0_err = row['F0_ERR']
        F0_mjd = row['F0_MJD']
        if(pd.isna(row['F0_citation_id'])):
            continue
        F0_citation_id = int(row['F0_citation_id'])
        F0_fitParameters_id = None
        if(pd.notna(row['F0_fitParameters_id'])):
           F0_fitParameters_id = int(row['F0_fitParameters_id'])
        F0_observingSystem_id = None
        if(pd.notna(row['F0_observingSystem_id'])):
            F0_observingSystem_id = int(row['F0_observingSystem_id'])
        F0_timeDerivative = int(row['F0_timeDerivative'])
        F0_companionNumber = int(row['F0_companionNumber'])
        P0,P0_err = derive.f0_to_p0(F0,F0_err)
        #print(f"Converted F0: {F0},{F0_err} to P0: {P0},{P0_err}")
        # Insert P0,P0_ERR into the database
        parameterType_id = utils.getParameterTypeID(con,'P')
        parameter_id=None
        derived_id=None
        derivedFromParameter_id=None
        parameter_id = utils.addParameter(con,pulsar_id,None,None,F0_fitParameters_id,F0_observingSystem_id,parameterType_id,0,F0_companionNumber,P0,P0_err,F0_mjd)
        derived_id = utils.addDerived(con,parameter_id,f0_to_p0_method,methodVersion)
        derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,F0_id)
        F1temp = cur.execute("SELECT value,uncertainty,parameter_id FROM viewParameter WHERE (referenceTime LIKE ? AND [parameter.citation_id] LIKE ? AND timeDerivative=1)",(F0_mjd,F0_citation_id))
        F1dat = F1temp.fetchone()
        if(F1dat):
            F1 = F1dat[0]
            F1_err = F1dat[1]
            F1_id = F1dat[2]
            P1,P1_err = derive.f1_to_p1(F0,F1,F1_err)
            #print(f"Converted F1: {F1},{F1_err} to P1: {P1},{P1_err}")
            # Insert P1,P1_ERR into the database
            ###############
            parameterType_id = utils.getParameterTypeID(con,'P')
            parameter_id=None
            derived_id=None
            derivedFromParameter_id=None
            parameter_id = utils.addParameter(con,pulsar_id,None,None,F0_fitParameters_id,F0_observingSystem_id,parameterType_id,1,F0_companionNumber,P1,P1_err,F0_mjd)
            derived_id = utils.addDerived(con,parameter_id,f1_to_p1_method,methodVersion)
            derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,F0_id)
            derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,F1_id)
            ###############
            
            # Insert P2,P2_ERR into the database

    else:
        # Deriving Fs Prom Ps
        #P0_err = row['P0_ERR']
        P0_mjd = row['P0_MJD']
        if(pd.isna(row['P0_citation_id'])):
            continue
        P0_citation_id = int(row['P0_citation_id'])
        P0_fitParameters_id = None
        if(pd.notna(row['P0_fitParameters_id'])):
            P0_fitParameters_id = int(row['P0_fitParameters_id'])
        P0_observingSystem_id = None
        if(pd.notna(row['P0_observingSystem_id'])):
            P0_observingSystem_id = int(row['P0_observingSystem_id'])
        P0_timeDerivative = int(row['P0_timeDerivative'])
        P0_companionNumber = int(row['P0_companionNumber'])
        F0,F0_err = derive.p0_to_f0(P0,P0_err)
        #print(f"Converted P0: {P0},{P0_err} to F0: {F0},{F0_err}")
        parameterType_id = utils.getParameterTypeID(con,'F')
        parameter_id=None
        derived_id=None
        derivedFromParameter_id=None
        parameter_id = utils.addParameter(con,pulsar_id,None,None,P0_fitParameters_id,P0_observingSystem_id,parameterType_id,P0_timeDerivative,P0_companionNumber,F0,F0_err,P0_mjd)
        derived_id = utils.addDerived(con,parameter_id,p0_to_f0_method,methodVersion)
        derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,P0_id)

        P1temp = cur.execute("SELECT value,uncertainty,parameter_id FROM viewParameter WHERE (referenceTime LIKE ? AND [parameter.citation_id] LIKE ? AND timeDerivative=1)",(P0_mjd,P0_citation_id))
        P1dat = P1temp.fetchone()
        if(P1dat):
            P1 = P1dat[0]
            P1_err = P1dat[1]
            P1_id
            F1,F1_err = derive.p1_to_f1(P0,P1,P1_err)
            #print(f"Converted P1: {P1},{P1_err} to F1: {F1},{F1_err}")
            derived_id=None
            parameter_id=None
            derivedFromParameter_id=None
            parameter_id = utils.addParameter(con,pulsar_id,None,None,P0_fitParameters_id,P0_observingSystem_id,parameterType_id,1,P0_companionNumber,F1,F1_err,P0_mjd)
            derived_id = utils.addDerived(con,parameter_id,p1_to_f1_method,methodVersion)
            derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,P0_id)
            derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,P1_id)
            

    # if P0 is not None and P1 is not None:
    #     #derive spin down age
    #     P0 = float(P0)
    #     P1 = float(P1)
        
    #     AGE = derive.derive_age(P0,P1)
    #     #derive bsurf
    #     BSURF = derive.derive_bsurf(P0,P1)
    #     #derive B_lc
    #     B_LC = derive.derive_b_lc(P0,P1)
    #     #derive Edot
    #     EDOT = derive.derive_edot(P0,P1)

    #     print(pulsar_id,AGE,BSURF,B_LC,EDOT)
    # else:
    #     print(pulsar_id,"------------------------")
        #Edotd2 (Also need dist and edot derived above for this)... Maybe do this later
    if(pulsar_id==10):
        exit()
        
    
