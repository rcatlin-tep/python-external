# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 07:02:25 2023

@author: AA58436
"""
import os
import pandas as pd
from teppwcore import teppwcore
from tepcoredata import tepcoredata
from tepcore import tepcore
import pi.core.DateUtil as DateUtil
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import Date
from pi.core.EmsCorpTagManager import EmsCorpTagManager
from pathlib import Path
from datetime import datetime

class AfReferenceBuilder:
    CORP_EMS_MAP_TABLE = 'ITESDW.dbo.CORP_POINT_TAG_REF_TEMP'
    af_sql_table = 'ITESDW.dbo.ADMS_POINT_DESC_TO_TAG_REF'
    objectypes = ['ANALOG','STATUS','ACCUMULATOR']
    basequerytemplate = '''SELECT s.pStation STATION_ID,
       ss.name AS STATION_NAME,
       s.Key SCADA_KEY,
       '{scadaobject}:{osisystemprefix}:' +  s.Key as EXPECTED_TAG_NAME,
       s.name SCADA_NAME,
       s.TYPE AS SCADA_POINT_TYPE,
       s.pUNIT AS UNIT_ID,
       su.NAME AS UNIT_NAME,
       s.pAORGROUP AS AOR_ID,
       ag.NAME AS AOR_NAME,
       s.PEQUIP AS EQUIP_ID,
       se.name AS EQUIP_NAME,
       s.archive_group,
       '{scadaobject}' as OBJECT_TYPE
  FROM SCADA:{scadaobject} s, 
  SCADA:STATION ss, 
  SCADA:AOR_GROUP ag, 
  SCADA:UNITS su, 
  SCADA:EQUIP se
where ss.recnum = s.pStation and
s.pAORGROUP = ag.recnum and
(s.pUNIT = su.recnum ) and
s.pEQUIP = se.recnum'''
    basequerytemplatenouom = '''SELECT s.pStation STATION_ID,
       ss.name AS STATION_NAME,
       s.Key SCADA_KEY,
       '{scadaobject}:{osisystemprefix}:' +  s.Key as EXPECTED_TAG_NAME,
       s.name SCADA_NAME,
       s.TYPE AS SCADA_POINT_TYPE,
       s.pUNIT AS UNIT_ID,
       '' AS UNIT_NAME,
       s.pAORGROUP AS AOR_ID,
       ag.NAME AS AOR_NAME,
       s.PEQUIP AS EQUIP_ID,
       se.name AS EQUIP_NAME,
       s.archive_group,
       '{scadaobject}' as OBJECT_TYPE
  FROM SCADA:{scadaobject} s, 
  SCADA:STATION ss, 
  SCADA:AOR_GROUP ag,  
  SCADA:EQUIP se
where ss.recnum = s.pStation and
s.pAORGROUP = ag.recnum and
s.pUNIT=0  and
s.pEQUIP = se.recnum'''
    tablecolumns = [  'STATION_ID','STATION_NAME','SCADA_KEY','EXPECTED_TAG_NAME', 'SCADA_NAME','SCADA_POINT_TYPE','UNIT_ID','UNIT_NAME','AOR_ID','AOR_NAME','EQUIP_ID','EQUIP_NAME','archive_group','OBJECT_TYPE']
    columndtypes={}
    def __init__(self,debug ,domain='dev',osissystem='dms',config='dtnweather.json',usepi=False):
        self.warnings = []
        self.debug = debug
        if domain =='QAS':
            self.dbinfo = teppwcore('qa')
        else:
            self.dbinfo = teppwcore(domain.lower())
        self.tc = tepcore(debug)
        self.osissystem = osissystem
        self.osisystemprefix = osissystem[0].upper()
        self.domain = domain     
        print(config)
        #self.settings = settings(debug,domain,osissystem,config )
        #self.tc.debug(vars(self.settings))
        #self.fileutils = FileUtils(self.settings)
        self.electradata = None
        self.dsn = (self.osissystem + self.domain).upper()
        self.tc.debug('Using odbc sql dsn {dsn}'.format(dsn=self.dsn))
        self.osidata = None 
        self.classname = 'AfReferenceBuilder'
        self.outfile = 'D:/osi/osi_cust/data/scadaref' + DateUtil.GetFullDateForFileStr() + '.csv'
        self._afdataconn = None
        self.emscorptagmanager = None
        self.PI_CONFIG =  'piconnectionmanager.json'
    
    def GetEmsCorpTagManager(self):
      
        if self.emscorptagmanager is None:
            self.emscorptagmanager = EmsCorpTagManager(self.debug ,self.domain,self.osissystem,self.PI_CONFIG,True)
            return self.emscorptagmanager
        else:
            return self.emscorptagmanager

    

   
    def GetAFData(self):
        if self._afdataconn is None:
            if self.osissystem.lower() =='dms':
                print('using dms')
                self._afdataconn = tepcoredata(self.dbinfo.GetUsername('ITESDWCORP'),self.dbinfo.GetPwd('ITESDWCORP'),self.dbinfo.GetDSNInKeePass('ITESDWCORP'),True,False,'PIAFSQL')  
            else:
                self._afdataconn = tepcoredata(self.dbinfo.GetUsername('PIAFSQL'),self.dbinfo.GetPwd('PIAFSQL'),self.dbinfo.GetDSNInKeePass('PIAFSQL'),True,False,'PIAFSQL') 
        return self._afdataconn
    
    def GetOSiData(self):
        if self.osidata is None:
            dbinfo = teppwcore(self.domain)
            self.osidata = tepcoredata('ELECTRA',self.dbinfo.GetPwd('ELECTRA'),self.dbinfo.GetDSNInKeePass('ELECTRA'),True,True,self.dsn)  
        return self.osidata
    def FetchAllScadaData(self):
        retdf = None
        for objecttype in self.objectypes:
            
            if self.osissystem.upper() =='EMS':
                if objecttype =='ANALOG':
                    query = self.basequerytemplate.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix).replace('SCADA:EQUIP se','').replace('s.pEQUIP = se.recnum', '').replace('se.name AS EQUIP_NAME,','').replace('(s.pUNIT = su.recnum ) and','(s.pUNIT = su.recnum )').replace('SCADA:UNITS su, ','SCADA:UNITS su')
                    query1 = self.basequerytemplatenouom.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix).replace('SCADA:EQUIP se',' ').replace('s.pEQUIP = se.recnum', ' ').replace('se.name AS EQUIP_NAME,','').replace('s.pUNIT=0  and','s.pUNIT=0').replace('SCADA:AOR_GROUP ag,','SCADA:AOR_GROUP ag')
                    analogcdonfiglookupquery = '''select
                                                ac.Key SCADA_KEY,
                                                se.name as EQUIP_NAME
                                                FROM SCADA:ANALOG_CONFIG ac,
                                                  SCADA:DEVICE_INSTANCE se
                                                where ac.pDeviceInstance = se.recnum'''
                                                                
                else:
                    query = self.basequerytemplate.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix).replace('SCADA:EQUIP','SCADA:DEVICE_INSTANCE').replace('s.pEQUIP', 's.pDeviceInstance')
                    query1 = self.basequerytemplatenouom.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix).replace('SCADA:EQUIP','SCADA:DEVICE_INSTANCE').replace('s.pEQUIP', 's.pDeviceInstance')
                
            else:
                query = self.basequerytemplate.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix)
                query1 = self.basequerytemplatenouom.format(scadaobject=objecttype,osisystemprefix=self.osisystemprefix)
            self.tc.debug('Fetching data with query {query}'.format(query=query))
            
            if objecttype =='ANALOG' and self.osissystem.upper() =='EMS':
                dfac =self.GetOSiData().run_query_for_df(analogcdonfiglookupquery)
                df = self.GetOSiData().run_query_for_df(query)
                df1 = self.GetOSiData().run_query_for_df(query1)
                if self.tc.isValidDf(dfac):
                    if self.tc.isValidDf(df):
                        df = df.merge(dfac, on='SCADA_KEY',how='left')
                        print(df.head())
                        print(df.dtypes)
                        df = df[self.tablecolumns]
                    if self.tc.isValidDf(df):
                        df1 = df1.merge(dfac, on='SCADA_KEY',how='left')
                        df1 = df1[self.tablecolumns]

            
            else:
                df = self.GetOSiData().run_query_for_df(query)
                df1 = self.GetOSiData().run_query_for_df(query1)
            if self.tc.isValidDf(df) or  self.tc.isValidDf(df1) :
                if retdf is None and self.tc.isValidDf(df):
                    retdf = df
                    if self.tc.isValidDf(df1):
                        retdf = retdf.append(df1, ignore_index=True)
                elif retdf is None and self.tc.isValidDf(df1):
                    retdf = df1
                    retdf = retdf.append(df1, ignore_index=True)
                    if self.tc.isValidDf(df):
                        retdf = retdf.append(df, ignore_index=True)
                else:
                    if self.tc.isValidDf(df):
                        retdf = retdf.append(df, ignore_index=True)
                    if self.tc.isValidDf(df1):
                        retdf = retdf.append(df1, ignore_index=True)
                    
        return retdf
                
    def AddScadaDataForCorpPiData(self):
        
        query = '''UPDATE T SET T.STATION_ID = T1.STATION_ID 
        ,T.STATION_NAME = T1.STATION_NAME
, T.SCADA_KEY = T1.SCADA_KEY
, T.EXPECTED_TAG_NAME = T1.EXPECTED_TAG_NAME 
, T.SCADA_NAME = T1.SCADA_NAME
, T.SCADA_POINT_TYPE =  T1.SCADA_POINT_TYPE
, T.UNIT_ID = T1.UNIT_ID
, T.UNIT_NAME = T1.UNIT_NAME
, T.AOR_ID =  T1.AOR_ID
 , T.AOR_NAME = T1.AOR_NAME
 , T.EQUIP_ID = T1.EQUIP_ID
, T.EQUIP_NAME = T1.EQUIP_NAME
, T.archive_group = T1.archive_group
, T.OBJECT_TYPE = T1.OBJECT_TYPE
, T.OSI_SYSTEM = T1.OSI_SYSTEM
    FROM {corptable} T JOIN  {table} T1  on T.EMS_TAG_NAME = T1.EXPECTED_TAG_NAME and T1.DOMAIN= T.DOMAIN'''.format(table =self.af_sql_table,corptable=self.CORP_EMS_MAP_TABLE)
        self.tc.debug(query)
        res = self.GetAFData().runUpdate(query)
        return res
        
    def OutPutCorpToSqlTable(self,datadf):
        insertDtTm = datetime.now()
        dtypes={'EMS_TAG_NAME':String(250),
            'CORP_TAG_NAME': String(250), 
            'INSERT_DT_TM': Date,
            'DOMAIN': String(15)
            }
        #datadf['OSI_SYSTEM'] = self.osissystem
        datadf['INSERT_DT_TM'] = insertDtTm 
        datadf['DOMAIN']=self.domain
        self.GetAFData().FastDFInsert(datadf, self.CORP_EMS_MAP_TABLE,False,dtypes,replacedata=False)
        return True
        
    
    def FetchCorpPiData(self):
        debug = True 
        res = self.GetEmsCorpTagManager().GetCorpPointsListWithAttributes()
        res.query('''pointsource=='EMS-CP-RT' ''',inplace=True)
        
        print(res[['instrumenttag','Name','descriptor']].values.tolist())
        res = res[['instrumenttag','Name']]
        res.rename(columns={'instrumenttag':'EMS_TAG_NAME','Name':'CORP_TAG_NAME'},inplace=True)
        
        return res

    def ClearCorpSqlTableOlderData(self):
        '''
        Will delete duplicates of scada_key, osi_system where INSERT_DT_TM is the lowest.

        Returns
        -------
        None.

        '''
        
        deletequery = '''delete T2 from  {af_sql_table}  T1
        JOIN {af_sql_table}  T2 on t1.CORP_TAG_NAME = t2.CORP_TAG_NAME and t1.domain = t2.domain where t2.INSERT_DT_TM < T1.INSERT_DT_TM'''.format(af_sql_table=self.CORP_EMS_MAP_TABLE)
        self.tc.debug('celaring older data with query {deletequery}'.format(deletequery=deletequery))
        delres = self.GetAFData().runUpdate(deletequery)
        return delres    

                
    
    def OutPutDataToCSV(self,datadf):
        self.tc.debug('saving data to file {outfile}'.format(outfile=self.outfile))
        try:
            datadf.to_csv(self.outfile,index=False)
        except Exception as e:
            print(str(e))
            return False
        return True
    
    
    def ClearSqlTableOlderData(self):
        '''
        Will delete duplicates of scada_key, osi_system where INSERT_DT_TM is the lowest.

        Returns
        -------
        None.

        '''
        
        deletequery = '''delete T2 from  {af_sql_table}  T1
        JOIN {af_sql_table}  T2 on t1.scada_key = t2.scada_key and t1.domain = t2.domain and t1.OSI_SYSTEM = t2.OSI_SYSTEM where t2.INSERT_DT_TM < T1.INSERT_DT_TM'''.format(af_sql_table=self.af_sql_table)
        self.tc.debug('celaring older data with query {deletequery}'.format(deletequery=deletequery))
        delres = self.GetAFData().runUpdate(deletequery)
        return delres

    
    def OutPutToSqlTable(self,datadf):
        '''
        STATION_ID	int
STATION_NAME	varchar(250)
SCADA_KEY	varchar(50)
EXPECTED_TAG_NAME	varchar(250)
SCADA_NAME	varchar(250)
SCADA_POINT_TYPE	int
UNIT_ID	int
UNIT_NAME	varchar(250)
AOR_ID	int
AOR_NAME	varchar(250)
EQUIP_ID	int
EQUIP_NAME	varchar(50)
archive_group	int
OBJECT_TYPE	varchar(50)
OSI_SYSTEM	varchar(20)
	

        Parameters
        ----------
        datadf : TYPE
            DESCRIPTION.

        Returns
        -------
        bool
            DESCRIPTION.

        '''
        insertDtTm = datetime.now()
        dtypes={'STATION_ID':Integer,
            'STATION_NAME': String(250), 
            'SCADA_KEY':String(50), 
            'EXPECTED_TAG_NAME': String(250),
            'SCADA_NAME':  String(250),
            'SCADA_POINT_TYPE':Integer,
            'UNIT_ID':Integer,
            'UNIT_NAME':String(250),
            'AOR_ID':Integer,
            'AOR_NAME':String(250),
            'EQUIP_ID':Integer,
            'EQUIP_NAME':String(50),
            'archive_group':Integer,
            'OBJECT_TYPE':String(50),
            'OSI_SYSTEM':String(20),
            'INSERT_DT_TM': Date,
            'DOMAIN': String(15)
            }
        datadf['OSI_SYSTEM'] = self.osissystem
        datadf['INSERT_DT_TM'] = insertDtTm 
        datadf['DOMAIN']=self.domain
        self.GetAFData().FastDFInsert(datadf, self.af_sql_table,False,dtypes,replacedata=False)
        return True
    
    def HandleInterfaceWarnings(self):
        
        """
        Emails any errors from the eim 216 interface

        Returns
        -------
        bool
            DESCRIPTION.

        """
        try:
            msglist1=[]
            emailto = self.settings.warningemailuser
            emailsubject ='{classname} Interfaace Warnings from domain {domain}'.format(classname=self.classname , domain=self.domain)
            emailbody='The following {classname} warnings have been triggered from '.format(classname=self.classname ) + self.osissystem
            
            if len(self.warnings)>0:
                #need to use tepcore of normal interface manager for email settings
                self.tc.debug('Warnings {warns}'.format(warns=str(self.warnings)))
                ltepcore = tepcore(True)
                dferrors = pd.DataFrame(self.warnings,columns=['warning','details'])
                msglist1.append([dferrors.to_html(),'html']) 
                self.tc.debug('We have {classname} interface errors !'.format(classname=self.classname ))
                ltepcore.SendMail(emailto,emailsubject,msglist1,emailbody )
        except Exception as e:
            print(str(e))
        return True     
    
    def DoInterface(self,startdt=None):
        outdf = self.FetchAllScadaData()
        self.OutPutDataToCSV(outdf)
        self.OutPutToSqlTable(outdf)
        self.ClearSqlTableOlderData()
        if self.osissystem.lower() !='dms':
            outcorpdf = self.FetchCorpPiData()
            #self.OutPutDataToCSV(outdf)
            self.OutPutCorpToSqlTable(outcorpdf)
            self.ClearCorpSqlTableOlderData()
            self.AddScadaDataForCorpPiData()
        self.HandleInterfaceWarnings()

        return True