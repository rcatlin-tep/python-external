# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 16:04:21 2020

@author: aa58436
"""
from tepemapxmlcore import tepemapxmlcore
from tepinterface import tepinterface as interface
from tepcore import tepcore
from tepcoredata import tepcoredata
from teppwcore import teppwcore
#from sqlalchemy import types
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import Date
import pandas as pd
#from sqlalchemy.types import Date
import time
#import concurrent.futures
#from itertools import repeat
import gc
import numpy as np
import math
from datetime import datetime ,timedelta
import itertools


class tepemapcore:
    INCLAUSESLIMIT = 500
    LOADOBJECTID = 12
    LINEOBJECTID = 8
    SWITCHOBJECTID = 11
    EMAP_LINE='EMAP_LINE'
    EMAP_DEVICE='EMAP_DEVICE'
    EMAP_SWITCH='EMAP_SWITCH'
    EMAP_TRANSFORMER= 'EMAP_TRANSFORMER'
#     8 line
# 9 transforer
# 10 regulateor
# 11 switch
# 12 load
# 13 capacitor
# 14 generator
# 15 ground
    queryforemapdevicesbyfeeder = '''select f.Name feeder_name
    , f.recnum
    , d.name device_name
    , d.object
    , d.omsflags
    , d.record
    , d.indic
    , d.validity
    , d.recnum pDevice
    ,d.NominalState
    ,d.NominalState_ind_1  
    ,d.NominalState_ind_2 
    ,d.PhaseTPState
    ,d.PhaseTPState_ind_1  
    ,d.PhaseTPState_ind_2
    ,d.pDeviceInfo
    ,edi.value1
    ,edi.value5
    from EMAP:EMAP_FEEDER f
    , EMAP:EMAP_DEVICE d 
    ,  EMAP:EMAP_DEVICE_INFO EDI
    where f.name = '{feedername}' and (D.pFeeder = f.recnum or D.pFeeder_ind_1 = f.recnum or  D.pFeeder_ind_2 = f.recnum) and d.indic=1 and d.pDeviceInfo = edi.recnum'''
    
    
    
    queryforemapdevices = '''select f.Name feeder_name
    , f.recnum
    , d.name device_name
    , d.object
    , d.omsflags
    , d.record
    , d.indic
    , d.validity
    , d.recnum pDevice
    ,d.NominalState
    ,d.NominalState_ind_1  
    ,d.NominalState_ind_2 
    ,d.PhaseTPState
    ,d.PhaseTPState_ind_1  
    ,d.PhaseTPState_ind_2
    ,d.pDeviceInfo
    ,edi.value1
    ,edi.value2
    ,edi.value3
    ,edi.value4
    ,edi.value5    
    from EMAP:EMAP_FEEDER f
    , EMAP:EMAP_DEVICE d 
    ,  EMAP:EMAP_DEVICE_INFO EDI
    where (D.pFeeder = f.recnum or D.pFeeder_ind_1 = f.recnum or  D.pFeeder_ind_2 = f.recnum) and d.indic=1 and d.pDeviceInfo = edi.recnum'''
    
    
    loadquery='''SELECT pDevice,Node1Conn FROM EMAP:EMAP_LOAD where recnum in ({inclause})'''
    
    allloadquery = '''SELECT pDevice,Node1Conn FROM EMAP:EMAP_LOAD where Indic=1 and Validity = 1 '''
    
    linequery='''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where recnum in ({inclause})'''
    
    switchquery = '''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide FROM EMAP:EMAP_SWITCH where recnum in ({inclause})'''
    
    allswitchquery = '''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,recnum FROM EMAP:EMAP_SWITCH where Indic=1 and Validity = 1 '''
    
    deviceinfobyid = '''SELECT  key1, value1, key2, value2, key3, value3, key4, value4 FROM EMAP:EMAP_DEVICE_INFO where recnum = {val} '''
    
    devicenamequery ='''select d.name device_name from EMAP:EMAP_DEVICE d where recnum={recnum}'''
    # and l.indic = 1 and l.validity = 1 seems to add a lot of time to the query qucker to do in df
    allinesquery ='''SELECT l.pDevice,l.Node1Conn, l.Node2Conn, l.UpstreamSide,l.indic,
                    D.pFeeder,  D.pFeeder_ind_1, D.pFeeder_ind_2 FROM EMAP:EMAP_LINE l,
                    EMAP:EMAP_DEVICE d where l.pDevice = d.recnum and  '''
    def __init__(self,domain,coredata=None,hostname=None,debug=False):
        self.tepcore=tepcore(debug)
        self.domain=domain
        self.dbinfo = teppwcore(domain)
        self.linesloaded=False
        self.devicesloaded=False
        self.switchdata=False
        self.TraceDataLst=[]
        self.FinalTraceData = pd.DataFrame()
        if coredata is None:
            self.osidata = tepcoredata('ELECTRA',self.dbinfo.GetPwd('ELECTRA'),self.dbinfo.GetDSNInKeePass('ELECTRA'),True,True,'OSI ODBC DataSource 3.x')    
        self.NewTraceData = pd.DataFrame(columns=['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','recnum'])
        
     
    @property 
    def LineData(self):
        return self.__lineData
    
    @LineData.setter
    def LineData(self,lineData):
        self.linesloaded=True
        self.__lineData = lineData
        
    
    @property 
    def SwitchData(self):
        return self.__switchData
    
    @SwitchData.setter
    def SwitchData(self,switchData):
        self.switchdata=True
        self.__switchData = switchData
        
    @property 
    def LoadData(self):
        return self.__loadData
    
    @LoadData.setter
    def LoadData(self,loadData):
        self.__loadData = loadData
     
        
    @property 
    def TraceData(self):
        return self.__traceData
    
    @TraceData.setter
    def TraceData(self,traceData):
        self.__traceData = traceData
    
    
    
    @property 
    def TraceDataLst(self):
        return self.__traceDataLst
    
    @TraceDataLst.setter
    def TraceDataLst(self,traceDataLst):
        self.__traceDataLst = traceDataLst
    
    
    @property 
    def DeviceData(self):
        return self.__deviceData
    
    @DeviceData.setter
    def DeviceData(self,deviceData):
        self.devicesloaded=True
        self.__deviceData = deviceData
        
    @property 
    def DeviceDataWithNodes(self):
        return self.__deviceDataWithNodes
    
    @DeviceDataWithNodes.setter
    def DeviceDataWithNodes(self,deviceDataWithNodes):
        self.devicesloaded=True
        self.__deviceDataWithNodes = deviceDataWithNodes
        
    @property 
    def FinalTraceData(self):
        return self.__FinalTraceData
    
    @FinalTraceData.setter
    def FinalTraceData(self,FinalTraceData):
        self.__FinalTraceData = FinalTraceData 
        
    @property 
    def NewTraceData(self):
        return self.__NewTraceData
    
    @NewTraceData.setter
    def NewTraceData(self,NewTraceData):
        self.__NewTraceData = NewTraceData   
     
        
    def ReconnectOSiData(self):
        self.tepcore.debug('Reconnected to OSI ODBC')
        self.osidata = None
        self.osidata = tepcoredata('ELECTRA',self.dbinfo.GetPwd('ELECTRA'),self.dbinfo.GetDSNInKeePass('ELECTRA'),True,True,'OSI ODBC DataSource 3.x')   
    def GetOsiData(self):
        return self.osidata
        
    def FileCacheDF(self,df,cachename,cachedir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\',mode='w'):
        #dleete only if not doing a append mode
        if mode == 'w':
            self.tepcore.SafeFileDelete(cachedir+cachename+'.cache')
        df.to_csv(cachedir+cachename+'.cache',mode=mode)
        
    def GetFileCacheDf(self,cachename,cachedir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'):
        try:
            df1 = pd.read_csv(cachedir+cachename+'.cache',engine='c',index_col=0)
        except:
            return False
        return df1
    
    def MoveCache(self,cachename,cachedir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\',finaldir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\completed\\'):
        cachefull = cachedir + cachename+'.cache'
        self.tepcore.CopyFile(cachefull, finaldir + cachename+'.cache')
        return self.tepcore.SafeFileDelete(cachefull)
      
    
    def MergeAllCicuitCache(self,circuits):
        outdf = None
        for circuit in circuits:
            cache = circuit+ 'maptraces'
            self.tepcore.debug('Working cache {cache}'.format(cache=cache))
            updates = self.GetFileCacheDf(cache)
            if self.tepcore.isValidDf(updates):
                if outdf is None:
                    outdf = updates
                else:
                    print('adding updates')
                    #outdf.append(updates)
                    outdf = pd.concat([outdf, updates], axis=0)
            else:
                self.tepcore.debug('{circuit} failed to load trace cache'.format(circuit=circuit))
        print(outdf.head())            
        outdf = outdf[['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','recnum.1','nominal_circuit']]
        outdf.reset_index(inplace=True)
        outdf.index = outdf.index+1
        outdf['recnum'] = outdf.index
        outdf['Validity'] = 1
        print(outdf.head())
        
        checkdf = outdf.query('pDevice_src==254605.0')
        print(checkdf.head())
        self.CreateatePopFile(outdf)  
        
        
    def FastUpdateFromCache(self,circuit=None):
        if circuit is None:
            cache = 'maptraces'
        else:
            cache = circuit+ 'maptraces'
        print(cache)
        updates = self.GetFileCacheDf(cache)
        
        
        if self.tepcore.isValidDf(updates):
            #result = self.UpdateFast(updates,'TEP_EMAP_DEVICE:EMAP_TRACE')
            updates = updates[['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','recnum']]
            self.CreateatePopFile(updates)
            self.MoveCache(cache)
            return True
        else:
            return False
            
    def GetDeviceNameFromDF(self,pDevice):
        if '.0' in str(pDevice):
            dev = str(pDevice)[:-2]
        else:
            dev = str(pDevice)
       # print('GetDeviceNameFromDF:')
        #print(dev)
        try: 
            
          
            devname = str(self.DeviceData[self.DeviceData.index==dev].iloc[0]['device_name'])
        except:
            self.tepcore.debug('error finding name for  {dev}'.format(dev=dev))
            devname = 'unkown device name {dev}'.format(dev=dev)
        #print('GetDeviceNameFromDF 1 performance log:')    
        return devname
   
    def GetDeviceName(self,recnum):
        sql = self.devicenamequery.format(recnum=recnum)
        df = self.osidata.run_query_for_df(sql)
        if self.tepcore.isValidDf(df):
            
            return df.iloc[0]['device_name']
        else:
            return None
    
    def GetAllLines(self,usecache=False): 
        df1 = None
        if usecache:
            df1 = self.GetFileCacheDf('lines')
        if self.tepcore.isValidDf(df1):
            self.tepcore.debug('using line cache')
            return df1
        else:
            self.tepcore.debug('running line query SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType,recnum FROM EMAP:EMAP_LINE where indic=1')
            df1 = self.osidata.run_query_for_df('SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType,recnum FROM EMAP:EMAP_LINE where indic=1',100000000)
            self.tepcore.debug('flusing line cache')
            self.FileCacheDF(df1,'lines')
        return df1
    
    def GetDevicesOnFeederMulti(self,circuit):

        #conn= self.osidata.GetNewConnection()
        print(circuit)
        #qyer = self.queryforemapdevicesbyfeeder.format(feedername=feedername)
        #self.tepcore.debug(qyer)
        #df = self.osidata.run_query_for_df_w_conn(conn,qyer)
        return circuit
    
    def GetAllCircuitsAsDF(self,circuits):
        querys = []
        for feedername in circuits:
            query = self.queryforemapdevicesbyfeeder.format(feedername=feedername)
            querys.append(query)
        results = self.osidata.GetDfsForQuerys(querys)
        return results

    def ClearAllCacheDevices(self,cachedir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\',caches = ['loads','switchs','devices','lines'],mode='clear'):
        
        if mode=='clear':
            for cachename in caches:
                self.tepcore.SafeFileDelete(cachedir+cachename+'.cachebkp')
                try:
                    self.tepcore.CopyFile(cachedir+cachename+'.cache', cachedir+cachename+'.cachebkp')
                except Exception as e:
                    self.tepcore.deub('unable to copy file {cachename}'.format(cachename==cachename))
        elif mode=='restore':
            for cachename in caches:
                #self.tepcore.SafeFileDelete(cachedir+cachename+'.cachebkp')
                try:
                    self.tepcore.CopyFile(cachedir+cachename+'.cachebkp', cachedir+cachename+'.cache')
                except Exception as e:
                    self.tepcore.deub('unable to copy file {cachename}'.format(cachename==cachename))
        return True
    
    def ProcessCachedFeederTraces(self,circuits=None):
        if circuits is not None:
           for circuit in circuits:
                self.FastUpdateFromCache(circuit)
        else:
            self.FastUpdateFromCache()
    
    def GetDevicesForCircuitFromChache(self,circuit):
        if not self.devicesloaded:
            devicecache = self.GetFileCacheDf('devices')
            self.DeviceData=devicecache
        return self.DeviceData.query('feeder_name==@circuit')
    
    
    def GetSwitchDataFromChache(self,recnums=None):
        if not self.switchdata:
            devicecache = self.GetFileCacheDf('switchs')
            self.SwitchData=devicecache.set_index('recnum')
        if recnums is None:
            return self.SwitchData
        return self.SwitchData[self.SwitchData.index.isin(recnums)]
    
    
    def CacheEMAPDeviceInfo(self):
        sql = '''select  edi.value1
                 ,edi.value2
                  ,edi.value3
                   ,edi.value4
                ,edi.value5
                ,edi.recnum 
                from EMAP:EMAP_DEVICE_INFO EDI  where indic = 1 and validity=1 '''
        df1 = self.osidata.run_query_for_df(sql)
        #df1 = df1.query('indic==1 and validity==1')
        df1.set_index('recnum' , inplace=True)
        self.FileCacheDF(df1,'alldeviceinfo')
        return True
    
    def CacheEMAPFeederData(self):
        feedersql = '''select name feeder_name , recnum from EMAP:EMAP_FEEDER where indic = 1 and validity=1'''
        df1 = self.osidata.run_query_for_df(feedersql)
        df1 = df1.query('indic==1 and validity==1')
        df1.set_index('recnum' , inplace=True)
        self.FileCacheDF(df1,'allfeeders')
        return True
    
    def CacheEMAPDeviceData(self):
        devquery = ''' select d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                ,d.NominalState
                ,d.NominalState_ind_1  
                ,d.NominalState_ind_2 
                ,d.PhaseTPState
                ,d.PhaseTPState_ind_1  
                ,d.PhaseTPState_ind_2
                ,d.pDeviceInfo
                ,d.pFeeder 
                ,d.pFeeder_ind_1
                ,d.pFeeder_ind_2 
                ,d.recnum
                from EMAP:EMAP_DEVICE d'''
        df1 = self.osidata.run_query_for_df(devquery)
        df1 = df1.query('indic==1 and validity==1')
        df1.set_index('recnum' , inplace=True)
        self.FileCacheDF(df1,'allemapdevice')
        return df1
            
    
    def GetAllFeederRecs(self):
        circsql = '''select recnum, name from emap:emap_feeder f where f.indic=1 and f.pAorGroup=5 order by f.recnum'''
        self.tepcore.debug(circsql)
        df1 = self.osidata.run_query_for_df(circsql)
        feederrecs = df1['recnum'].tolist()
        return  feederrecs
    
    def GetDevicesForFeeder(self,feederrec,retry=True):
        maxtrys=4
        sql = '''select 
        d.pFeeder
        ,d.pFeeder_ind_1
        ,d.pFeeder_ind_2
        , d.pNominalFeeder
        ,d.pNominalFeeder_ind_1
        ,d.pNominalFeeder_ind_2
        ,f.Name feeder_name
        , f.recnum
        , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                ,d.NominalState
                ,d.NominalState_ind_1  
                ,d.NominalState_ind_2 
                 ,d.PhaseState
                ,d.PhaseState_ind_1  
                ,d.PhaseState_ind_2
                ,d.PhaseTPState
                ,d.PhaseTPState_ind_1  
                ,d.PhaseTPState_ind_2
                ,d.pDeviceInfo
                ,d.isAbnormal
                ,edi.value1
                 ,edi.value2
                  ,edi.value3
                   ,edi.value4
                ,edi.value5
                from  EMAP:EMAP_FEEDER f
                , EMAP:EMAP_DEVICE d  
                ,  EMAP:EMAP_DEVICE_INFO EDI
                where (f.recnum={recnum})
                and (D.pNominalFeeder = {recnum} or D.pNominalFeeder_ind_1 = {recnum}  or  D.pNominalFeeder_ind_2 ={recnum} ) 
               and d.indic=1 and d.pDeviceInfo = edi.recnum  '''.format(recnum=feederrec)
        #self.tepcore.debug('running some query')
        self.osidata.debug = False;
        
        if retry:
            res = False
            retrycnt=0
            while  ( retrycnt < maxtrys):
                retrycnt = retrycnt + 1
                resdf = self.osidata.run_query_for_df(sql)
                tempres = self.tepcore.isValidDf(resdf)
                if tempres:
                    return resdf
                else:
                    self.tepcore.debug('Failed to get results retrying {feederrec}'.format(feederrec=str(feederrec)))
                    #reestablish coredata might help
                    #self.osidata = tepcoredata('ELECTRA',self.dbinfo.GetPwd('ELECTRA'),self.dbinfo.GetDSNInKeePass('ELECTRA'),True,True,'OSI ODBC DataSource 3.x')
                    self.osidata.debug = True;
                    res = tempres
                
        else:    
            return self.osidata.run_query_for_df(sql)
    
    def CacheAllDevices(self,circuits=None):
        dfdevices = None
        self.tepcore.debug('backup existing cache')
        self.tepcore.SafeFileDelete('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'+'devices'+'.cachebkup')
        self.tepcore.CopyFile('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'+'devices'+'.cache', 'D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'+'devices'+'.cachebkup')
        self.tepcore.SafeFileDelete('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'+'devices'+'.cache')
        if circuits is None:
            device_sql = '''select f.Name feeder_name
                , f.recnum
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                , d.NominalState
                , d.NominalState_ind_1  
                , d.NominalState_ind_2 
                , d.PhaseTPState
                , d.PhaseTPState_ind_1  
                , d.PhaseTPState_ind_2
                , d.pDeviceInfo
                , edi.value1
                , edi.value2
                , edi.value3
                , edi.value4
                , edi.value5
                from  EMAP:EMAP_FEEDER f, EMAP:EMAP_DEVICE d, EMAP:EMAP_DEVICE_INFO EDI
                where (D.pFeeder = f.recnum or D.pFeeder_ind_1 = f.recnum or D.pFeeder_ind_2 = f.recnum)
                and d.pDeviceInfo = edi.recnum
                and d.indic=1 and f.pAorGroup=5 and f.indic=1 '''
            dfdevices = self.osidata.run_query_for_df(device_sql)
        else:
            circinclause = None
            for circuit in circuits:
                if circinclause is None:
                    circinclause = "'" + circuit +  "'" 
                else:
                    circinclause = circinclause + ',' + "'" + circuit +  "'" 
            circuit_recnumsql = '''select distinct recnum from EMAP:EMAP_FEEDER where name in ({circinclause}) '''.format(circinclause=circinclause)
            circuit_recnumdf = self.osidata.run_query_for_df(circuit_recnumsql)
            circuit_recnumlist = circuit_recnumdf.recnum.unique().tolist()
            circuit_recnumlist = str(circuit_recnumlist)[1:-1]
            device_sql1 = '''select f.Name feeder_name
                , f.recnum
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                , d.NominalState
                , d.NominalState_ind_1  
                , d.NominalState_ind_2 
                , d.PhaseTPState
                , d.PhaseTPState_ind_1  
                , d.PhaseTPState_ind_2
                , d.pDeviceInfo
                , edi.value1
                , edi.value2
                , edi.value3
                , edi.value4
                , edi.value5
                , f.pAorGroup
                , f.indic feeder_indic
                , d.indic device_indic
                from  EMAP:EMAP_FEEDER f, EMAP:EMAP_DEVICE d, EMAP:EMAP_DEVICE_INFO EDI
                where D.pFeeder_ind_1 = f.recnum and d.pDeviceInfo = edi.recnum and f.recnum in ({circuit_recnumlist}) '''.format(circuit_recnumlist=circuit_recnumlist)
            dfdevices1 = self.osidata.run_query_for_df(device_sql1)
            device_sql2 = '''select f.Name feeder_name
                , f.recnum
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                , d.NominalState
                , d.NominalState_ind_1  
                , d.NominalState_ind_2 
                , d.PhaseTPState
                , d.PhaseTPState_ind_1  
                , d.PhaseTPState_ind_2
                , d.pDeviceInfo
                , edi.value1
                , edi.value2
                , edi.value3
                , edi.value4
                , edi.value5
                , f.pAorGroup
                , f.indic feeder_indic
                , d.indic device_indic
                from  EMAP:EMAP_FEEDER f, EMAP:EMAP_DEVICE d, EMAP:EMAP_DEVICE_INFO EDI
                where D.pFeeder_ind_1 = f.recnum and d.pDeviceInfo = edi.recnum and f.recnum in ({circuit_recnumlist}) '''.format(circuit_recnumlist=circuit_recnumlist)
            dfdevices2 = self.osidata.run_query_for_df(device_sql2)
            device_sql3 = '''select f.Name feeder_name
                , f.recnum
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                , d.NominalState
                , d.NominalState_ind_1  
                , d.NominalState_ind_2 
                , d.PhaseTPState
                , d.PhaseTPState_ind_1  
                , d.PhaseTPState_ind_2
                , d.pDeviceInfo
                , edi.value1
                , edi.value2
                , edi.value3
                , edi.value4
                , edi.value5
                , f.pAorGroup
                , f.indic feeder_indic
                , d.indic device_indic
                from  EMAP:EMAP_FEEDER f, EMAP:EMAP_DEVICE d, EMAP:EMAP_DEVICE_INFO EDI
                where D.pFeeder_ind_1 = f.recnum and d.pDeviceInfo = edi.recnum and f.recnum in ({circuit_recnumlist}) '''.format(circuit_recnumlist=circuit_recnumlist)
            dfdevices3 = self.osidata.run_query_for_df(device_sql3)
            dfdevices = pd.concat([dfdevices1, dfdevices2, dfdevices3], axis=0)
            dfdevices.drop_duplicates(inplace=True)
            dfdevices.query('pAorGroup == 5 and feeder_indic == 1 and device_indic == 1', inplace=True)
            dfdevices.drop(columns=['pAorGroup', 'feeder_indic', 'device_indic'], inplace=True)
        self.tepcore.debug('Flushing current devices to cache')
        self.FileCacheDF(dfdevices,'devices','D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\','a')

        return True
            
    
    def CacheAllObjects(self,circuits=None):
        
       
        
        
        df1=None
        df1 = self.osidata.run_query_for_df(self.allloadquery)
        if self.tepcore.isValidDf(df1):
            self.FileCacheDF(df1,'loads')
        else:
            self.tepcore.debug('Failed to load loads cache with sql {sql}'.format(sql =self.allloadquery))
        
        df1=None
        df1 = self.osidata.run_query_for_df(self.allswitchquery)
        if self.tepcore.isValidDf(df1):
            self.FileCacheDF(df1,'switchs')
            self.SwitchData = df1
        else:
            self.tepcore.debug('Failed to load switch cache with sql {sql}'.format(sql =self.allswitchquery))
        
        
        self.GetAllLines(False)
        self.PopulateTepTraceData(False)
        cachedevices = self.CacheAllDevices(circuits)
        if cachedevices:
            self.tepcore.debug('Finished Caching devices')
        else:
            self.tepcore.debug('Failed to load devices cache with sql {sql}'.format(sql =self.queryforemapdevices))
        

            
            
            
        df1=None
        df1 = self.osidata.run_query_for_df('SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where indic=1',100000000)
        if self.tepcore.isValidDf(df1):
            self.FileCacheDF(df1,'lines')
            self.LineData = df1
        else:
            self.tepcore.debug('Failed to load devices cache with sql {sql}'.format(sql ='SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where indic=1'))
     
    
    
    def CacheEmapDeviceLinesToOracle(self):
        sql = '''select  ed.pFeeder,  
ed.pFeeder_ind_1,  
ed.pFeeder_ind_2, 
ed.PhaseTPState , 
ed.PhaseTPState_ind_1 , 
ed.PhaseTPState_ind_2 , 
ed.Record, 
ed.recnum  ,
ed.spare49
from emap:emap_device ed 
where ed.object = 8
and ed.Indic=1 
and ed.Validity = 1'''
        df1 = self.osidata.run_query_for_df(sql,100000000)
        dtypes = {'pFeeder':Integer,
        'pFeeder_ind_1':Integer, 
        'pFeeder_ind_2': Integer, 
        'PhaseTPState': Integer,
         'PhaseTPState_ind_1':Integer, 
          'PhaseTPState_ind_2':Integer, 
           'Record':Integer, 
            'recnum':Integer,
             'spare49':Integer,
             'insert_dt_tm':Date}
        
        electradata= tepcoredata('ELECTRA',self.dbinfo.GetPwd('ELECTRA'),self.dbinfo.GetDSNInKeePass('ELECTRA'),True,True)  
         
        if self.tepcore.isValidDf(df1):
            df1['insert_dt_tm']= datetime.now()
            self.tepcore.debug(df1.head())
            electradata.ReplaceTable(df1, 'EMAP_LINE',dtypes)
            
            
    
    def GetDeviceChache(self):
        self.DeviceData=self.GetFileCacheDf('devices').set_index('pDevice')
        return  self.DeviceData
    
    
    def GetAllCaches(self):        
        self.DeviceData=self.GetFileCacheDf('devices')
        self.DeviceData['pDevice']= self.DeviceData['pDevice'].astype(str)
        self.DeviceData.set_index('pDevice',inplace=True)
        self.LineData=self.GetFileCacheDf('lines')
        self.SwitchData = self.GetFileCacheDf('switchs')
        self.TraceData = self.GetFileCacheDf('existingtraces')


    def SetValidityFlag(self):
        print('Need to update Validty = 0 where pDevice_Src /pDevice_dest is no longer valid')
                
    def PopulateTepTraceData(self,usecache):
        tracedir = self.tepcore.GetConfigItem('tracecachedir')
        self.tepcore.debug('PopulateTepTraceData usecache {usecache}'.format(usecache=str(usecache)))
        if usecache:
            self.TraceData = self.GetFileCacheDf('existingtraces')
            return True
        numrecs = 1000000
        cnt = 0
        arg1=1
        df = None
        cnter = 0
        if not usecache:
            #indexvals = np.arange(1, 5000000,1).tolist()
            #tempallvalsdf = pd.DataFrame(index=indexvals)
            
            destination= tracedir + '182.xml'
            dbinfo = teppwcore(self.domain)
            electradata = tepcoredata('ELECTRA',dbinfo.GetPwd('ELECTRA'),dbinfo.GetDSNInKeePass('ELECTRA'))
            admsgisinterface = interface(self.domain, 'fake',electradata)
            emapxmlcore = tepemapxmlcore(self.domain)
            df = emapxmlcore.GetEmapTepTraceData(admsgisinterface)
# =============================================================================
#             while cnt <= 20:
#                 cnt = cnt + 1
#                 
#                 arg2 = 250000 * cnt
#                 query = '''select pDevice_src, pDevice_dest , recnum, recnum idx, 0 update_ind, Validity from TEP_EMAP_DEVICE:EMAP_TRACE where recnum between {arg1} and {arg2}'''.format(arg1=arg1, arg2=arg2)
#                 self.tepcore.debug(query)
#                 if df is None:
#                     df = self.osidata.run_query_for_df(query)
#                 else:
#                     df = df.append(self.osidata.run_query_for_df(query))
#                     
#                 cnter = cnter + 1
#                 if cnter ==5:
#                     self.tepcore.debug('Flushing current devices to cache')
#                     self.FileCacheDF(df,'existingtraces','D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\','a')
#                     cnter = 0
#                     dfdevices = None
#             
#                 arg1 = arg2 + 1
#                 print(df.head())
# =============================================================================
        #df.set_index('idx',inplace=True)

        
        #self.FileCacheDF(df,'existingtraces')
        self.TraceData = df
        return True
        
    
    def GetTepTraceRecNum(self,pDevice_src, pDevice_dest):
        
        try :
            #tempdf = self.TraceData.query('pDevice_src ==@pDevice_src and pDevice_dest==@pDevice_dest')
            if 1==1:#tempdf.empty:
                #maxnumdf = self.TraceData[self.TraceData['pDevice_src']==np.nan]
                maxnumdf = self.TraceData.query('pDevice_src != pDevice_src')
                #print('np.nan list')
                #print(maxnumdf.head())
                maxnum = maxnumdf.index.min()
            else:
                #should only be first index
                return tempdf.index.tolist()[0]
            
        except Exception as e:
            print(str(e))
            
            maxnum = 1
        return maxnum
    
    def MergeNewDataFrame(self,df):
        mergedf = self.NewTraceData.merge(how='left',left_on=['pDevice_src','pDevice_dest'],right_on=['pDevice_src','pDevice_dest'])
        
    def InsertIntoTraceInfoDf(self,pDevice_src,pDevice_dest,Name_src,Name_dest,direction,recnum):
        data = []
        data.append([pDevice_src,pDevice_dest,Name_src,Name_dest,direction,recnum])
        tempdf = pd.DataF1rame(data,columns=['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','recnum'])
        self.NewTraceData = self.NewTraceData.append(tempdf)
        #self.tepcore.debug(self.NewTraceData.head())
        return self.NewTraceData
        
    def PerformFastUpdateFromDf(self,circuit=None):
        tracedir = self.tepcore.GetConfigItem('tracecachedir')
        self.tepcore.debug('PerformFastUpdateFromDf {circuit}'.format(circuit=circuit))
        #updates = self.TraceData.query('update_ind==1')
        #updates = updates.drop(labels=['update_ind'],axis=1)
        updates = None
        updates = pd.DataFrame(self.TraceDataLst,columns= ['pDevice_src','pDevice_dest','Name_src','Name_dest','direction'])
        updates['Validity']=1
        t = time.time()
        updates['TraceDate']=int(t)
        updates['nominal_circuit']=circuit
        updates['recnum'] = updates.index
        updates = updates.reindex(columns= ['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','nominal_circuit', 'recnum','Validity','TraceDate'])
        print(updates.head())
        if circuit is None:
            self.FileCacheDF(updates,'maptraces',tracedir)
        else:
            self.FileCacheDF(updates,circuit+'maptraces',tracedir)
            
        #result = self.UpdateFast(updates,'TEP_EMAP_DEVICE:EMAP_TRACE')
        return True
    
    def PerformFastUpdateFromDfbyCircuits(self,circuit=None):
        tracedir = self.tepcore.GetConfigItem('tracecachedir')
        self.tepcore.debug('PerformFastUpdateFromDf all circuits ') 

        tempDevicedf = self.DeviceDataWithNodes[['object', 'device_name']].copy()
        tempTraceddf = self.FinalTraceData
        tempTraceddf['pDevice'] = tempTraceddf['list_of_children']
        tempTraceddf.set_index('pDevice',inplace=True)
        tempTraceddf = tempTraceddf.merge(tempDevicedf, how = 'inner', left_index=True, right_index=True)
        
        tempTraceddf = tempTraceddf.query("parent_id != list_of_children")
        indices = tempTraceddf[tempTraceddf['object'] == 8].index
        tempTraceddf.drop(indices, inplace=True)
        
        tempTraceddf = tempTraceddf.drop_duplicates(subset=['parent_id', 'list_of_children'])
        
        tempTraceddf.rename(columns={"device_name": "Name_dest"}, inplace=True)
        tempTraceddf.rename(columns={"parent_id": "pDevice_src"}, inplace=True)
        tempTraceddf.rename(columns={"list_of_children": "pDevice_dest"}, inplace=True)
        tempTraceddf.rename(columns={"nominal_circuit_src": "nominal_circuit"}, inplace=True)
        
        tempTraceddf = tempTraceddf[['pDevice_src', 'pDevice_dest', 'Name_src', 'Name_dest', 'direction', 'nominal_circuit']]
        self.FinalTraceData = tempTraceddf
        
        updates = None             
        updates = pd.DataFrame(self.FinalTraceData,columns= ['pDevice_src','pDevice_dest','Name_src','Name_dest','direction', 'nominal_circuit'])  
        updates.reset_index(inplace = True)
        updates['recnum'] = updates.index
        updates['Validity']=1
        t = time.time()
        updates['TraceDate']=int(t)
        updates = updates.reindex(columns= ['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','nominal_circuit', 'recnum','Validity','TraceDate'])
        #print(updates.head())
        if circuit is None:
            self.FileCacheDF(updates,'maptraces',tracedir)
        else:
            self.FileCacheDF(updates,circuit+'maptraces',tracedir)

        return True
    
    def PerformGenerateNoTracedFiles(self,circuit=None):
        tracedir = self.tepcore.GetConfigItem('tracecachedir')
        self.tepcore.debug('Generate empty traced file ') 
        
        updates = pd.DataFrame(columns = ['pDevice_src','pDevice_dest','Name_src','Name_dest','direction','nominal_circuit', 'recnum','Validity','TraceDate'])             
        print(updates.head())
        if circuit is None:
            self.FileCacheDF(updates,'maptraces',tracedir)
        else:
            self.FileCacheDF(updates,circuit+'maptraces',tracedir)

        return True
        
    def UpdateTraceInfo(self,pDevice_src,pDevice_dest,Name_src,Name_dest,direction,circuit):
        
        print('UpdateTraceInfo circuit'.format(circuit=circuit))
       #recnum = self.GetTepTraceRecNum(pDevice_src,pDevice_dest)
        #sql = '''UPDATE TEP_EMAP_DEVICE:EMAP_TRACE SET pDevice_src ={pDevice_src}, pDevice_dest ={pDevice_dest}, Name_src ='{Name_src}' , Name_dest ='{Name_dest}', direction='{direction}' where recnum = {recnum}'''.format(pDevice_src=pDevice_src,pDevice_dest=pDevice_dest,Name_src=Name_src,Name_dest=Name_dest,recnum=int(recnum),direction=direction)
        self.tepcore.debug('update UpdateTraceInfo {circuit}'.format(circuit=circuit))
        self.TraceDataLst.append([pDevice_src,pDevice_dest,Name_src,Name_dest,direction])
        #self.TraceData = pd.concat([self.TraceData,pd.Series([pDevice_src,pDevice_dest,Name_src,Name_dest,direction,circuit])],axis=1)
        #self.TraceData.concat([pDevice_src,pDevice_dest,Name_src,Name_dest,direction,circuit])
        #self.TraceData.at[recnum, 'pDevice_src'] = pDevice_src
        #self.TraceData.at[recnum, 'pDevice_dest'] = pDevice_dest
        #self.TraceData.at[recnum, 'Name_src'] = Name_src
        #self.TraceData.at[recnum, 'Name_dest'] = Name_dest
        #self.TraceData.at[recnum, 'direction'] = direction
        #self.TraceData.at[recnum, 'update_ind'] = 1
        #self.TraceData.at[recnum, 'recnum'] = 0 #no longer recnum
        #self.TraceData.at[recnum, 'nominal_circuit'] = circuit
        #self.tepcore.debug('updating recnum {recnum}'.format(recnum=str(recnum)))
        
        
        return 1
    def GetDevicesOnFeeder(self,feedername):
        qyer = self.queryforemapdevicesbyfeeder.format(feedername=feedername)
        self.tepcore.debug(qyer)
        df = self.osidata.run_query_for_df(qyer)
        return df
    
    def SplitList(self,my_list):
        n = self.INCLAUSESLIMIT
        final = [my_list[i * n:(i + 1) * n] for i in range((len(my_list) + n - 1) // n )]  
        return final
    
    
    def LoadAllLines(self):
        self.LineData =  self.GetAllLines(True)
        return self.LineData 
    
    def GenerateInClause(self,listofvals):
        inclauses = []
        if len(listofvals) > self.INCLAUSESLIMIT:
              sublist = self.SplitList(listofvals)           
              for curlist in sublist:
                  inclause = None
                  for curitm in curlist:
                      if inclause is not None:
                          inclause = inclause + ',' + str(curitm)
                      else:
                          inclause = str(curitm)
                  inclauses.append(inclause)
        else:
            inclause = None
            for curitm in listofvals:
                  if inclause is not None:
                      inclause = inclause + ',' + str(curitm)
                  else:
                      inclause = str(curitm)
            inclauses.append(inclause)
        return inclauses
    
    def GetLoads(self,loads):
        df = None
        incluases =  self.GenerateInClause(loads)
        for inclause in incluases:
          
            temploadquery = self.loadquery.format(inclause=inclause)
            self.tepcore.debug(temploadquery)
            dftemp = self.osidata.run_query_for_df(temploadquery)
            
            if df is None:
                df = dftemp
            else:
                df = df.append(dftemp)
        return df
    def GetObjectParallel(self,feederdf,objecttype):
        
        df = None
        queries=[]
        if objecttype == self.LINEOBJECTID:   
            query = self.linequery
            dfobjs = feederdf.loc[feederdf['object'] == self.LINEOBJECTID]
            objectids = dfobjs['record'].tolist()
        elif objecttype == self.LOADOBJECTID:   
            query = self.loadquery
            dfobjs = feederdf.loc[feederdf['object'] == self.LOADOBJECTID]
            objectids = dfobjs['record'].tolist()
        elif objecttype == self.SWITCHOBJECTID:   
            query = self.switchquery
            dfobjs = feederdf.loc[feederdf['object'] == self.SWITCHOBJECTID]
            objectids = dfobjs['record'].tolist()
            
        incluases =  self.GenerateInClause(objectids)  
        for inclause in incluases:
          
            temploadquery = query.format(inclause=inclause)
            self.tepcore.debug(temploadquery)
            queries.append(temploadquery)
           
        results  = self.osidata.ParrallelQueryForDF(queries)
        for dftemp in results:
            if df is None:
                df = dftemp
            else:
                df = df.append(dftemp)
        return df
    
    
    def GetObject(self,feederdf,objecttype):
        df = None
        
        if objecttype == self.LINEOBJECTID:   
            query = self.linequery
            dfobjs = feederdf.loc[feederdf['object'] == self.LINEOBJECTID]
            objectids = dfobjs['record'].tolist()
        elif objecttype == self.LOADOBJECTID:   
            query = self.loadquery
            dfobjs = feederdf.loc[feederdf['object'] == self.LOADOBJECTID]
            objectids = dfobjs['record'].tolist()
        elif objecttype == self.SWITCHOBJECTID:   
            query = self.switchquery
            dfobjs = feederdf.loc[feederdf['object'] == self.SWITCHOBJECTID]
            objectids = dfobjs['record'].tolist()
            
        incluases =  self.GenerateInClause(objectids)  
        for inclause in incluases:
          
            temploadquery = query.format(inclause=inclause)
            self.tepcore.debug(temploadquery)
            dftemp = self.osidata.run_query_for_df(temploadquery)
            
            if df is None:
                df = dftemp
            else:
                df = df.append(dftemp)
        return df

    
    #currently only gets to a switch in the future can merge together assortment of dataframes if needed. could also just do queries in db to see but seem to be a bit slower then we would like
    def GetAttachedDevice(self,nodeid,upstreamside=1,direction='down'):
        #if we want to try bot hat oncedfsegs= self.linesdf[(self.linesdf["Node1Conn"]==nodeid) | (self.linesdf["Node2Conn"]==nodeid)]
        dfdevice= self.SwitchData[(self.SwitchData["Node1Conn"]==nodeid)| (self.SwitchData["Node2Conn"]==nodeid)]
        return dfdevice
    
    def GetNextSegmentViaQuery(self,nodeid,direction='down',nodecnt=0):
       #if we want to try bot hat oncedfsegs= self.linesdf[(self.linesdf["Node1Conn"]==nodeid) | (self.linesdf["Node2Conn"]==nodeid)]
       if direction=='down':
           query = '''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where (Node1Conn ={nodeid} and UpstreamSide=1) or (Node2Conn = {nodeid} and UpstreamSide=2)'''.format(nodeid =str(int(nodeid)))
       else:
           if nodecnt != 1:
               query = '''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where (Node1Conn ={nodeid} and UpstreamSide=2) or (Node2Conn = {nodeid} and UpstreamSide=1)'''.format(nodeid =str(int(nodeid))) 
           else:
               #on our upstream node if the second basbar splits up trace that one as well
               query = '''SELECT pDevice,Node1Conn, Node2Conn, UpstreamSide,pLineType FROM EMAP:EMAP_LINE where (Node1Conn ={nodeid} and UpstreamSide=2) or (Node2Conn = {nodeid} and UpstreamSide=1) or (Node1Conn = {nodeid} and UpstreamSide=1)  or (Node2Conn = {nodeid} and UpstreamSide=2) '''.format(nodeid =str(int(nodeid))) 
          
       print(query)
       dfsegs= self.osidata.run_query_for_df(query)
       return dfsegs
    
    def GetNextSegment(self,nodeid,direction='down',nodecnt=0):
       #if we want to try bot hat oncedfsegs= self.linesdf[(self.linesdf["Node1Conn"]==nodeid) | (self.linesdf["Node2Conn"]==nodeid)]
       #start = time.time()
       #print(self.LineData.head())
       #print(nodeid)
       if direction =='down':
           #dfsegs= self.LineData.query('("Node1Conn"=={nodeid} and "UpstreamSide"==1) or ("Node2Conn"=={nodeid} & "UpstreamSide"==2)'.format(nodeid=nodeid))
           dfsegs= self.LineData.query('(Node1Conn==@nodeid and UpstreamSide==1) or (Node2Conn==@nodeid & UpstreamSide==2)')
       else:
           if nodecnt != 1:
               #dfsegs= self.LineData[(self.LineData["Node1Conn"]==nodeid & self.LineData["UpstreamSide"]==2) | (self.LineData["Node2Conn"]==nodeid & self.LineData["UpstreamSide"]==1)]
               dfsegs= self.LineData.query('(Node1Conn==@nodeid and UpstreamSide==2) or (Node2Conn==@nodeid & UpstreamSide==1)')
           else:
               #dfsegs= self.LineData[(self.LineData["Node1Conn"]==nodeid & self.LineData["UpstreamSide"]==2) | (self.LineData["Node2Conn"]==nodeid & self.LineData["UpstreamSide"]==1) | (self.LineData["Node1Conn"]==nodeid & self.LineData["UpstreamSide"]==1) | (self.LineData["Node2Conn"]==nodeid & self.LineData["UpstreamSide"]==2)]
               dfsegs= self.LineData.query('(Node1Conn==@nodeid and UpstreamSide==2) or (Node2Conn==@nodeid & UpstreamSide==1) or (Node1Conn==@nodeid & UpstreamSide==1) or (Node2Conn==@nodeid & UpstreamSide==2)')
       #end = time.time()
       #self.tepcore.debug('Query execution time seconds {seconds}'.format(seconds = end - start))
       return dfsegs
            
    def GetLastTracableLine(self,nodeid,direction='down',curdf=None,nodelist=None,nodecnt=0,splitrace=False,splitracenodeid=None):
        print('GetLastTracableLine')
        nextnodeid = nodeid
        if nodelist is None:
            nodelist = []
            nodelist.append(nextnodeid)
        else:
            nodelist.append(nextnodeid)
        if curdf is None:
            trycnt = 0
            while nextnodeid> 0 and trycnt < 50:
                trycnt = trycnt + 1
                newdf = self.GetNextSegment(nextnodeid,direction,nodecnt)#self.GetNextSegmentViaQuery(nextnodeid,direction,nodecnt)
                continuetrace = False            
                if self.tepcore.isValidDf(newdf):
                    if direction=='up'  and len(newdf["Node2Conn"].tolist())>1:
                        print('Lets split this trace up node2s found {cnt}'.format(cnt = len(newdf["Node2Conn"].tolist())))
                        for busindex, busrow in newdf.iterrows():                        
                            #if the device was already traced and not the currnet trace. Continue the the current trace and append on a second trace 
                            if busrow['Node2Conn'] in nodelist and busrow['Node2Conn']  !=nodeid:
                                self.tepcore.debug('Node2Conn already found this node {Node2Conn}'.format(Node2Conn=busrow['Node2Conn']))
                            elif  busrow['Node2Conn']  !=nodeid:
                                self.tepcore.debug('Node2Conn not found  {Node2Conn}'.format(Node2Conn=busrow['Node2Conn']))
                                self.tepcore.debug('We need to trace all these nodes! ')
                                splitrace = True
                                splitracenodeid =  busrow['Node2Conn'] 
                                newtraceindex = busindex
                            elif  busrow['Node2Conn']  ==nodeid:
                                print('We need to continue this trace ')
                                curtracebusindex = busindex
                                continuetrace = True
                    if continuetrace:
                        newdf = newdf[newdf.index==curtracebusindex]
                                                
                    if direction=='up':
                        nodecnt = nodecnt + 1
                    try:
                        node1size =len(newdf["Node1Conn"].tolist())
                        node2size =len(newdf["Node1Conn"].tolist())
                        upstream = newdf["UpstreamSide"].tolist()[0]  
                        if upstream == 2 and direction=='down':
                            #swapping nodes
                            nextnodeid = newdf["Node1Conn"].tolist()[0]
                        elif upstream == 1 and direction=='up':
                            nextnodeid = newdf["Node1Conn"].tolist()[0]
                        else:
                            nextnodeid = newdf["Node2Conn"].tolist()[0]  
                        splitracenodeid, curdf = self.GetLastTracableLine(nextnodeid,direction,newdf,nodelist,nodecnt,splitrace,splitracenodeid)
                    except :
                        nextnodeid = 0
                else:
                    nextnodeid = 0
  
        return splitracenodeid,curdf
                
    def GetNextSegmentForLine(self,upstream,direction,linerow):
        lastlineinsegid = 0
        if upstream == 2 and direction=='up':
            lastlineinsegid = linerow["Node2Conn"]
        elif  upstream == 1 and direction=='up':
            lastlineinsegid = linerow["Node1Conn"]
        elif upstream == 2 and direction=='down':
            lastlineinsegid = linerow["Node1Conn"]
        elif upstream == 1 and direction=='down':
            lastlineinsegid = linerow["Node2Conn"]
        return lastlineinsegid
         
    def GetNextIdForSwitch(self,upstream,direction,curdevice):
         lastlineinsegid = 0
         if upstream == 2 and direction=='up':
             lastlineinsegid = curdevice["Node2Conn"].tolist()[0]
         elif  upstream == 1 and direction=='up':
             lastlineinsegid = curdevice["Node1Conn"].tolist()[0]
         elif upstream == 2 and direction=='down':
             lastlineinsegid = curdevice["Node1Conn"].tolist()[0]   
         elif upstream == 1 and direction=='down':
             lastlineinsegid = curdevice["Node2Conn"].tolist()[0]
         return lastlineinsegid
    
    
    
    def GetConnectedLines(self,nextnodeid, previousnodeid):
        if not self.linesloaded:
            self.LineData = self.GetAllLines(True)
        dfsegs= self.LineData.query('(Node1Conn==@nextnodeid and Node2Conn!=@previousnodeid) or (Node2Conn==@nextnodeid and Node1Conn!=@previousnodeid) ')
        return  dfsegs
        
        
        
    def GetAllLastNodes(self,nodeid,direction) :
        cnter = 0
        nodes = []
        nodepairs= []
        nodepairs.append([nodeid,0,1,0])
        nodepairdfs = pd.DataFrame(nodepairs,columns=['nodeid','previousnodeid','newind','islastsegind'])
        nwenodesdf = nodepairdfs.query('newind==1')
        nextnoedsdf = nwenodesdf
        while self.tepcore.isValidDf(nextnoedsdf) and len(nodes)<10 and cnter<100:
            cnter = cnter + 1
            previousnodeid = 0
            nextnodeid = 0
            nodepairdfs = None
            #print('Restarting our loop')
            #print(nextnoedsdf.head())
            for r in zip(list(zip(*[nextnoedsdf[col] for col in nextnoedsdf]))):
                #print(r[0])
                nextnodeid = r[0][0]
                previousnodeid = r[0][1]
                dfsegs = None
                print('querying {nextnodeid}, with prior node id {previousnodeid} '.format(nextnodeid=nextnodeid,previousnodeid=previousnodeid))
                dfsegs= self.LineData.query('(Node1Conn==@nextnodeid and Node2Conn!=@previousnodeid) or (Node2Conn==@nextnodeid and Node1Conn!=@previousnodeid) ')            
                newnextnodeid= nextnodeid
                newnodepairs=''
                if self.tepcore.isValidDf(dfsegs):
                    newnodepairs = []
                    for inerindex,segsrow in dfsegs.iterrows():
                         newpreviousnodeid = nextnodeid
                         upside = segsrow['UpstreamSide']
                        
                         newnextnodeid = self.GetNextSegmentForLine(upside,direction,segsrow)
                        
                         if [newnextnodeid,newpreviousnodeid,1,0] not in newnodepairs and newpreviousnodeid!=newnextnodeid:
                             
                             newnodepairs.append([newnextnodeid,newpreviousnodeid,1,0])
                     
                         
                        
                else:
                    #nwenodesdf.at[index,'islastsegind']=1
                    if nextnodeid not in nodes:
                        nodes.append(nextnodeid)
                    if previousnodeid not in nodes:
                        nodes.append(previousnodeid)
                    
                previousnodeid = 0
                nextnodeid = 0
                if len(newnodepairs)>0:
                    nodepairdfs = pd.DataFrame(newnodepairs,columns=['nodeid','previousnodeid','newind','islastsegind'])
                    
                else:
                    nodepairdfs = None
                    
                    
            nwenodesdf['newind']=0        
            if self.tepcore.isValidDf(nodepairdfs):
                #print('adding in new node pairs')
                nwenodesdf = nwenodesdf.append(nodepairdfs)
                nodepairdfs=None
                newnodepairs = []
 
                
            nwenodesdf = nwenodesdf.reset_index(drop=True)
            #nodepairdfs['newind']=0
            #if there are no new nodes then set the cnter to exit
            nextnoedsdf = nwenodesdf.query('newind==1')#nodepairdfs
           
            if not self.tepcore.isValidDf(nextnoedsdf):
                #print('No more nodes exiting')
                cnter = 101

                
        #the next connection has no connections so could be line attached to switch.
        if len(nodes)==0:
            nodes.append(nodeid)
        #print(nodepairdfs.head())  
        #print('returninnodes')
        #print(nodes)              
        return nodes
    
    def GetAllTracedSwitches(self,nextnodeid,direction,previousnodeid=0,nodes=None):
        nodes = self.GetAllLastNodes(nextnodeid,direction)
        switchdf = pd.DataFrame()
        for node in nodes:
            #print('finding switches for {node}'.format(node=node))
            switchdf = switchdf.append(self.GetAttachedDevice(node))
        #print(switchdf.head())
        return switchdf
                
            
    
    def TraceObjectsOnFeeder(self, feederdf,direction='down',devicenames='*',devicetype='UGTR'):
        directions=[]
        tracerecnum= 0
        tracerecnum  = self.GetNextTepTraceRecNum()      
        loadsdf = self.GetObject(feederdf, self.LOADOBJECTID)
        self.LoadData =loadsdf
        switchdf = self.GetObject(feederdf, self.SWITCHOBJECTID)
        self.SwitchData = switchdf
        
        self.LineData = self.GetAllLines()#self.GetObject(feederdf, self.LINEOBJECTID)
        self.DeviceData = feederdf.set_index('pDevice')
        
        self.tepcore.debug('device data sample')
        self.tepcore.debug(self.DeviceData.head())
        self.tepcore.debug('device data end')
        self.TraceDataLst = []
        loadsdf.set_index('pDevice',inplace=True)
        tempdf = feederdf.loc[feederdf['object'] == self.LOADOBJECTID]
        tempdf.set_index('pDevice',inplace=True)
        
        mergedf = loadsdf.merge(tempdf, how = 'inner', left_index=True, right_index=True)
        #we can filter on UGTR external id from device info value 1
        #upstream test to knowhere ugloadsdf = mergedf[mergedf['device_name'].str.contains("SSV-2")]
        #ugloadsdf = mergedf[mergedf['device_name'].str.contains("FAP-1")]
        #ugloadsdf = mergedf[mergedf['device_name'].str.contains("MSN-12")]
        #dxv-8 is for three phase
        if devicenames == '*':
            ugloadsdf = mergedf[mergedf['value1'].str.contains(devicetype)]
        else:
            #DXV-8
            ugloadsdf = mergedf[mergedf['device_name'].str.contains(devicenames)]
        if direction =='both':
            directions.append('down')
            directions.append('up')
            
        else:
            directions.append(direction)
            
        self.tepcore.debug('Starting {direction} tracing'.format(direction=direction))
        for index, row in ugloadsdf.iterrows():
            devices = []
            for direction in directions:
                #loads only have one node otherwise would need to use similar logic as below for side and node.
                lastlineinsegid = row["Node1Conn"]
                devicendstrace = False
                nextdeviceid = None
                devicename = row['device_name']
                currentdeviceinforecord = row['pDeviceInfo']
                curdevicerec  = index
                self.tepcore.debug('Starting {direction} trace for {devicename}'.format(devicename=devicename,direction=direction))
                loopcnt = 0
                newidtotrace = None
                while lastlineinsegid > 0 and loopcnt <=50:
                     loopcnt+=1
                     splitrace, curlinedf = self.GetLastTracableLine(lastlineinsegid,direction)
                     if self.tepcore.isValidDf(curlinedf):
                         #on upsream trace a multi phase line can be split into a downstream and upstream trace
                         if direction=='up' and splitrace is not None:
                             #we want to do the downward split trace
                             splitraceid, splitlinedf = self.GetLastTracableLine(splitrace,'down')
                             if self.tepcore.isValidDf(splitlinedf):
                                 print('Found a new line to trace need to set this UPPP')
                             else:
                                 newdevice = self.GetAttachedDevice(splitrace)
                                 if self.tepcore.isValidDf(newdevice):
                                     newidtotrace = self.GetNextIdForSwitch(1,'down',newdevice)
                                     
                                                          
                         for lineindex,linerow in curlinedf.iterrows():
                             upstream = linerow["UpstreamSide"]

                             lastlineinsegid = self.GetNextSegmentForLine(upstream,direction,linerow)
                             curdevice = self.GetAttachedDevice(lastlineinsegid)
                             if self.tepcore.isValidDf(curdevice):
                                 lastlineinsegid = self.GetNextIdForSwitch(upstream,direction,curdevice)
                                 nextdeviceid = curdevice.iloc[0]['pDevice']
                                 #print('switch name:'+ self.GetDeviceNameFromDF(nextdeviceid))
                                 #stop trace on second device change logic to be more like next device with diffrent name.
                                 if devicendstrace:
                                     lastlineinsegid = 0
                                     founddevicename = self.GetDeviceNameFromDF(nextdeviceid)
                                     devices.append([direction,founddevicename,nextdeviceid,devicename,curdevicerec])
                                     self.tepcore.debug('Next {direction} siwtch for {devname} is {downstream}'.format(devname = row['device_name'], downstream=founddevicename, direction=direction))
                                     if newidtotrace is not None:               
                                         lastlineinsegid = newidtotrace
                                         direction = 'down'
                                         newidtotrace = None
                                 else:
                                     devicendstrace = True
                             else:
                                self.tepcore.debug('end of trace no devices attached to line node {lastlineinsegid}'.format(lastlineinsegid=lastlineinsegid))
                                lastlineinsegid = 0
                                nextdeviceid = 0
                                if newidtotrace is not None:
                                    lastlineinsegid = newidtotrace
                                    direction = 'down'
                                    newidtotrace = None
                                    
                                    
                     else:
                          self.tepcore.debug('No lines attahced to device end of trace ')
                          lastlineinsegid = 0
                          print('Can we start with splitted id {splitrace}'.format(splitrace=splitrace))
            if len(devices) > 0:
                #value5 = ','.join(map(str, devices))
                print ('updating device')
                for tracedevice in devices:
                    print(tracedevice)
                    pDevice_src = tracedevice[4]
                    pDevice_dest =  tracedevice[2]
                    Name_src =  tracedevice[3]
                    Name_dest =  tracedevice[1]
                    devdirection = tracedevice[0]
                    self.UpdateTraceInfo(pDevice_src,pDevice_dest,Name_src,Name_dest,devdirection)

        
        
        return True
    
    def BuildDevicesWithNodes(self, devicesdf,usecache=True):
        if not usecache:
            switchdf = self.GetObject(devicesdf, self.SWITCHOBJECTID)
            if not self.tepcore.isValidDf( switchdf):
                self.tepcore.debug('No switches found on circuit')
                return False
            else:
                self.SwitchData = switchdf
        else:
            switchdf = self.GetSwitchDataFromChache()
        switchdf['pDevice'] = switchdf['pDevice'].astype(int)
        switchdf = switchdf.set_index('pDevice')
        if not self.tepcore.isValidDf(switchdf):
            print('switchdf is not valid we have an error')
        else:
            self.tepcore.debug('switch sample')
            self.tepcore.debug(switchdf.dtypes)
            self.tepcore.debug(switchdf.head())
        
        if self.linesloaded==False and usecache==False:
            self.LineData = self.GetAllLines(True)
        linesdf =  self.LineData
        linesdf['pDevice'] = linesdf['pDevice'].astype(int)
        linesdf = linesdf.set_index('pDevice')
        if not self.tepcore.isValidDf(linesdf):
            print('linesdf is not valid we have an error')
        else:
            self.tepcore.debug('lines sample')
            self.tepcore.debug(linesdf.dtypes)
            self.tepcore.debug(linesdf.head())
            
        #get all lines on the feeder
        if not self.tepcore.isValidDf(devicesdf):
            print('devicesdf is not valid we have an error')
            return True
        else:
            print(devicesdf['object'].head(25))
        lineobject = self.LINEOBJECTID
        switchobject = self.SWITCHOBJECTID
        indices = devicesdf[devicesdf['feeder_name'] == 'feeder_name'].index
        devicesdf.drop(indices, inplace=True)
        devicesdf['object'] = devicesdf['object'].astype(int)
        tempdf = devicesdf.query('object ==@lineobject or object==@switchobject')
        
        if not self.tepcore.isValidDf(tempdf):
            print('tempdf is not valid we have an error')
        else:
            print('tempdf sample')

        tempdf.reset_index(inplace=True)
        tempdf['pDevice'] = tempdf['pDevice'].astype(int)
        tempdf.set_index('pDevice',inplace=True)
        
        #Internal lines
        mergedf1 = linesdf.merge(tempdf, how = 'inner', left_index=True, right_index=True)
        
        #Hypernode switches
        mergedf2 = switchdf.merge(tempdf, how = 'inner', left_index=True, right_index=True)
        
        mergedf = pd.concat([mergedf1, mergedf2])
        
        mergedf[[ 'NominalState', 'NominalState_ind_1', 'NominalState_ind_2']] = mergedf[['NominalState','NominalState_ind_1', 'NominalState_ind_2']].astype(str)
        
        mergedf["flag_nominal"] = (
            np.where(
                mergedf['NominalState'] == '1'
                 , np.where(mergedf['NominalState_ind_1'] == '1'
                            , '1'
                            , '0')
                 , np.where(mergedf['NominalState_ind_2'] == '1'
                            , '1'
                            , '0'
                           )
            )
        )
        
        upOpendf = mergedf.query("flag_nominal == '0' and object ==11")
        upOpendf['UpstreamNode'] = upOpendf['Node1Conn']
        upOpendf['DownstreamNode'] = upOpendf['Node2Conn']
        upOpendf['flag'] = 'UpOpen'
        
        downOpendf = mergedf.query("flag_nominal == '0' and object ==11")
        downOpendf['UpstreamNode'] = downOpendf['Node2Conn']
        downOpendf['DownstreamNode'] = downOpendf['Node1Conn']
        downOpendf['flag'] = 'DownOpen'
        
        mergedf['UpstreamNode'] = np.where(mergedf['UpstreamSide']==2, mergedf['Node2Conn'], mergedf['Node1Conn'])
        mergedf['DownstreamNode'] = np.where(mergedf['UpstreamSide']==2, mergedf['Node1Conn'], mergedf['Node2Conn']) 
        mergedf['flag'] = 'Normal'
        
        mergedf = mergedf.append(upOpendf)
        mergedf = mergedf.append(downOpendf)
        
        if not self.tepcore.isValidDf(mergedf):
            print('mergedf is not valid we have an error')
        
        return mergedf
    
    def getUpchildNodes(self, df, parent_id, countr=0):
        list_of_children = []
        list_of_children.append(df[df['DownstreamNode'] == parent_id]['pDevice_dest'].values)
        
        if countr > 100:
            list_of_children =  [item for sublist in list_of_children for item in sublist]
            return list_of_children  
        else:
            for i_, r_ in df[df['DownstreamNode'] == parent_id].iterrows():
                countr=countr+sum(map(len, list_of_children))
                if r_['UpstreamNode'] != parent_id and r_['object'] != 11:
                    list_of_children.append(self.getUpchildNodes(df, r_['UpstreamNode'], countr))
      
        # to flatten the list 
        list_of_children =  [item for sublist in list_of_children for item in sublist]
        return list_of_children
    
    def getDownchildNodes(self, df, parent_id, countr=0):
        list_of_children = []
        list_of_children.append(df[df['UpstreamNode'] == parent_id]['pDevice_dest'].values)
        
        if countr > 100:
            list_of_children =  [item for sublist in list_of_children for item in sublist]
            return list_of_children
        else:
            for i_, r_ in df[df['UpstreamNode'] == parent_id].iterrows():
                countr=countr+sum(map(len, list_of_children))
                if r_['DownstreamNode'] != parent_id and r_['object'] != 11:
                    list_of_children.append(self.getDownchildNodes(df, r_['DownstreamNode'], countr))
      
        # to flatten the list 
        list_of_children =  [item for sublist in list_of_children for item in sublist]
        return list_of_children
        return list_of_children
    
    def TraceDevicesOnFeeder(self, feederdf,deviceWithNodesdf,direction='down',circuit=None,usecache=True,devicenames='internal'):
        directions=[]
        deviceWithNodesdf['pDevice_dest'] = deviceWithNodesdf.index
        objectsToTracedf = feederdf.query("value5==@devicenames or value1.str.contains('HYPNSW')") 
        
        if not self.tepcore.isValidDf(objectsToTracedf):
            print('objectsToTracedf is not valid we have an error')
            self.PerformGenerateNoTracedFiles(circuit)
        else:
            if direction =='both':
                directions.append('down')
                directions.append('up')
            else:
                directions.append(direction)
    
            self.FinalTraceData = pd.DataFrame()
            for index, row in objectsToTracedf.iterrows():
                devices = []
                devicename = str(row['device_name'])
                curdevicerec  = index
                for direct in directions:
                    if direct=='up':
                        temp_df = pd.DataFrame(columns=['parent_id', 'list_of_children'])
                        temp_df['list_of_children'] = pd.Series(self.getUpchildNodes(deviceWithNodesdf, row['UpstreamNode'],0))
                        temp_df['parent_id'] = curdevicerec
                        temp_df['Name_src'] = devicename
                        temp_df['direction'] = direct
                        temp_df['nominal_circuit_src'] = circuit
                        self.FinalTraceData = self.FinalTraceData.append(temp_df)
                    else:
                        temp_df = pd.DataFrame(columns=['parent_id', 'list_of_children'])
                        temp_df['list_of_children'] = pd.Series(self.getDownchildNodes(deviceWithNodesdf, row['DownstreamNode'],0))
                        temp_df['parent_id'] = curdevicerec
                        temp_df['Name_src'] = devicename
                        temp_df['direction'] = direct
                        temp_df['nominal_circuit_src'] = circuit
                        self.FinalTraceData = self.FinalTraceData.append(temp_df)
        
            self.PerformFastUpdateFromDfbyCircuits(circuit)
        
        return True
    
    def TraceInternalWorldUgLinesOnFeeder(self, feederdf,direction='down',circuit=None,usecache=True,devicenames='internal',devicetype='UGTR'):
        directions=[]
        self.TraceDataLst = []
        if not usecache:
            switchdf = self.GetObject(feederdf, self.SWITCHOBJECTID)
            if not self.tepcore.isValidDf( switchdf):
                self.tepcore.debug('No switches found on circuit')
                return False
            else:
                self.SwitchData = switchdf
        else:
            switchdf = self.GetSwitchDataFromChache()
        
        switchdf['pDevice'] = switchdf['pDevice'].astype(int)
        switchdf = switchdf.set_index('pDevice')
        if not self.tepcore.isValidDf(switchdf):
            print('switchdf is not valid we have an error')
        else:
            self.tepcore.debug('switch sample')
            self.tepcore.debug(switchdf.dtypes)
            self.tepcore.debug(switchdf.head())
        
        #self.DeviceData 
       #circdevicedata= feederdf.set_index('pDevice')
        if self.linesloaded==False and usecache==False:
            self.LineData = self.GetAllLines(True)
        linesdf =  self.LineData
        linesdf['pDevice'] = linesdf['pDevice'].astype(int)
        linesdf = linesdf.set_index('pDevice')
        if not self.tepcore.isValidDf(linesdf):
            print('linesdf is not valid we have an error')
        else:
            self.tepcore.debug('lines sample')
            self.tepcore.debug(linesdf.dtypes)
            self.tepcore.debug(linesdf.head())
            
        #get all lines on the feeder
        if not self.tepcore.isValidDf(feederdf):
            print('feederdf is not valid we have an error')
            return True
        else:
            print(feederdf['object'].head(25))
        lineobject = self.LINEOBJECTID
        switchobject = self.SWITCHOBJECTID
        #print(lineobject)
        feederdf['object'] = feederdf['object'].astype(int)
        #pd.to_numeric(feederdf['object'], errors='coerce')
        #print(feederdf.dtypes)
        tempdf = feederdf.query('object ==@lineobject or object==@switchobject')
        
        #.loc[feederdf['object'] == ]
        
        if not self.tepcore.isValidDf(tempdf):
            print('tempdf is not valid we have an error')
        else:
            print('tempdf sample')
         #   print(tempdf.head())
        #tempdf.set_index('pDevice',inplace=True)
        #runs out of memory on 32bit python so force gc on temp dfs    
        #del [[feederdf]]
        #gc.collect()
        #feederdf=pd.DataFrame()
        
        #print(linesdf)
        #print(tempdf)
        tempdf.reset_index(inplace=True)
        tempdf['pDevice'] = tempdf['pDevice'].astype(int)
        tempdf.set_index('pDevice',inplace=True)
        
        #Internal lines
        mergedf1 = linesdf.merge(tempdf, how = 'inner', left_index=True, right_index=True)
        
        #Hypernode switches
        mergedf2 = switchdf.merge(tempdf, how = 'inner', left_index=True, right_index=True)
        
        mergedf = pd.concat([mergedf1, mergedf2])

        if not self.tepcore.isValidDf(mergedf):
            print('mergedf is not valid we have an error')
        #else:
         #   print('sample merged data')
        #    self.tepcore.debug(mergedf.head())
        #3phase example loadsdf
        #ugloadsdf = mergedf.query("value5=='internal' and value1=='UGLN:T.303339460'")
        ugloadsdf = mergedf.query("value5==@devicenames or value1.str.contains('HYPNSW')")
        #self.tepcore.debug(ugloadsdf.head())
        #self.tepcore.debug(ugloadsdf.dtypes)
        #del [[mergedf,tempdf]]
        #gc.collect()
        
        if not self.tepcore.isValidDf(ugloadsdf):
            print('ugloadsdf is not valid we have an error')
        
        if direction =='both':
            directions.append('down')
            directions.append('up')
        else:
            directions.append(direction)
        
        #self.PopulateTepTraceData(usecache) 
       # self.tepcore.debug('Starting {direction} tracing'.format(direction=direction))
        for index, row in ugloadsdf.iterrows():
            devices = []
            devicename = str(row['device_name'])
            #currentdeviceinforecord = row['pDeviceInfo']
            node1conn = row["Node1Conn"]
            node2conn = row["Node2Conn"]
            linupside = row['UpstreamSide']
            curdevicerec  = index
            #print('node1conn {node1conn} node2conn {node2conn}  linupside{linupside}'.format(linupside=linupside,node1conn=node1conn,node2conn=node2conn))
            if linupside > 0:
                for direct in directions: 
                    lastlineinsegid = 0
                    lastlineinsegid = self.GetNextSegmentForLine(linupside,direct,row)
    
                   
                    #print('node1conn {node1conn} node2conn {node2conn}  linupside{linupside}'.format(linupside=linupside,node1conn=node1conn,node2conn=node2conn)) 
                    self.tepcore.debug('Starting {direction} trace for {devicename} using {lastlineinsegid}'.format(devicename=devicename,direction=direct,lastlineinsegid=lastlineinsegid))
                    nextswitches = self.GetAllTracedSwitches(lastlineinsegid, direct)
                    for swindex,swrow in nextswitches.iterrows():
                        devices.append([direct,self.GetDeviceNameFromDF(swrow['pDevice']),swrow['pDevice'],devicename,curdevicerec])
                
            if len(devices) > 0:
                #value5 = ','.join(map(str, devices))
                   
                for tracedevice in devices:
                    pDevice_src = tracedevice[4]
                    pDevice_dest =  tracedevice[2]
                    Name_src =  tracedevice[3]
                    Name_dest =  tracedevice[1]
                    devdirection = tracedevice[0]
                    self.UpdateTraceInfo(pDevice_src,pDevice_dest,Name_src,Name_dest,devdirection,circuit)

        self.PerformFastUpdateFromDf(circuit)
        
        return True
    
    def TraceUpandDownOnFeederByDeviceName(self,feederdf,direction='down',devicename='*'):
        print('Doing a trace by device')
        self.TraceObjectsOnFeeder(feederdf,direction,devicename)
        
    
    
    def ExportToOracle(self):
        dtypes = {'value5':String(250),
        'recnum':Integer, 
        'name': String(250), 
        'value1': String(250)}
        osiquery = '''select edi.value5, d.recnum, d.name,edi.value1 FROM EMAP:EMAP_DEVICE_INFO EDI, EMAP:EMAP_DEVICE d where edi.key5 = 'Traces' and d.pDeviceInfo = edi.recnum'''
        df = self.osidata.run_query_for_df(osiquery)
        gsrdata= tepcoredata('ADMS',self.dbinfo.GetPwd('ADMS'),self.dbinfo.GetDSNInKeePass('ADMS'),True,True)  
        return gsrdata.ReplaceTable(df, 'EMAP_DEVICE_TRACE',dtypes)
    
    
    
    def TraceUgsForCircuits(self,circuits,usecache=True,direction='both'):
        if usecache:
            self.GetAllCaches()
            self.DeviceDataWithNodes = self.BuildDevicesWithNodes(self.DeviceData, usecache)
        count=0
        for circuit in circuits:
            count=count+1
            print(count)
            self.tepcore.debug('Working circuit {circuit}'.format(circuit=circuit))
            if usecache:
                self.tepcore.debug('TraceUgsForCircuits useingchace {cachearg} '.format(cachearg=str(usecache)) )
                feederdf = self.DeviceDataWithNodes.query('feeder_name==@circuit')
            if not self.tepcore.isValidDf(feederdf) and not usecache:
                feederdf = self.GetDevicesOnFeeder(circuit)
            self.tepcore.debug(feederdf.head())
            #self.tepcore.debug(feederdf.dtypes)
            if self.tepcore.isValidDf(feederdf):
                if self.tepcore.isValidDf(self.DeviceDataWithNodes):
                    loadsdf = self.TraceDevicesOnFeeder(feederdf,self.DeviceDataWithNodes,direction,circuit,usecache)
            else:
                self.tepcore.debug('Failed to find devices for circuit {circuit}'.format(circuit=circuit))
            
        return loadsdf
    
    def InsertFast(self,df):
        res = self.osidata.FastDFInsert(df,'TEP_EQUIPMENT:EQUIPMENT')
    def splitDataFrameIntoSmaller(self,df, chunkSize = 10000): 
    	listOfDf = []
    	numberChunks = len(df) // chunkSize + 1
    	for i in range(numberChunks):
    		listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    	return listOfDf	
    
    #cast data of all ints to proper inte type for sql
    def UpdateFast(self,df,table,allints=False):
        df1 = df.reset_index()
        print(df1.head())
        for smallerdf in self.splitDataFrameIntoSmaller(df1,100):
            smallerdf1 = smallerdf.reset_index(drop=True)
            smallerdf1 = smallerdf1.drop('index', 1)
            print(smallerdf1.head())
            res = self.osidata.FastDFUpdate(smallerdf1,table,'recnum',allints)
        return res
    
    
    def CreateatePopFile(self,df,dbnum=182,dbname='TEP_EMAP_DEVICE.DB',outdir='D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\'):
        self.tepcore.debug('CreateatePopFile')
        line1 = '{dbnum} {dbname}'.format(dbnum=str(dbnum),dbname=dbname)
        header = '''*------------------------------------------------------------------------------*
*  Creation Date/Time:  Thu Sep 10 13:08:27 2020
*------------------------------------------------------------------------------*'''


##1	EMAP_TRACE	0	1	2	3	4	5	6	7	8	9	10	11
#*	record	pDevice_src	pDevice_dest	TraceDate	TracedLinesCnt	Name_src	Name_dest	direction	extra	Validity	extra_1	nominal_circuit	current_circuit	
        header1='''1	EMAP_TRACE	0 1 2 4 5	6 10'''
        header2='*record pDevice_src pDevice_dest TraceDate Name_src Name_dest direction nominal_circuit'
        footer1='0'
        footer2='*'
        footer3='0'
        self.tepcore.SafeFileDelete(outdir + dbname + '.DAT')
        outF = open(outdir + dbname + '.DAT', "a+")
        outF.write(line1)
        outF.write("\n")
        outF.write(header1)
        outF.write("\n")
        outF.write(header2)
        outF.write("\n")
        recnum = 1
        for index,row in df.iterrows():
            record=str(row['recnum'])
            pDevice_src=row['pDevice_src']
            pDevice_dest=row['pDevice_dest']
            Name_src=str(row['Name_src'])
            Name_dest =str( row['Name_dest'])
            direction=row['direction']
            validity=str(row['Validity'])
            nominalcircuit = row['nominal_circuit']
            tracedate = row['TraceDate']
            line= '''{record} {pDevice_src} {pDevice_dest} {tracedate} "{Name_src}" "{Name_dest}" "{direction}" "{nominalcircuit}"'''.format(record=recnum, pDevice_src = pDevice_src, pDevice_dest=pDevice_dest, Name_src=Name_src,Name_dest=Name_dest,direction=direction,validity='1',nominalcircuit=nominalcircuit, tracedate=tracedate)
            outF.write(line)
            outF.write("\n")
            recnum = recnum + 1
        outF.write(footer1)
        outF.write("\n")
        outF.write(footer2)
        outF.write("\n")
        outF.write(footer3)
        outF.write("\n")
        outF.close()
        return outdir + dbname + '.DAT'
   
    #Energization Switch is a switch connected with one side Proposed Install and One Side In-Service
    def IsEnergizationSwitch(self, devicerow):
        print(devicerow)
        switchdevice = devicerow['record']
        
        switchdf = self.SwitchData.query('pDevice==@switchdevice')
        for index,row in switchdf.iterrows():
            upstream = row['UpstreamSide']
            upnodeid = self.GetNextSegmentForLine(upstream,'up',row)
            uplinedf = self.GetNextSegment(upnodeid,'up')
            downnodeid = self.GetNextSegmentForLine(upstream,'down',row)
            downlinedf =   self.GetNextSegment(downnodeid,'down')
            if self.tepcore.isValidDf(uplinedf) and self.tepcore.isValidDf(downlinedf) :
                upstatus=None
                downstatus=None
                for uplineindex,uplinerow in uplinedf.iterrows():
                    lindeviceid = uplinerow['pDevice']
                    linedatadf = self.DeviceData.query('record == @lindeviceid')
                    if self.tepcore.isValidDf(linedatadf):
                        for linedataindex,linedatarow in linedatadf.itterows():
                            if upstatus is None:
                                upstatus = linedatarow['value3']
                for downlineindex,downlinerow in downlinedf.iterrows():
                    lindeviceid = downlinerow['pDevice']
                    linedatadf = self.DeviceData.query('record == @lindeviceid')
                    if self.tepcore.isValidDf(linedatadf):
                        for linedataindex,linedatarow in linedatadf.itterows():
                            if downstatus is None:
                                downstatus = linedatarow['value3']
                if upstatus is None or downstatus is None:
                    return False
                else:
                    if (upstatus =='Proposed Install' and downstatus =='In-Service') or (downstatus =='Proposed Install' and upstatus =='In-Service'):
                        return True
                            
            
        
        return False
    def AddEnergizationIndicatorToEmapDevice(self,pDeviceInfoRecord):
        sql = '''UPDATE EMAP:EMAP_DEVICE_INFO SET VALUE4 = '1' WHERE RECNUM = {pDeviceInfoRecord}'''.format(pDeviceInfoRecord=pDeviceInfoRecord)
        #sql = ''' UPDATE EMAP:EMAP_DEVICE SET TAGS = 17 WHERE RECNUM = {pDeviceInfoRecord}'''.format(pDeviceInfoRecord=pDeviceInfoRecord)
        self.tepcore.debug('update sql {sql}'.format(sql=sql))
        self.osidata.runUpdate(sql)
        
    def UpdatePhaseTopology(self,recnum_list,pResetPhase):
        if pResetPhase=='A':
            sql = '''update emap:emap_device set PhaseTPState = 0, NominalTPState = 0 where recnum in ({recnum_list}) '''.format(recnum_list=recnum_list)
        if pResetPhase=='B':
            sql = '''update emap:emap_device set PhaseTPState_ind_1 = 0, NominalTPState_ind_1 = 0 where recnum in ({recnum_list}) '''.format(recnum_list=recnum_list)
        if pResetPhase=='C':
            sql = '''update emap:emap_device set PhaseTPState_ind_2 = 0, NominalTPState_ind_2 = 0 where recnum in ({recnum_list}) '''.format(recnum_list=recnum_list)
        
        self.tepcore.debug('update sql {sql}'.format(sql=sql))
        self.osidata.runUpdate(sql)
        
    def UpdateLAprocessingFlag(self,recnum_list,pFlag):
        sql = '''update emap:emap_source set LAProcessingFlag = {pFlag} where recnum in ({recnum_list}) '''.format(pFlag=pFlag,recnum_list=recnum_list)
        
        self.tepcore.debug('update sql {sql}'.format(sql=sql))
        self.osidata.runUpdate(sql)
        
    def UpdateEnergizationPoints(self):
# =============================================================================
#         self.tepcore.debug('Updating exsiting gis energization points')
#         sql = '''select ed.spare64 ,ed.recnum
#                 from EMAP:EMAP_DEVICE_INFO EDI, EMAP:EMAP_DEVICE ED 
#                 WHERE EDI.key4='ENERGIZATION' and EDI.value4='1' and edi.recnum = ed.pDeviceInfo and ed.spare64=0'''
#         energizepointsgis = self.osidata.run_query_for_df(sql)
#         if self.tepcore.isValidDf(energizepointsgis):
# 
#             energizepointsgis['spare64']=1
#             print (energizepointsgis.dtypes)
#             self.UpdateFast(energizepointsgis,'EMAP:EMAP_DEVICE',True)
#             
#             
#         sqlremove = '''select ed.spare64 ,ed.recnum
#                 from EMAP:EMAP_DEVICE_INFO EDI, EMAP:EMAP_DEVICE ED 
#                 WHERE EDI.key4='ENERGIZATION' and EDI.value4='0' and edi.recnum = ed.pDeviceInfo and ed.spare64=1'''
#         rmenergizepointsgis = self.osidata.run_query_for_df(sqlremove)
#         if self.tepcore.isValidDf(rmenergizepointsgis):
# 
#             rmenergizepointsgis['spare64']=0
#             print (rmenergizepointsgis.dtypes)
#             self.UpdateFast(rmenergizepointsgis,'EMAP:EMAP_DEVICE',True)
# =============================================================================
        return True
        
    def ResetAbnormals(self):
        updatesql = '''update EMAP:EMAP_DEVICE set PhaseState = NominalState,
PhaseState_ind_1 = NominalState_ind_1 ,
PhaseState_ind_2 =NominalState_ind_2
where (NominalState <> PhaseState or PhaseState_ind_1 <> NominalState_ind_1 or PhaseState_ind_2 <> NominalState_ind_2)'''
        #return self.osidata.runUpdate(updatesql)
        print('clearing abnormals has been diabled')
        return True
    
    
    
    def GetMaestroFlagedDevies(self):
        query = '''select   d.Name
             ,d.pFeeder
              ,d.pFeeder_ind_1
                  ,d.pFeeder_ind_2
                  , d.pNominalFeeder
                  ,d.pNominalFeeder_ind_1
                  ,d.pNominalFeeder_ind_2
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                ,d.NominalState
                ,d.NominalState_ind_1  
                ,d.NominalState_ind_2 
                , d.PhaseState
                , d.PhaseState_ind_1
                , d.PhaseState_ind_2
                ,d.PhaseTPState
                ,d.PhaseTPState_ind_1  
                ,d.PhaseTPState_ind_2
                ,d.pDeviceInfo
                ,edi.value1 externalid
                 ,edi.value2
                  ,edi.value3
                   ,edi.value4
                ,edi.value5
                from  EMAP:EMAP_DEVICE d  
                ,  EMAP:EMAP_DEVICE_INFO EDI
                where  d.spare64 = 1 and d.indic=1 and d.object = 11 and d.pDeviceInfo = edi.recnum '''
        flagedf = self.osidata.run_query_for_df(query)
        return flagedf
    
    def GetPhaseStateForDevice(self,record):
        
        query = '''  select recnum record, d.PhaseState
                , d.PhaseState_ind_1
                , d.PhaseState_ind_2 
                from emap:emap_device d where d.recnum = {record}'''.format(record = record)

        return self.osidata.run_query_for_df(query)
                
                
    def GetAdmsAbnormals(self,advancedquery = False):
        
        
      if advancedquery:
          abnormalquery ='''select   d.Name
             ,d.pFeeder
              ,d.pFeeder_ind_1
                  ,d.pFeeder_ind_2
                  , d.pNominalFeeder
                  ,d.pNominalFeeder_ind_1
                  ,d.pNominalFeeder_ind_2
                , d.name device_name
                , d.object
                , d.omsflags
                , d.record
                , d.indic
                , d.validity
                , d.recnum pDevice
                ,d.NominalState
                ,d.NominalState_ind_1  
                ,d.NominalState_ind_2 
                , d.PhaseState
                , d.PhaseState_ind_1
                , d.PhaseState_ind_2
                ,d.PhaseTPState
                ,d.PhaseTPState_ind_1  
                ,d.PhaseTPState_ind_2
                ,d.pDeviceInfo
                ,edi.value1 external_id
                 ,edi.value2
                  ,edi.value3
                   ,edi.value4
                ,edi.value5
                from  EMAP:EMAP_DEVICE d  
                ,  EMAP:EMAP_DEVICE_INFO EDI
                where  d.isabnormal = 1 and d.indic=1 and d.object = 11 and d.pDeviceInfo = edi.recnum '''
      else:
          abnormalquery ='''select recnum, name,object, PhaseState, NominalState , NominalState ,  PhaseState_ind_1 , NominalState_ind_1 , PhaseState_ind_2 , NominalState_ind_2 
          from EMAP:EMAP_DEVICE 
          where (NominalState <> PhaseState or PhaseState_ind_1 <> NominalState_ind_1 or PhaseState_ind_2 <> NominalState_ind_2)'''

      abnormals = self.osidata.run_query_for_df(abnormalquery)
      return abnormals
     

    def RepopulateTraceCacheForCircuit(self,circuit,getFile=True):
        tracedir = self.tepcore.GetConfigItem('tracecachedir')
        #get the current trace data
        self.PopulateTepTraceData(True)
        
        
        #filter out the circuit from current trace data
        self.TraceData = self.TraceData.query('nominal_circuit != @circuit')
        
        self.tepcore.debug(self.TraceData.head())
        if getFile:
            curcircuittracecache = self.GetFileCacheDf(circuit+'maptraces',tracedir)
            #add current trace for circuit            
            if self.tepcore.isValidDf(curcircuittracecache):
                print(curcircuittracecache.head())
                self.TraceData = pd.concat([self.TraceData,curcircuittracecache])
                self.TraceData.reset_index(inplace=True,drop=True)
                self.TraceData['recnum'] = self.TraceData.index
                self.FileCacheDF(self.TraceData,'existingtraces')
            else:
                self.tepcore.debug('no trace cache for {circuit}'.format(circuit=circuit))
                self.tepcore.debug('removing trace for {circuit}'.format(circuit=circuit))
                self.TraceData.reset_index(inplace=True,drop=True)
                self.TraceData['recnum'] = self.TraceData.index
                self.FileCacheDF(self.TraceData,'existingtraces')
        else:
            self.tepcore.debug('removing trace for {circuit}'.format(circuit=circuit))
            self.TraceData.reset_index(inplace=True,drop=True)
            self.TraceData['recnum'] = self.TraceData.index
            self.FileCacheDF(self.TraceData,'existingtraces')
        return True
        
    
    def ClearOSIDB(self,db):
        admsinterface = interface( self.domain, None,None)
        EDIXMLCOMMANDPROC = 'dbclear'
        EDIXMLCOMMANDARGS = ['{db}'.format(db=db)]
        poped= admsinterface.run_process(EDIXMLCOMMANDPROC, EDIXMLCOMMANDARGS, True)
        if poped==0:
             self.tepcore.debug('db clear finished {db}'.format(db=db))
             return True
         
            
         
    def PopulateTracesFromCache(self):
         tracedir = self.tepcore.GetConfigItem('tracecachedir')
         admsinterface = interface( self.domain, None,None)
         dbinfo = teppwcore(self.domain)
         electradata = tepcoredata('ELECTRA',dbinfo.GetPwd('ELECTRA'),dbinfo.GetDSNInKeePass('ELECTRA'))
         admsgisinterface = interface(self.domain, 'https://',electradata)
         self.PopulateTepTraceData(True)
         # Removing missing Nominal Circuits
         self.TraceData.dropna(subset=['nominal_circuit'], inplace=True)
         # Removing duplicate data (grouping by pDevice_src, pDevice_dest and order by TraceDate) keeping the most recent date
         self.TraceData.sort_values(by=['pDevice_src', 'pDevice_dest', 'TraceDate'], inplace=True)
         self.TraceData.drop_duplicates(subset=['pDevice_src', 'pDevice_dest'], keep='last', inplace=True, ignore_index=True)
         # Creating TEP_EMAP_DEVICE.DB.DAT file
         popfile = self.CreateatePopFile(self.TraceData,outdir=tracedir)
         args = []
         args.append('--taskGroup')
         args.append('DBCLEAR182')
         print(args)
         proc = admsgisinterface.run_process('virtuoso_exec',args,True) 
         
         self.tepcore.debug('created populate file at {popfile}'.format(popfile=popfile))
         EDIXMLCOMMANDPROC = 'populate'
         EDIXMLCOMMANDARGS = ['{popfile}'.format(popfile=popfile)]
         poped= admsinterface.run_process(EDIXMLCOMMANDPROC, EDIXMLCOMMANDARGS, True)
         if poped==0:
             self.tepcore.debug('population finished')
             return True
    
            
    def ResetAllSpare64(self,value=0):
        updatesql = 'update emap:emap_device set spare64 = {value} where spare64 = 1'.format( value=value)
        self.tepcore.debug(updatesql)
        return self.osidata.runUpdate(updatesql)
    
    def UpdateSpare64(self,devicerec,value):
        updatesql = 'update emap:emap_device set spare64 = {value} where recnum={record}'.format(record=devicerec, value=value)
        self.tepcore.debug(updatesql)
        return self.osidata.runUpdate(updatesql)
    
    def UpdateSpare49(self,feederrec,value):
        updatesql = 'update emap:emap_device set spare49 = {value} where ( pFeeder={feederrec} or pFeeder_ind_1 = {feederrec}  or pFeeder_ind_2 = {feederrec} or pNominalFeeder={feederrec} or pNominalFeeder_ind_1 = {feederrec}  or pNominalFeeder_ind_2 = {feederrec})'
        
        #could do with top state excluded but need to set on breaker somhow still and  (PhaseTPState <>8 and PhaseTPState_ind_1 <>8  and PhaseTPState_ind_2 <>8)'
        for curfedrecord in feederrec:
           tempsql =  updatesql.format(value=value,feederrec=curfedrecord)
           self.tepcore.debug(tempsql)
           self.osidata.runUpdate(tempsql)
            
    #this is likely not needed lets try to set it on eveything at once by feederrec
    def GetLinesForParallelFeeders(self,feederdf):
        
        query = ''' '''
    
    def GetParallelFeedersAsList(self,df):
        feedersrec= []
        df.columns= df.columns.str.lower()
        print(df.dtypes)
        
        df1 = df['pfeeder'].drop_duplicates().tolist()
        df2 = df['pfeeder_ind_1'].drop_duplicates().tolist()
        df3 = df['pfeeder_ind_2'].drop_duplicates().tolist()
        
        for row in df1:
            currec = row
            if currec not in feedersrec:
                feedersrec.append(currec)
        
        for row in df2:
            currec = row
            if currec not in feedersrec:
                feedersrec.append(currec)
        
            
        for row in df3:
            currec = row
            if currec not in feedersrec:
                feedersrec.append(currec)
        
        return feedersrec
    #move this function to tepinterface,as we are using oracle cache.
    def GetParallelFeeders(self):
        
        getfeeders = ''' select  ed.pFeeder,  
ed.pFeeder_ind_1,  
ed.pFeeder_ind_2, 
  ed.pNominalFeeder,
             ed.pNominalFeeder_ind_1,
             ed.pNominalFeeder_ind_2,
ed.PhaseTPState , 
ed.PhaseTPState_ind_1 , 
ed.PhaseTPState_ind_2 , 
ed.Record, 
ed.recnum  
from emap:emap_device ed 
where (ed.PhaseTPState = 8 or ed.PhaseTPState_ind_1= 8 or ed.PhaseTPState_ind_2  = 8 )
and ed.object = 11
and ed.CustomAttributes = 1
and ed.Indic=1 
and ed.Validity = 1'''
        parallelsdf = self.osidata.run_query_for_df(getfeeders)
        if self.tepcore.isValidDf(parallelsdf):
            self.tepcore.debug(parallelsdf.head())
            return self.GetParallelFeedersAsList(parallelsdf)
        else:
            self.tepcore.debug('no parallel circuits')
            return None
            
        
    def GetEMAPFeederRecFromName(self,name):
        
# =============================================================================
#         query = '''select ef.recnum,
#             ed.pNominalFeeder,
#              ed.pNominalFeeder_ind_1,
#              ed.pNominalFeeder_ind_2,
#             ed.pFeeder,  
#             ed.pFeeder_ind_1,  
#             ed.pFeeder_ind_2
#          from emap:emap_device ed 
#         ,emap:emap_feeder ef where ed.Name = '{name}'
#         and ed.Indic=1 
#         and ed.Validity = 1
#         and ed.object = 11
#         and  (ed.pFeeder = ef.recnum or ed.pFeeder_ind_1 = ef.recnum or  ed.pFeeder_ind_2 = ef.recnum or ed.pNominalFeeder = ef.recnum or ed.pNominalFeeder_ind_1 = ef.recnum or  ed.pNominalFeeder_ind_2 = ef.recnum)'''.format(name=name)
# =============================================================================
        query = ''' select distinct EF.recnum  from  emap:emap_feeder ef1, emap:emap_device ed, emap:emap_feeder ef where (ed.pfeeder =  ef1.recnum  or ed.pfeeder_ind_1 = ef1.recnum
        or ed.pfeeder_ind_2 = ef1.recnum or pnominalfeeder = ef1.recnum or pnominalfeeder_ind_1 = ef1.recnum or pnominalfeeder_ind_2 = ef1.recnum)
        and (ed.pfeeder = ef.recnum or ed.pfeeder_ind_1 = ef.recnum
        or ed.pfeeder_ind_2 = ef.recnum or pnominalfeeder = ef.recnum or pnominalfeeder_ind_1 = ef.recnum or pnominalfeeder_ind_2 = ef.recnum)
        and ef1.name = '{name}' '''.format(name=name)
        
        querydf = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(querydf):
            #self.tepcore.debug(querydf.head())
            return querydf
        else:
            self.tepcore.debug('no parallel circuits')
            return None
        
    
    def GetEMAPDeviceRecFromName(self,name):
        

        query = ''' select distinct ed.recnum  from  emap:emap_device ed where ed.Name ='{name}' '''.format(name=name)
        
        querydf = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(querydf):
            #self.tepcore.debug(querydf.head())
            return querydf['recnum'].tolist()[0]
        else:
            self.tepcore.debug('no device ')
            return None
    
    
    def GetCurrentParallelFlags(self):
         
        query = '''select  ed.pFeeder,  
ed.pFeeder_ind_1,  
ed.pFeeder_ind_2, 
ed.PhaseTPState , 
ed.PhaseTPState_ind_1 , 
ed.PhaseTPState_ind_2 , 
ed.Record, 
ed.recnum  ,
ed.spare49 
from emap:emap_device ed 
where ed.spare49=1
and ed.object = 11
and ed.CustomAttributes = 1
and ed.Indic=1 
and ed.Validity = 1'''
        df = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(df):
            feedersrec= []
            print(df.dtypes)
            df1 = df['pFeeder'].drop_duplicates().tolist()
            df2 = df['pFeeder_ind_1'].drop_duplicates().tolist()
            df3 = df['pFeeder_ind_2'].drop_duplicates().tolist()
            
            for row in df1:
                currec = row
                if currec not in feedersrec:
                    feedersrec.append(currec)
            
            for row in df2:
                currec = row
                if currec not in feedersrec:
                    feedersrec.append(currec)
            
                
            for row in df3:
                currec = row
                if currec not in feedersrec:
                    feedersrec.append(currec)
            
            return feedersrec
        else:
            return []
    
    
    def UpdateAbnormalsFromPoweron(self):
        #type','gisid','urdid','positionid','desc','statea','stateb','statec'
        abnormalcachedf =  self.GetFileCacheDf('poabnormals')
        updatedata = []
        updatesql = '''update EMAP:EMAP_DEVICE set PhaseState = {statea},
                        PhaseState_ind_1 = {stateb} ,
                        PhaseState_ind_2 ={statec} 
                        where recnum = {pDevice}'''
        devcahcedf = self.GetDeviceChache()
        print(devcahcedf.head())
        for index,row in abnormalcachedf.iterrows():
            typeid = row['type']
            gisid = row['gisid']
            statea= row['statea']
            stateb= row['stateb']
            statec= row['statec']
            states = [statea, stateb, statec]
            finalstatea=statea
            finalstateb=stateb
            finalstatec=statec
            if statea == 2 or stateb == 2 or statec == 2:
                print('solving switch states')
                if statea ==2 and (stateb==1 and statec==1):
                    finalstatea=1
                elif  statea ==2 and (stateb==0 and statec==0):
                    finalstatea=0
                elif  statea ==2:
                     
                    if stateb!=2 and statec==2:
                        finalstatea=stateb
                    elif stateb==2 and statec!=2:
                        finalstatea = statec
                if stateb ==2 and (statea==1 and statec==1):
                    finalstateb=1
                elif  stateb ==2 and (statea==0 and statec==0):
                    finalstateb=0
                elif  stateb ==2:
                     
                    if statea!=2 and statec==2:
                        finalstateb=statea
                    elif statea==2 and statec!=2:
                        finalstateb = statec
                if statec ==2 and (statea==1 and stateb==1):
                    finalstatec=1
                elif  stateb ==2 and (statea==0 and stateb==0):
                    finalstatec=0
                elif  statec ==2:
                    
                    if statea<2 and stateb==2:
                        
                        finalstatec=statea
                    elif statea==2 and stateb!=2:
                       
                        finalstatec = stateb
                   
                        

            externalid=typeid+str(gisid)

            
            devicdf = devcahcedf.query("value1==@externalid")
            if self.tepcore.isValidDf(devicdf):
                devicid = devicdf.index[0]
                devicename = devicdf.iloc[0].device_name
                self.tepcore.debug('Changing state of device name {devicename}'.format(devicename=devicename))
                #sql = updatesql.format(statea=finalstatea, stateb=finalstateb,statec=finalstatec, pDevice=devicid )
                #self.tepcore.debug(sql)
                updatedata.append([finalstatea,finalstateb,finalstatec,devicid])
                #self.osidata.runUpdate(sql)
            devicdf=None
                
        updatedf = pd.DataFrame(data = updatedata, columns = ['PhaseState','PhaseState_ind_1','PhaseState_ind_2','recnum'],index=None)
        self.UpdateFast(updatedf,'EMAP:EMAP_DEVICE',True)
        print(updatedf.head())
        #print('updates into adms currently disabled uncomment lines if sync is needed')
        
        
    
    
    def GetMaestroCommitDate(self):
        query = '''select ModelTime from emap:emap_process_info  '''
        #where ImportFlag<>0
        
        df = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(df):
            
            epochtime = df['ModelTime'].tolist()[0]
            otime =datetime.fromtimestamp(epochtime)
            
            return otime
            
        else:
            return None
    def MaestroEmapDBCommiteedCheck(self):
        retrycnt = 1
        commiteddate = self.GetMaestroCommitDate()
        while commiteddate is None and retrycnt <=10:
            time.sleep(20)
            commiteddate = self.GetMaestroCommitDate()
            retrycnt=retrycnt+1
        
        if commiteddate is not None:
            lastepochday =commiteddate.replace(minute=0, hour=0, second=0, microsecond=0)
            curday = datetime.now().replace(minute=0, hour=0, second=0, microsecond=0)
            daysdif = (lastepochday - curday).days
            if daysdif == 0:
                return True
            else:
                return False
        return False
            
    def CheckMaestroStatus(self):
        
        query = '''select ImportFlag from emap:emap_process_info  '''
        #where ImportFlag<>0
        
        df = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(df):
            
            filterdf = df.query('ImportFlag == 0')
            if self.tepcore.isValidDf(filterdf):
                return False
            else:
                return True
            
        else:
            return True
        
    def GetTopologySet(self, pRegion, pDevice):
        if pRegion == 'TEP' and pDevice == 'Line':
            print('Getting EMAP_DEVICE data to reset Topology data on SEGMENTS and TEP AOR Group')
            query = '''select emap:emap_device.recnum, emap:emap_device.PhaseTPState, emap:emap_device.PhaseTPState_ind_1, emap:emap_device.PhaseTPState_ind_2
                        , emap:emap_line.Phase1Conn, emap:emap_line.Phase1Conn_ind_1, emap:emap_line.Phase1Conn_ind_2
                        from emap:emap_device, emap:emap_line
                        where emap:emap_device.pAORGroup = 5
                        and emap:emap_device.object = 8
                        and emap:emap_line.recnum = emap:emap_device.record '''
        elif pRegion == 'TEP' and pDevice == 'Switch':
            print('Getting EMAP_DEVICE data to reset Topology data on SWITCHES/FUSES and TEP AOR Group')
            query = '''select emap:emap_device.recnum, emap:emap_device.PhaseTPState, emap:emap_device.PhaseTPState_ind_1, emap:emap_device.PhaseTPState_ind_2
                        , emap:emap_switch.Phase1Conn, emap:emap_switch.Phase1Conn_ind_1, emap:emap_switch.Phase1Conn_ind_2
                        from emap:emap_device, emap:emap_switch
                        where emap:emap_device.pAORGroup = 5
                        and emap:emap_device.object = 11
                        and emap:emap_switch.switchtype in (0, 2)
                        and emap:emap_switch.recnum = emap:emap_device.record '''
        elif pRegion == 'UNSE' and pDevice == 'Line':            
            print('Getting EMAP_DEVICE data to reset Topology data on SEGMENTS and UNSE AOR Group')
            query = '''select emap:emap_device.recnum, emap:emap_device.PhaseTPState, emap:emap_device.PhaseTPState_ind_1, emap:emap_device.PhaseTPState_ind_2
                        , emap:emap_line.Phase1Conn, emap:emap_line.Phase1Conn_ind_1, emap:emap_line.Phase1Conn_ind_2
                        from emap:emap_device, emap:emap_line
                        where emap:emap_device.pAORGroup = 6
                        and emap:emap_device.object = 8
                        and emap:emap_line.recnum = emap:emap_device.record '''
        elif pRegion == 'UNSE' and pDevice == 'Switch':            
            print('Getting EMAP_DEVICE data to reset Topology data on SWITCHES/FUSES and UNSE AOR Group')
            query = '''select emap:emap_device.recnum, emap:emap_device.PhaseTPState, emap:emap_device.PhaseTPState_ind_1, emap:emap_device.PhaseTPState_ind_2
                        , emap:emap_switch.Phase1Conn, emap:emap_switch.Phase1Conn_ind_1, emap:emap_switch.Phase1Conn_ind_2
                        from emap:emap_device, emap:emap_switch
                        where emap:emap_device.pAORGroup = 6
                        and emap:emap_device.object = 11
                        and emap:emap_switch.switchtype in (0, 2)
                        and emap:emap_switch.recnum = emap:emap_device.record '''
        
        df = self.osidata.run_query_for_df(query)
        if self.tepcore.isValidDf(df):
            # Filtering by Phase1Conn and PhaseTPState and creating new data frames'
            TPStatedf_0 = df.query('Phase1Conn == 0 and PhaseTPState > 0')
            TPStatedf_1 = df.query('Phase1Conn_ind_1 == 0 and PhaseTPState_ind_1 > 0')
            TPStatedf_2 = df.query('Phase1Conn_ind_2 == 0 and PhaseTPState_ind_2 > 0')
            
            # Reseting PhaseTPState, PhaseTPState_ind_1, PhaseTPState_ind_2
            if self.tepcore.isValidDf(TPStatedf_0):
                df_0 = TPStatedf_0[['recnum']]
                recnum_listA = df_0.recnum.unique().tolist()
                recnum_listA = str(recnum_listA)[1:-1]
                # Reseting A phase
                self.UpdatePhaseTopology(recnum_listA,'A')
            if self.tepcore.isValidDf(TPStatedf_1):
                df_1 = TPStatedf_1[['recnum']]
                recnum_listB = df_1.recnum.unique().tolist()
                recnum_listB = str(recnum_listB)[1:-1]
                # Reseting B phase
                self.UpdatePhaseTopology(recnum_listB,'B')
            if self.tepcore.isValidDf(TPStatedf_2):
                df_2 = TPStatedf_2[['recnum']]
                recnum_listC = df_2.recnum.unique().tolist()
                recnum_listC = str(recnum_listC)[1:-1]
                # Reseting C phase
                self.UpdatePhaseTopology(recnum_listC,'C')
            
            return True
        else:
            return False
        
    def SetLAprocessingflag(self, pRegion):
        if pRegion == 'TEP' :
            print('Getting EMAP_DEVICE data and TEP AOR Group')
            query_device = '''select emap:emap_device.recnum recnum_device, emap:emap_device.pSource, emap:emap_device.record
                        from emap:emap_device
                        where emap:emap_device.pAORGroup = 5
                        and emap:emap_device.object = 11 '''
            print('Getting EMAP_SWITH data and switch type BREAKER')
            query_switch = '''select emap:emap_switch.recnum recnum_switch, emap:emap_switch.pNode2MeasGroup
                        from emap:emap_switch
                        where emap:emap_switch.switchtype = 1 '''
            print('Getting EMAP_MEAS_GROUP data')
            query_measgroup = '''select emap:emap_meas_group.recnum recnum_measgroup, emap:emap_meas_group.pMeasPhaseKW, emap:emap_meas_group.pMeasPhaseKW_ind_1, emap:emap_meas_group.pMeasPhaseKW_ind_2
                        , emap:emap_meas_group.pMeasPhaseKVAR, emap:emap_meas_group.pMeasPhaseKVAR_ind_1, emap:emap_meas_group.pMeasPhaseKVAR_ind_2
                        from emap:emap_meas_group '''
        
        df_device = self.osidata.run_query_for_df(query_device)
        df_switch = self.osidata.run_query_for_df(query_switch)
        df_measgroup = self.osidata.run_query_for_df(query_measgroup)
        
        df_device_switch = pd.merge(df_device, df_switch, left_on='record', right_on='recnum_switch')
        df_device_switch_measgroup = pd.merge(df_device_switch, df_measgroup, how='left', left_on='pNode2MeasGroup', right_on='recnum_measgroup')
        
        if self.tepcore.isValidDf(df_device_switch_measgroup):
            print(df_device_switch_measgroup)
            # filter to find the set of sources that have at least one scada link on KW and KVAR by Phases -- Set Flag to 1
            df_sources_wscadaphases = df_device_switch_measgroup.query('pMeasPhaseKW > 0 or pMeasPhaseKW_ind_1 > 0 or pMeasPhaseKW_ind_2 > 0 or pMeasPhaseKVAR > 0 or pMeasPhaseKVAR_ind_1 > 0 or pMeasPhaseKVAR_ind_2 > 0')
            if self.tepcore.isValidDf(df_sources_wscadaphases):
                df_source = df_sources_wscadaphases[['pSource']]
                recnum_source = df_source.pSource.unique().tolist()
                recnum_source = str(recnum_source)[1:-1]
                self.UpdateLAprocessingFlag(recnum_source,'1')
            # filter to find the set of sources that have relation with meas group but don't have scada link on KW and KVAR by Phases -- Reset Flag to 0 (in case the scada link was removed)
            df_sources_wscadanophases = df_device_switch_measgroup.query('pMeasPhaseKW == 0 and pMeasPhaseKW_ind_1 == 0 and pMeasPhaseKW_ind_2 == 0 and pMeasPhaseKVAR == 0 and pMeasPhaseKVAR_ind_1 == 0 and pMeasPhaseKVAR_ind_2 == 0')
            if self.tepcore.isValidDf(df_sources_wscadanophases):
                df_source = df_sources_wscadanophases[['pSource']]
                recnum_source = df_source.pSource.unique().tolist()
                recnum_source = str(recnum_source)[1:-1]
                self.UpdateLAprocessingFlag(recnum_source,'0')
            # filter to find the set of sources that don't have any relation with the scada links -- Reset Flag to 0 (in case the scada link was removed)
            df_sources_wscadanan = df_device_switch_measgroup.query('recnum_measgroup.isna()')
            if self.tepcore.isValidDf(df_sources_wscadanan):
                df_source = df_sources_wscadanan[['pSource']]
                recnum_source = df_source.pSource.unique().tolist()
                recnum_source = str(recnum_source)[1:-1]
                self.UpdateLAprocessingFlag(recnum_source,'0')
            return True
        else:
            return False    
        
    def FindInvalidLineCuts(self):
        #'invaldi line cuts on emap:emap_line_cut'
        query = ''' '''
    
    
    def ClearBadTEmps(self, df):
        

        delline = 'delete from emap:emap_line where recnum ={linerec}'
        delldev = 'delete from emap:emap_device where recnum ={linerec}'
        dellcut = 'delete from emap:emap_line_cut where recnum ={linerec}'

        for index, row in df.iterrows():
            obj= row['object']
            if obj==7:
                dbcleartemp = delldev.format(linerec= row['bad_record'])
                self.osidata.runUpdate(dbcleartemp)
            elif obj == 8:
                dbcleartemp = delline.format(linerec= row['bad_record'])
                self.osidata.runUpdate(dbcleartemp)
            elif obj == 30:
                dbcleartemp = dellcut.format(linerec= row['bad_record'])
                self.osidata.runUpdate(dbcleartemp)
                dbcleartemp = delline.format(linerec= row['emap_line_recnum'])
                self.osidata.runUpdate(dbcleartemp)
                dbcleartemp =  delline.format(linerec= row['side_two_line'])
                self.osidata.runUpdate(dbcleartemp)
                if not math.isnan(row['pDevice']):
                    dbcleartemp = delldev.format(linerec= row['pDevice'])
                self.osidata.runUpdate(dbcleartemp)

    
    def CreateFixScriptInvalidLines(self, df,outfile):
        
        self.tepcore.debug('Creating bad line fix file {outfile}'.format(outfile=outfile))
        delline = 'delete from emap:emap_line where recnum ={linerec} \n'
        delldev = 'delete from emap:emap_device where recnum ={linerec} \n'
        dellcut = 'delete from emap:emap_line_cut where recnum ={linerec} \n'
        with open(outfile, "a") as file_object:
            # Append 'hello' at the end of file
            file_object.write('dbdump 91 > D:\\osi\\osi_cust\\data\\91_bkup_linefix.dat \n')
            file_object.write('odbc_sql \n')
            for index, row in df.iterrows():
                obj= row['object']
                if obj==7:
                    dbcleartemp = delldev.format(linerec= row['bad_record'])
                elif obj == 8:
                    dbcleartemp = delline.format(linerec= row['bad_record'])
                elif obj == 30:
                    dbcleartemp = dellcut.format(linerec= row['bad_record'])
                    dbcleartemp = dbcleartemp +  delline.format(linerec= row['emap_line_recnum'])
                    dbcleartemp = dbcleartemp +  delline.format(linerec= row['side_two_line'])
                    dbcleartemp = dbcleartemp +  delldev.format(linerec= row['pDevice'])
                file_object.write(dbcleartemp)
            file_object.write('osii_emap_validate \n')
            
    
    

    
    
    
    def FindInvalidTempLines(self):
        #invalid temp lines tend to have a record that points to emap_device where that emap device is pointing to a diffrent emap_line
        #on emap validate report as "ERROR (Validate Device Cross-Reference): EMAP_LINE record 1798403: pDevice refers to EMAP_DEVICE record that refers to incorrect object 8"
        fres = None
        querytemdevices = '''
                select ed.recnum, ed.record, ed.validity
                from
                emap:emap_device ed
                where ed.recnum >= 1998000
                and ed.indic = 1'''
                            
                            
        querytemplines = '''select el.recnum, el.pDevice, el.validity from
                                emap:emap_line el 
                                where el.pDevice >= 1998000
                                and el.indic = 1'''
                                
        dfdevice = self.osidata.run_query_for_df(querytemdevices)
        dflines = self.osidata.run_query_for_df(querytemplines)
        if self.tepcore.isValidDf(dfdevice) and self.tepcore.isValidDf(dflines):
           mergedset =  dflines.merge(dfdevice, left_on =['pDevice'] , right_on = ['recnum'], how='left', indicator=True)
           #print(mergedset.head())
           results = mergedset.query('record != recnum_x')
           results.rename(columns={'recnum_x':'bad_record'},inplace=True)
           results= results[['bad_record']]
           results['object']=8
           results['issue'] = 'ERROR (Validate Device Cross-Reference): EMAP_LINE record x: pDevice refers to EMAP_DEVICE record that refers to incorrect object 8'
           #print(results.head())
           #find EMAP_DEVICE record 1998006: Record field references EMAP_LINE record 1798408 which references a different EMAP_DEVICE record"
           mergedset1 =  dfdevice.merge(dflines, left_on =['record'] , right_on = ['recnum'], how='left', indicator=True)
           results1 = mergedset1.query('''_merge == 'both' and recnum_x != pDevice''')
           print('find EMAP_DEVICE record x: Record field references EMAP_LINE record x which references a different EMAP_DEVICE record"')
           #print(results1.head(15))
           
           results1.rename(columns={'recnum_x':'bad_record'},inplace=True)
           results1= results1[['bad_record']]
           results1['object']=7
           results1['issue'] = 'EMAP_DEVICE record x: Record field references EMAP_LINE record x which references a different EMAP_DEVICE record'
           #print(results1.head())
           
           
           
           ### this error may also ocure if emap_line that matches has validity flag != 1
           #EMAP_DEVICE record 1998008: Field 27 index 0 refers to invalid line record 1798410."
           #print('mergeset 1')
           #print(mergedset1.head(50))
           results2= mergedset1.query('''(_merge == 'left_only' and recnum_x != pDevice) or (validity_x != validity_y and recnum_x != pDevice)''')
           #print(results2.head(15))
           results2.rename(columns={'recnum_x':'bad_record'},inplace=True)
           results2= results2[['bad_record']]
           results2['object']=7
           results2['issue'] = 'EMAP_DEVICE record x: Field 27 index 0 refers to invalid line record x'
           #print(results2.head())
           
           if self.tepcore.isValidDf(results):
               if self.tepcore.isValidDf(fres):
                   fres = fres.append(results, ignore_index=True)
               else :
                   fres = results
           
           if self.tepcore.isValidDf(results1):
               if self.tepcore.isValidDf(fres):
                   fres = fres.append(results1, ignore_index=True)
               else :
                   fres = results1
               
           if self.tepcore.isValidDf(results2):
               if self.tepcore.isValidDf(fres):
                   fres = fres.append(results2, ignore_index=True)
               else :
                   fres = results2
           return fres
    
    def FindBadCuts(self):
        fres = None
        queryside1 = '''select pLine1 emap_line_recnum , recnum bad_record, pLine2  side_two_line from emap:EMAP_LINE_CUT elc where elc.indic = 1'''
        queryside2= '''select pLine2 emap_line_recnum , recnum bad_record , pLine1  side_two_line from emap:EMAP_LINE_CUT elc where elc.indic = 1'''
        querylines = ''' select el.recnum, el.pDevice, el.validity from
                                emap:emap_line el 
                                where el.indic = 1'''
        invalidtemplines = self.FindInvalidTempLines()
        
        queryside1res = self.osidata.run_query_for_df(queryside1)
        queryside2res = self.osidata.run_query_for_df(queryside2)
        querylinesres = self.osidata.run_query_for_df(querylines) 
        
        if self.tepcore.isValidDf(queryside1res) and self.tepcore.isValidDf(queryside2res) :
            if self.tepcore.isValidDf(invalidtemplines):
                badlines = invalidtemplines.query('object == 8')
                if self.tepcore.isValidDf(badlines):
                    badmatchres  = queryside1res.merge(badlines, left_on =['emap_line_recnum'] , right_on = ['bad_record'], how='inner', indicator=True)
                    if self.tepcore.isValidDf(badmatchres):
                       badmatchres['error'] = 'temp cut  pline1 connected to what appears to be invalid line'
                       if self.tepcore.isValidDf(fres):
                           fres = fres.append(badmatchres, ignore_index=True)
                       else :
                           fres = badmatchres
               
                    badmatchres1  = queryside2res.merge(badlines, left_on =['emap_line_recnum'] , right_on = ['bad_record'], how='inner', indicator=True)
                    if self.tepcore.isValidDf(badmatchres1):
                       badmatchres1['error'] = 'temp cut pLine2  connected to what appears to be invalid line'
                       if self.tepcore.isValidDf(fres):
                           fres = fres.append(badmatchres1, ignore_index=True)
                       else :
                           fres = badmatchres1
            
            
            if self.tepcore.isValidDf(querylinesres):
                   self.tepcore.debug('Check emap line is valid for temp cut sides')
                   badmatchres1  = queryside1res.merge(querylinesres, left_on =['emap_line_recnum'] , right_on = ['recnum'], how='left', indicator=True)
                   
                   badmatchres1fin = badmatchres1.query('''_merge == 'left_only' or (_merge == 'both' and validity != 1)  ''')
                   badmatchres2  = queryside2res.merge(querylinesres, left_on =['emap_line_recnum'] , right_on = ['recnum'], how='left', indicator=True) 
                   badmatchres2fin = badmatchres2.query('''_merge == 'left_only'  or (_merge == 'both' and validity != 1)''')
                   if self.tepcore.isValidDf(badmatchres1fin):
                       print(badmatchres1fin.head())
                       badmatchres1fin['error'] = 'temp cut pLine1 is invalid'
                       if self.tepcore.isValidDf(fres):
                           fres = fres.append(badmatchres1fin, ignore_index=True)
                       else :
                           fres = badmatchres1fin
                     
                   
                   if self.tepcore.isValidDf(badmatchres2fin):
                       print(badmatchres2fin.head())
                       badmatchres2fin['error'] = 'temp cut pLine2 is invalid'
                       if self.tepcore.isValidDf(fres):
                           fres = fres.append(badmatchres2fin, ignore_index=True)
                       else :
                           fres = badmatchres2fin

            if self.tepcore.isValidDf(fres):
                fres= fres[['bad_record','emap_line_recnum', 'error','pDevice', 'side_two_line']]
                fres['object']=30
                
            return fres
       
    def GetAbnormalPropsedInstall(self,retrycnt=0):
          querypropsoedabnormals = '''select ed.name, ed.recnum, ed.NominalState, ed.NominalState_ind_1, ed.NominalState_ind_1, ed.NominalState_ind_2, ed.PhaseState, ed.PhaseState_ind_1, ed.PhaseState_ind_2,
                                        ed.IsAbnormal,edi.value1, ed.DisplayTags,ec.isGanged from emap:emap_device ed,
                                        emap:emap_device_info edi,
                                        emap:emap_switch ec
                                        where ed.IsAbnormal <> 0
                                        and edi.recnum = ed.pDeviceInfo
                                        and edi.value3='Proposed Install'
                                        and ec.recnum = ed.record
                                        and ed.object = 11'''
          queryside1res = self.osidata.run_query_for_df(querypropsoedabnormals)
          
          if self.tepcore.isValidDf(queryside1res):
              print('')
              return queryside1res
          else:
              if type(queryside1res)==bool:
                  if queryside1res == False and retrycnt<= 10:
                      self.tepcore.debug('retyiing query')
                      time.sleep(10)
                      retrycnt=retrycnt+1
                      self.ReconnectOSiData()
                      return self.GetAbnormalPropsedInstall(retrycnt)
                  else:
                      return None
              return None
                  
          
    def UpdateAbnormalPropsedInstall(self,updatedf,astate=-1,bstate=-1,cstate=-1)   :
        
        curupdatedtemplate=None
        updatetemplatethreephase = ''' update 
                             emap:emap_device
                            set PhaseState = {astate},
                            PhaseState_ind_1 = {bstate} ,
                            PhaseState_ind_2 ={cstate}
                            where IsAbnormal <> 0
                            and recnum in ({inclause})
                            and object = 11'''
                            
                            
        updatetemplateaphase=''' update 
                             emap:emap_device
                            set PhaseState = {astate}
                            where IsAbnormal <> 0
                            and recnum in ({inclause})
                            and object = 11'''
        
        
        updatetemplatebphase=''' update 
                             emap:emap_device
                            set PhaseState_ind_1 = {bstate}
                            where IsAbnormal <> 0
                            and recnum in ({inclause})
                            and object = 11'''
                            
        updatetemplatecphase=''' update 
                             emap:emap_device
                            set PhaseState_ind_2 = {cstate}
                            where IsAbnormal <> 0
                            and recnum in ({inclause})
                            and object = 11'''
        
        if astate >=0 and bstate >=0 and cstate>=0:
            curupdatedtemplate = updatetemplatethreephase
        elif astate >=0 and bstate ==-1 and cstate==-1:
            curupdatedtemplate = updatetemplateaphase
        elif astate ==-1 and bstate >=0 and cstate==-1:
            curupdatedtemplate = updatetemplatebphase
        elif astate ==-1 and bstate ==-1 and cstate>=0:
            curupdatedtemplate = updatetemplatecphase
            
      
                            
                            
        reqlist = updatedf['recnum'].tolist()
        updatestmts = []
       
        if len(reqlist) > 200:
           chunks =  self.tepcore.SplitArray(reqlist,200)
           for chunk in chunks:
               inclause = ','.join([str(i) for i in chunk])
               if astate >=0 and bstate >=0 and cstate>=0:
                   updatestatement = curupdatedtemplate.format(inclause=inclause,astate=astate,bstate=bstate,cstate=cstate)
               elif astate >=0 and bstate ==-1 and cstate==-1:
                   updatestatement = curupdatedtemplate.format(inclause=inclause,astate=astate)
               elif astate ==-1 and bstate >=0 and cstate==-1:
                   updatestatement = curupdatedtemplate.format(inclause=inclause,bstate=bstate)
               elif astate ==-1 and bstate ==-1 and cstate>=0:
                   updatestatement = curupdatedtemplate.format(inclause=inclause,cstate=cstate)
               
               updatestmts.append(updatestatement)
        else:
           inclause = ','.join([str(i) for i in reqlist])
           if astate >=0 and bstate >=0 and cstate>=0:
               updatestatement = curupdatedtemplate.format(inclause=inclause,astate=astate,bstate=bstate,cstate=cstate)
           elif astate >=0 and bstate ==-1 and cstate==-1:
               updatestatement = curupdatedtemplate.format(inclause=inclause,astate=astate)
           elif astate ==-1 and bstate >=0 and cstate==-1:
               updatestatement = curupdatedtemplate.format(inclause=inclause,bstate=bstate)
           elif astate ==-1 and bstate ==-1 and cstate>=0:
               updatestatement = curupdatedtemplate.format(inclause=inclause,cstate=cstate)
           updatestmts.append(updatestatement)
           
           
        for update in updatestmts:
           retrycnt = 0
           print(update)
           updateres = self.osidata.runUpdate(update)
           while not updateres and retrycnt <= 20:
               print('returning updated')
               time.sleep(10)
               retrycnt = retrycnt + 1
               self.ReconnectOSiData()
               updateres = self.osidata.runUpdate(update)
               
               
        
        
    def ProcessPIAbnormalsPerPhase(self,abnormaldf):
        
        
        gangednomopen = abnormaldf.query('''isGanged ==1 and (NominalState == 0 and NominalState_ind_1 == 0 or NominalState_ind_2 == 0 ) and DisplayTags==0''')
        gangednomclose = abnormaldf.query('''isGanged ==1 and (NominalState == 1 and NominalState_ind_1 == 1 or NominalState_ind_2 == 1) and DisplayTags==0''')
        
        
        anomclose = abnormaldf.query('''isGanged ==0 and (NominalState == 1 and PhaseState ==0) and DisplayTags==0''')
        bnomclose = abnormaldf.query('''isGanged ==0 and ( NominalState_ind_1 == 1 and PhaseState_ind_1 ==0) and DisplayTags==0''')
        cnomclose = abnormaldf.query('''isGanged ==0 and (NominalState_ind_2 == 1 and PhaseState_ind_2 ==0) and DisplayTags==0''')
        
        
        anomopen = abnormaldf.query('''isGanged ==0 and (NominalState == 0 and PhaseState ==1) and DisplayTags==0''')
        bnomopen = abnormaldf.query('''isGanged ==0 and ( NominalState_ind_1 == 0 and PhaseState_ind_1  ==1) and DisplayTags==0''')
        cnomopen = abnormaldf.query('''isGanged ==0 and (NominalState_ind_2 == 0 and PhaseState_ind_2 ==1) and DisplayTags==0''')
        
        
        if self.tepcore.isValidDf(gangednomopen):
            self.tepcore.debug('processing abnormal open gang opreated')
            self.UpdateAbnormalPropsedInstall(gangednomopen,0,0,0)
        
        
        if self.tepcore.isValidDf(gangednomclose):
            self.tepcore.debug('processing abnormal close gang opreated')
            self.UpdateAbnormalPropsedInstall(gangednomclose,1,1,1)
            
            
        if self.tepcore.isValidDf(anomclose):
            self.tepcore.debug('processing abnormal close a phase')
            self.UpdateAbnormalPropsedInstall(anomclose,1)
        
        
        if self.tepcore.isValidDf(bnomclose):
            self.tepcore.debug('processing abnormal close b phase')
            self.UpdateAbnormalPropsedInstall(bnomclose,-1,1)
            
            
        if self.tepcore.isValidDf(cnomclose):
            self.tepcore.debug('processing abnormal close c phase')
            self.UpdateAbnormalPropsedInstall(cnomclose,-1,-1,1)
            
            
        if self.tepcore.isValidDf(anomopen):
            self.tepcore.debug('processing abnormal open a phase')
            self.UpdateAbnormalPropsedInstall(anomopen,0)
        
        
        if self.tepcore.isValidDf(bnomopen):
            self.tepcore.debug('processing abnormal open b phase')
            self.UpdateAbnormalPropsedInstall(bnomopen,-1,0)
            
            
        if self.tepcore.isValidDf(cnomopen):
            self.tepcore.debug('processing abnormal open c phase')
            self.UpdateAbnormalPropsedInstall(cnomopen,-1,-1,0)
            
            
        return True
            
           
            
        
        
        
        
        
        