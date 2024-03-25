# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 13:37:11 2020

@author: aa58436
"""
import pandas as pd
import cx_Oracle
from tepcore import tepcore
from sqlalchemy import create_engine,types,engine
from sqlalchemy.sql import text as sa_text
from timeit import default_timer as timer
from datetime import datetime
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
import concurrent.futures
import time
import json
from sqlalchemy import text
import urllib
import pyodbc
from urllib.parse import quote_plus
class tepcoredata:
    def __init__(self,user,password,dbname,debug=False,debugsql=False,DSN=None,SQLSCHEMA=None):
        self.user=user
        self.password=password
        self.dbname=dbname
        self.tepcore = tepcore(debug)
        self.debugsql = debugsql
        self.cnxn=None
        self.osiconn=False
        self.debug = debug
        self.odbcconnected=False
        self.postgressconnected = False
        #connection pool
        try:
            #self.db_pool = cx_Oracle.SessionPool(user=self.user, password=self.password, dsn=self.dbname, min=1, max=20, increment=1, threaded=True)
            #dbconn = db_pool.acquire()#cx_Oracle.Connection(dsn=dbname, pool=db_pool)
            #connection pool for sql qlchemy used for inserts
            if DSN is None:
                if self.user =='sqllite':
                    self.engine = create_engine('sqlite:///{dbname}'.format(dbname=self.dbname))
                
                else:
                    print('attempting to connect to with {user} {dbname}'.format(dbname=self.dbname, user=self.user))
                    self.engine = create_engine('oracle://'+self.user+':'+quote_plus(self.password)+'@' + self.dbname,pool_size=10,echo=debugsql, pool_pre_ping=True)

            else:
                #autocommit tricks it to not try to set auto commit setting not supported by the driver it doesnt mean it is auto commiting
                
                self.dsn = DSN
                if 'PIAFSQL' in self.dsn:

                    #print('attempting to connect into DSN: ' + DSN)
                    if SQLSCHEMA is None:
                        params = urllib.parse.quote_plus('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+dbname+';UID='+user+';PWD='+ password+';TrustServerCertificate=yes')
                    else:
                        params = urllib.parse.quote_plus('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+dbname+';UID='+user+';PWD='+ password+';TrustServerCertificate=yes;' + 'Database={SQLSCHEMA};'.format(SQLSCHEMA=SQLSCHEMA))
                    #print(params)
                    self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)

                    self.odbcconnected=True 
                elif 'POSTGRESS' in self.dsn:
                    self.postgressconnected= True
                    self.engine = create_engine('postgresql+psycopg2://'+self.user+':'+self.password+'@' + self.dbname,connect_args={'sslmode': "allow"})
                else:
                    try:
                        self.cnxn = pyodbc.connect("DSN={DSN}".format(DSN=DSN),autocommit=True)
                        self.odbcconnected=True 
                    except Exception as e:
                        print(str(e))
                    #connect_args = {'autocommit': True}
                    #self.engine =  create_engine("mssql+pyodbc://{DSN}".format(DSN=DSN),connect_args=connect_args,pool_size=10,echo=debugsql, pool_pre_ping=True)
                    self.osiconn=True
                    #sql = "Select Key From SCADA:ANALOG where recnum=1"
                    #data = pd.read_sql(sql,self.cnxn )
                    #self.cnxn.close()
                    self.tepcore.debug('Using osi odbc with dsn name {DSN}'.format(DSN=self.dsn))
        except (cx_Oracle.OperationalError, cx_Oracle.DatabaseError, cx_Oracle.InterfaceError) as e:
            tepcore.Debug('Failed to connect to oracle')
            tepcore.Debug(str(e))
    
    
    def __del__(self):
        if self.osiconn:
            self.cnxn.close()
    def GetConnection(self):
        if self.osiconn:
            if self.odbcconnected:
                return self.cnxn#pyodbc.connect("DSN={DSN}".format(DSN=self.dsn),autocommit=True)
            else:
                self.odbcconnected = True
                return self.GetNewConnection()
        else:
            #seems to be some issue with autocommit attributes on odbc sql server
            if self.odbcconnected:
                return self.engine.connect().execution_options(autocommit=False)
            else:
                return self.engine.connect().execution_options(autocommit=True)
    
    def GetNewConnection(self):
        if self.osiconn:
            if self.odbcconnected:
                    self.tepcore.Debug('Attempting to close osi connection')
                    self.cnxn.close()
                    self.cnxn = None
                    self.cnxn = pyodbc.connect("DSN={DSN}".format(DSN=self.dsn),autocommit=True)
                    self.tepcore.Debug('closed and created new osi connection')

            return self.cnxn
        else:
            return self.engine.connect().execution_options(autocommit=True)
        
        
    def multip_proc_table_replace_df(self,dfin, table):
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        now = datetime.now()
        self.tepcore.Debug('start time insert :{now}'.format(now=now))
        start = timer()
        alqcon = self.engine.connect()
        self.engine.execute(sa_text('''TRUNCATE TABLE {table}'''.format(table=table)).execution_options(autocommit=True))
        dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table, con=alqcon, index=False,if_exists='append',index_label=None)
        alqcon.close()
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start))    
        return True

    def InsertNewTable(self,dfin, table,dtypes=None):
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        now = datetime.now()
        msg='start time insert :{now}'.format(now=now)
        self.tepcore.Debug(msg)
        start = timer()
        alqcon = self.engine.connect()
        dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table, con=alqcon, index=False,if_exists='replace',index_label=None,dtype=dtypes)
        alqcon.close()
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start))    
        return True        
    
    def ReplaceTable(self,dfin, table,dtypes=None,replancenan=True):
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        now = datetime.now()
        msg='start time insert :{now}'.format(now=now)
        self.tepcore.Debug(msg)
        start = timer()
        alqcon = self.engine.connect()
        if replancenan:
            dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table.lower(), con=alqcon, index=False,if_exists='replace',index_label=None,dtype=dtypes)
        alqcon.close()
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start))    
        return True        
    
    def AppendTable(self,dfin, table,dtypes=None):
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        now = datetime.now()
        msg='start time insert :{now}'.format(now=now)
        self.tepcore.Debug(msg)
        start = timer()
        try:
            alqcon = self.engine.connect()
            dfin.replace(np.nan, '', regex=True,inplace=True)
            dfin.to_sql(name=table.lower(), con=alqcon, index=False,if_exists='append',index_label=None,dtype=dtypes)
            alqcon.close()
        except Exception as e:
            self.tepcore.debug('exception occured')
            self.tepcore.debug(str(e))
            return False
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start))    
        return True 
  # ARGS are a collection []       
    def ExecuteOracleProcedure(self,proc, args=None):
        success =False
        con = self.engine.raw_connection()
        try:
            
            if args is not None:
                con.cursor().callproc(proc,args)
            else:
                con.cursor().callproc(proc)
            self.tepcore.Debug('object exists')
            success = True
        except Exception as e:
            self.tepcore.Debug(str(e))
            success = False
            self.tepcore.debug(proc )
            self.tepcore.debug(args )
            
        finally:    
            con.close ()
        return success
    
    
    def ExecuteOracleViewRefreshParallel(self,view):
        

        return [self.ExecuteOracleProcedure('adms.adms_sw_electric_helper.REFRESH_VIEW',view),view]

    
    
    
    def call_ExecuteOracleViewRefreshParallel(self, views , workers=6):
        from multiprocessing.dummy import Pool as ThreadPool
        #if workers == -1:
        #    workers = get_core_count()
        pool = ThreadPool(workers)
        views = views#[['OH_STEP_INSTALLATION_MV'],['UG_FUSE_EV'],['UG_FUSE_TB'],['BOB']]
        results = pool.map(self.ExecuteOracleViewRefreshParallel,views)
        pool.close()
        print('pool closed')
        pool.join()
        return results 
    
    def run_proc_function_for_result(self,func):
        success =False
        con = self.engine.raw_connection()
        try:
            result = con.cursor().callfunc(func,int)
            self.tepcore.Debug('object exists')
            return result
        except Exception as e:
            self.tepcore.Debug(str(e))
            success = False
        finally:    
            con.close ()
        return 0
    
    def ExecuteOracleRawQuery(self,sql,paramdata=None,commit=False):
        success =False
        con = self.engine.raw_connection()
        #auto commit
        #con.autocommit  = True
        try:
            if paramdata is not None:
                cursor = con.cursor().execute(sql,paramdata)
            else:
                cursor = con.cursor().execute(sql)
            self.tepcore.Debug('Execute Oracle Raw Success')
            #success = True
            if commit:
                con.commit()
            success = cursor.fetchall()
        except Exception as e:
            self.tepcore.Debug(str(e))
            success = False
        finally:    
            con.close ()
        return success
    def ExecuteOracleRaw(self,sql):
        success =False
        con = self.engine.raw_connection()
        #auto commit
        #con.autocommit  = True
        try:
            con.cursor().execute(text(sql))
            self.tepcore.Debug('Execute Oracle Raw Success')
            success = True
            con.commit()
        except Exception as e:
            self.tepcore.Debug(str(e))
            success = False
        finally:    
            con.close ()
        return success
    
    
    def ExecuteOracleRawMany(self,sql,data,overidetime=False):
        success =False
        if self.osiconn:
            con= self.GetConnection()
        else:
            con = self.engine.raw_connection()
        #auto commit
        #con.autocommit  = True
        try:
            if (overidetime):
                
                con.cursor().setinputsizes(None,None,None,None,None,None,None,None,None,None,None,None,None,None,cx_Oracle.TIMESTAMP)
            con.cursor().executemany(sql,data)
            self.tepcore.Debug('Execute Oracle Raw Success')
            success = True
            con.commit()
        except Exception as e:
            self.tepcore.Debug('Failed Oracle Raw error:')
            self.tepcore.Debug(str(e))
            success = False
        finally:
            if not self.osiconn:
                con.close ()
        return success
    
    
    def tableExists(self, objectname):
        exists =False  
        with self.engine.connect() as con:
            try:
                if con.execute('select * from {obj}'.format(obj= objectname) ):
                    self.tepcore.Debug('object exists')
                    exists = True
                else:
                    self.tepcore.Debug('not connected!')
                    exists = False
            except:
                #tepcore.Debug(str(e))
                exists = False
        return exists
    
    def runUpdate(self, sql):
        success =False  
        with self.GetConnection() as con:
            try:
                #osi conss still use raw text
                if self.osiconn:
                    con.execute(sql)
                else:
                
                    con.execute(text(sql))
                self.tepcore.Debug('update succeses')
                success = True
                if self.odbcconnected and not self.osiconn: 
                    con.execute('commit')
                    self.tepcore.Debug('odbc commit succeses')


            except Exception as e:
                self.tepcore.Debug(str(e))
                success = False
        return success
    
    #only suppported with sql alchemy object supported by sqlalchemy?
# =============================================================================
#     def runExecuteMany(self, sql,data):
#          success =False  
#          with self.engine.connect().execution_options(autocommit=True)  as con:
#              try:
#                  con.execute(sql,data)
#                  self.tepcore.Debug('object exists')
#                  success = True
#              except Exception as e:
#                  self.tepcore.Debug(str(e))
#                  success = False
#          return success    
# =============================================================================
    def dropTable(self, table):
        sql = 'DROP TABLE IF EXISTS {table}'.format(table=table)
        success =False  
        with self.engine.connect().execution_options(autocommit=True)  as con:
            try:
                con.execute(text(sql))
                self.tepcore.Debug('object exists')
                success = True
            except:
                #tepcore.Debug(str(e))
                success = False
        return success
            
    def run_query_for_df(self,query,chunksize=100000,osiretrycnt=0):
        '''
        
        Runs a query and returns a pandas dataframe.
        Parameters
        ----------
        query : TYPE
            DESCRIPTION.
        chunksize : TYPE, optional
            DESCRIPTION. The default is 100000.
        osiretrycnt : TYPE, optional
            For an ois odbc driver automaticcly retyr up to 5 times. This is ment to be used internally. The default is 0.

        Returns
        -------
        TYPE
            pd.Dataframe.

        '''
        alqcon= None
        if self.debug:
            start = time.time()
            self.tepcore.debug(start)
            self.tepcore.debug(query)
        try:

            if osiretrycnt > 0 and self.osiconn:
                self.tepcore.debug('Aquire new connection')

                alqcon = self.GetNewConnection()
                self.tepcore.debug('alqcon is {alqcon}'.format(alqcon=str(alqcon)))
            else:
                alqcon = self.GetConnection()
            df = pd.DataFrame()
            if self.osiconn:
                for chunks in pd.read_sql(query, con=alqcon, chunksize=100000):
                    df = df.append(chunks)
                
            else:
                for chunks in pd.read_sql(text(query), con=alqcon, chunksize=100000):
                    df = df.append(chunks)
        except Exception as e:
            print('failed to get DF!')
            print(str(e))
            if self.osiconn:
                if 'General ISAM error' in str(e) and osiretrycnt<=2:
                    osiretrycnt+=1
                    if self.tepcore.isValidDf(df):
                       
                        self.tepcore.debug('Spam error returning df')
                        print(df.head())
                        
                    else:
                        print('Retrying osi non sensical error try cnt with lower data cnt {osiretrycnt}'.format(osiretrycnt=str(osiretrycnt)))
                        return self.run_query_for_df(query,500,osiretrycnt)
                    

            else:
                #no other connection types support an automatic retry as generally the drivers dont have random connection issues like osis
                return False
        finally:  
            if not self.osiconn and alqcon is not None:
                alqcon.close ()
        if self.debug:
            end = time.time()
            self.tepcore.debug('Query execution time seconds {seconds}'.format(seconds = end - start))
        return df
    
    def run_query_for_df_w_conn(self,query,chunksize=100000):
        self.tepcore.debug('Aquire new connection')
        alcon = self.GetNewConnection()
        self.tepcore.debug('Aquired new connection')
        if self.debug:
            start = time.time()
            self.tepcore.debug(start)
        try:
            df = pd.DataFrame()
            for chunks in pd.read_sql(query, con=alcon, chunksize=100000):
                df = df.append(chunks)
        except Exception as e:
            print('failed to get DF!')
            print(str(e))
            return False
        finally:  
            alcon.close ()
        if self.debug:
            end = time.time()
            self.tepcore.debug('Query execution time seconds {seconds}'.format(seconds = end - start))
        return df
    
    def GetDfsForQuerys(self,querys):
        dfs = self.ParrallelQueryForDF(querys)
        return dfs
                
    def multip_proc_insert_df(self,dfin, table,dtype=None,if_exists='replace',index=False):
        alqcon = self.engine.connect()
        #engine.execute(sa_text('''TRUNCATE TABLE {table}'''.format(table=table)).execution_options(autocommit=True))
        
        dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table, con=alqcon, index=index,if_exists=if_exists,index_label=None,dtype=dtype)
        alqcon.close()
        return True
    
    def UpdateTableColFromDf(self,df,table,cols,keys,suffix=False, dtype=None):
        columnstmt=None
        keystmt=None
        if not suffix:
            temptable = table + '_{suffix}'.format(suffix='TEMP')
        else:
            temptable = table + '_{suffix}'.format(suffix=suffix)
        colcnt = 0    
        for key, value in cols.items():
            if colcnt==0:
                columnstmt = 'f.{value} = t.{value}'.format(value= value)
            else:
                columnstmt = columnstmt + ',f.{value} = t.{value}'.format(value= value)
            colcnt=colcnt+1
        
        keycnt = 0    
        for key, value in keys.items():
            if keycnt==0:
                keystmt = 'f.{value} = t.{value}'.format(value= value)
            else:
                keystmt = keystmt + ' AND f.{value} = t.{value}'.format(value= value)
            keycnt=keycnt+1
        sql = '''MERGE INTO {table} f USING (select * from {temp_table}) t ON( {keys}) WHEN MATCHED THEN UPDATE SET {cols}'''.format(table=table,temp_table =temptable, cols=columnstmt,keys=keystmt )
        self.tepcore.Debug(sql)    
        if not self.tableExists(temptable):
                self.InsertNewTable(df,temptable,dtype)   
        else:
            self.ReplaceTable(df,temptable,dtype)
        status=self.runUpdate(sql)
        return status
      
    def MergeTableColFromDf(self,df,table,cols,keys,suffix=False, dtype=None,fastmode=False):
        columnstmt=None
        keystmt=None
        mergecolstmt=None
        mergeinsertcolstmt=None
        mergecolstmtcnt=0
        mergeinsertcolstmtcnt=0
        if not suffix:
            temptable = table + '_{suffix}'.format(suffix='TEMP')
        else:
            temptable = table + '_{suffix}'.format(suffix=suffix)
        colcnt = 0    
        for key, value in cols.items():
            curvalue=value
            if colcnt==0:
                columnstmt = 'f.{value} = t.{value}'.format(value= curvalue)
            else:
                columnstmt = columnstmt + ',f.{value} = t.{value}'.format(value= curvalue)
            colcnt=colcnt+1
            if mergecolstmtcnt ==0:
                mergecolstmt = '{value}'.format(value= curvalue)
 
                mergecolstmtcnt+=1
            else:
                mergecolstmt =  mergecolstmt +  ',{value}'.format(value= curvalue)
                
                mergecolstmtcnt+=1
            
            if mergeinsertcolstmtcnt==0:
                mergeinsertcolstmt = 't.{value}'.format(value= curvalue)
                mergeinsertcolstmtcnt+=1
            else:
                mergeinsertcolstmt =  mergeinsertcolstmt +  ',t.{value}'.format(value= curvalue)
                mergeinsertcolstmtcnt+=1
        
        keycnt = 0    
        for key, value in keys.items():
            curvalue=value
            if keycnt==0:
                keystmt = 'f.{value} = t.{value}'.format(value= curvalue)
            else:
                keystmt = keystmt + ' AND f.{value} = t.{value}'.format(value= curvalue)
            keycnt=keycnt+1
            
            if mergecolstmtcnt ==0:
                mergecolstmt = '{value}'.format(value= curvalue)
                mergecolstmtcnt+=1
             
            else:
                mergecolstmt =  mergecolstmt +  ',{value}'.format(value= curvalue)
                mergecolstmtcnt+=1
                
            
            if mergeinsertcolstmtcnt==0:
                mergeinsertcolstmt = 't.{value}'.format(value= curvalue)
                mergeinsertcolstmtcnt+=1
            else:
                mergeinsertcolstmt =  mergeinsertcolstmt +  ',t.{value}'.format(value= curvalue)
                mergeinsertcolstmtcnt+=1
        self.tepcore.Debug('mergeinsertcolstmt {mi}'.format(mi=mergeinsertcolstmt))  
        self.tepcore.Debug('mergecolstmt {mcs}'.format(mcs=mergecolstmt))  
        
        sql = '''MERGE INTO {table} f USING (select * from {temp_table}) t ON( {keys}) 
        WHEN MATCHED THEN UPDATE SET {cols} 
        WHEN NOT MATCHED THEN INSERT ({mergecolstmt}) 
        values ({mergeinsertcolstmt})'''.format(table=table,temp_table =temptable, cols=columnstmt,keys=keystmt,mergecolstmt=mergecolstmt, mergeinsertcolstmt=mergeinsertcolstmt)
        self.tepcore.Debug(sql)    
        if not self.tableExists(temptable):
            if fastmode:
                # def FastDFInsert(self,dfin,table,createtable=False,tabledtypes=None,replacedata=False):
                self.FastDFInsert(df,temptable,True,dtype,True)
            else:
                self.InsertNewTable(df,temptable,dtype)   
        else:
            if fastmode:
                # def FastDFInsert(self,dfin,table,createtable=False,tabledtypes=None,replacedata=False):
                self.FastDFInsert(df,temptable,False,dtype,True)
            else:
                self.ReplaceTable(df,temptable,dtype)
        status=self.runUpdate(sql)
        return status
    
    def clean_text(self,text):
        if type(text)is str:
            
            unwanted_char = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff'
            text = "".join([(" " if n in unwanted_char else n) for n in text if n not in unwanted_char])
            return text
        else:
            return text
    
    def SqlToDateStr(self,strin):
        sqltodate='''TO_DATE('{strin}','YYYY-MM-DD HH24:MI:SS')'''.format(strin=strin)
        return sqltodate
    
    def ParrallelQueryForDF(self,queries,numthreads=5):
        
        pool = ThreadPool(numthreads)
        self.tepcore.debug('begiingng mp')
        if self.osiconn:
            results = pool.map(self.run_query_for_df_w_conn, queries)
        else:
            results = pool.map(self.run_query_for_df, queries)
        pool.close()
        pool.join()
        return results
    
        
    def convertSequenceToDict(self,list):
        """for  cx_Oracle:
            For each element in the sequence, creates a dictionary item equal
            to the element and keyed by the position of the item in the list.
            >>> convertListToDict(("Matt", 1))
            {'1': 'Matt', '2': 1}
        """
        dict = {}
        argList = range(1,len(list)+1)
        for k,v in zip(argList, list):
            #print(k,v)
            dict[str(k)] = v
        return dict
    
    def convertSequenceToDictOdbc(self,list):
        """for  Pyodbc:
            For each element in the sequence, creates a dictionary item equal
            to the element and keyed by the position of the item in the list.
            >>> convertListToDict(("Matt", 1))
            {'1': 'Matt', '2': 1}
        """
        dict = {}
        argList = range(1,len(list)+1)
        for k,v in zip(argList, list):
            #print(k,v)
            dict[k] = v
        return dict
    
    
    def db_colname(self,pandas_colname):
        '''convert pandas column name to a DBMS column name
            TODO: deal with name length restrictions, esp for Oracle
        '''
        colname =  pandas_colname.replace(' ','_').strip()                  
        return colname
    
    def CreatEmptyTableForDf(self,dfin,table,dtypes):
        
        self.ReplaceTable(dfin.head(1),table,dtypes)
        delsql = '''DELETE FROM {table}'''.format(table=table)
        return self.runUpdate(delsql)
    
    
    
    def convertSequenceToDictConvertTime(self,list,convert17=True):
        """for  cx_Oracle:
            For each element in the sequence, creates a dictionary item equal
            to the element and keyed by the position of the item in the list.
            >>> convertListToDict(("Matt", 1))
            {'1': 'Matt', '2': 1}
        """
        dict = {}
        argList = range(1,len(list)+1)
        for k,v in zip(argList, list):
            #print(k)
            #print(v)
            if convert17 and (k==17 or k==18):
                #print('converting value')
                #print(v)
                if v == '' and k==17:
                    v = 4133894400000000000
                if v == '' and k==18:
                    v = -2208988800000000000
                if not type(v) is pd.Timestamp:
                    
                    v = pd.to_datetime(int(v)/1000000,unit='ms')
                #print(v)
            dict[str(k)] = v
        return dict
    
    def FastDFInsertDep(self,dfin,table,createtable=False,tabledtypes=None,replacedata=False):
        if createtable:
            self.CreatEmptyTableForDf(dfin,table,tabledtypes)
        
        if replacedata:
             delsql = '''DELETE FROM {table}'''.format(table=table)
             self.runUpdate(delsql)
            
        
        self.tepcore.debug('starting insert')
        dfin.reset_index(drop=True,inplace=True)
        dfin.replace(np.nan, '', regex=True,inplace=True)
        cols=[self.db_colname(k) for k in dfin.dtypes.index]
        colnames = ','.join(cols)
        if self.osiconn:
            colpos = ', '.join([str('?') for i,f in enumerate(cols)])
        else:
            colpos = ', '.join([':'+str(i+1) for i,f in enumerate(cols)])
        name = table
        insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (name, colnames, colpos)
        print ('insert_sql', insert_sql)
        if self.osiconn:
            newdata=[]
            
            data = [ self.convertSequenceToDictConvertTime(rec) for rec in dfin.values]  
            #print(data)
            #print(type([data]))
            for i in data:
                #print(i)
                val = []
                for key,value in i.items():
                    val.append(value)
                newdata.append(val)
            data = newdata
            #print(data)
        else:
            data = [ self.convertSequenceToDictConvertTime(rec) for rec in dfin.values] 
        #print(data)
        result = self.ExecuteOracleRawMany(insert_sql, data,False)
        
        self.tepcore.debug(' multip_proc_insert_df Finished Insert')

        return result
    
    def FastDFInsert(self,dfin,table,createtable=False,tabledtypes=None,replacedata=False):
        if createtable:
            self.CreatEmptyTableForDf(dfin,table,tabledtypes)
        
        if replacedata:
             delsql = '''DELETE FROM {table}'''.format(table=table)
             self.runUpdate(delsql)
            
        
        self.tepcore.debug('starting insert')
        dfin.reset_index(drop=True,inplace=True)
        dfin.replace(np.nan, '', regex=True,inplace=True)
        cols=[self.db_colname(k) for k in dfin.dtypes.index]
        colnames = ','.join(cols)
        #odbs connections use ? oracle uese :1 :2 etc.
        if self.osiconn or self.odbcconnected or self.postgressconnected:
            colpos = ', '.join([str('?') for i,f in enumerate(cols)])
        else:
            colpos = ', '.join([':'+str(i+1) for i,f in enumerate(cols)])
        name = table
        insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (name, colnames, colpos)
        print ('insert_sql', insert_sql)
        if self.osiconn or self.odbcconnected:
            newdata=[]
            
            data = [ self.convertSequenceToDictOdbc(rec) for rec in dfin.values]  
            #print(data)
            #print(type([data]))
            for i in data:
                #print(i)
                val = []
                for key,value in i.items():
                    val.append(value)
                newdata.append(val)
            data = newdata
            #print(data)
        else:
            data = [ self.convertSequenceToDict(rec) for rec in dfin.values] 
        #print(data)
        result = self.ExecuteOracleRawMany(insert_sql, data)
        
        self.tepcore.debug(' multip_proc_insert_df Finished Insert')

        return result
    
    def FastDFUpdate(self,dfin,table,updatekey,allints=False,addfilter=None):

        
        self.tepcore.debug('starting update {allints}'.format(allints=allints))
        dfin.reset_index(drop=True,inplace=True)
        dfin.replace(np.nan, '', regex=True,inplace=True)
        cols=[self.db_colname(k) for k in dfin.dtypes.index]
        colnames = None
        for col in cols:
            if updatekey not in col:
                if colnames is None:
                    colnames = col + '= ?'
                else:
                    colnames = colnames + ',' + col + '= ?'
            
        if self.osiconn:
            colpos = ', '.join([str('?') for i,f in enumerate(cols)])
        else:
            colpos = ', '.join([':'+str(i+1) for i,f in enumerate(cols)])
 
        if addfilter is None:
            insert_sql = 'UPDATE %s SET %s WHERE %s=?' % (table, colnames, updatekey)
        else:
            insert_sql = 'UPDATE %s SET %s WHERE %s=? %s' % (table, colnames, updatekey, addfilter)
        print ('update_sql', insert_sql)
        if self.osiconn:
            newdata=[]
            
            data = [ self.convertSequenceToDictOdbc(rec) for rec in dfin.values]  
            print(data)
            #print(type([data]))
            for i in data:
                #print(i)
                val = []
                for key,value in i.items():
                    if allints:
                        val.append(int(value))
                    else:
                        val.append(value)
                newdata.append(val)
            data = newdata
        else:
            data = [ self.convertSequenceToDict(rec) for rec in dfin.values] 
        
        result = self.ExecuteOracleRawMany(insert_sql, data)
        
        self.tepcore.debug(' FastDFUpdate Finished update')

        return result
   
    def SqlTableMerge(self,tablename,stagequery, indexcols,outtable, mergecols=None):
    	#curquery="""select * from {tablename}""".format(tablename = tablename)
    	newquery="""select * from ({stagequery} minus {tablename}) t""".format(tablename = tablename, stagequery=stagequery)
    	print(tablename)
    	#curtbl = run_query_for_df(tablename)
    	print(newquery)
    	newdf = self.run_query_for_df(newquery)
    	curcols = newdf.dtypes.index
    	# curtbl.set_index(indexcols)
    	# newtbl.set_index(indexcols)
    	# mergedf = pd.merge(curtbl, newtbl, how='outer', left_index=True, right_index=True, indicator=True)
    	# newdf = mergedf[(mergedf['_merge']=='left_only')]
    	# deldf = mergedf[(mergedf['_merge']=='right_only')]
    	# updtdf = mergedf[(mergedf['_merge']=='both')]
    	if not newdf.empty:
    		print('adding new rows')
    		newdf.columns = curcols
#    		print.head(newdf)
    		self.FastDFInsert(newdf,outtable)
            
    def GetDataAsJsonDataTable(self,res):
        '''
        This fucntion will return a pandas data frame oriented as a json table in the data json element only. Other elements do exists in data frame if needed 
        create a new function and return the full json..

        Parameters
        ----------
        res : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            Json[data].

        '''
        jstring = res.to_json(orient='table')
        pandasjsondata = json.loads(jstring)
        return pandasjsondata['data']
   
    def RunQueryForJsonData(self,query):
        return self.GetDataAsJsonDataTable(self.run_query_for_df(query))
    
    

    def ExecuteSqlRawMany(self,sql,data):
        success =False
        if self.osiconn:
            con= self.GetConnection()
        else:
            con = self.engine.raw_connection()
        #auto commit
            con.autocommit  = False
        try:
            con.cursor().fast_executemany = True #try to speed her up
            con.cursor().executemany(sql,data)
            self.tepcore.Debug('Execute ExecuteSqlRawMany Raw Success')
            success = True
            con.commit()
        except Exception as e:
            self.tepcore.Debug('Failed ExecuteSqlRawMany Raw error:')
            self.tepcore.Debug(str(e))
            success = False
        #finally:
            #if not self.osiconn:
                #con.close ()
        return [success, con]


    def chunks(self,l, n= 5000):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n): #use xrange in python2, range in python3
            yield l[i:i + n]
        
    def SqlServerFastDfInsert(self,dfin,table,createtable=False,tabledtypes=None,replacedata=False):
        if createtable:
            self.CreatEmptyTableForDf(dfin,table,tabledtypes)
        
        if replacedata:
             delsql = '''DELETE FROM {table}'''.format(table=table)
             self.runUpdate(delsql)
            
        
        self.tepcore.debug('starting insert')
        dfin.reset_index(drop=True,inplace=True)
        dfin.replace(np.nan, '', regex=True,inplace=True)
        cols=[self.db_colname(k) for k in dfin.dtypes.index]
        colnames = ','.join(cols)
        #odbs connections use ? oracle uese :1 :2 etc.
        if self.osiconn or self.odbcconnected or self.postgressconnected:
            colpos = ', '.join([str('?') for i,f in enumerate(cols)])
        else:
            colpos = ', '.join([':'+str(i+1) for i,f in enumerate(cols)])
        name = table
        insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (name, colnames, colpos)
        print ('insert_sql', insert_sql)
        if self.osiconn or self.odbcconnected:
            newdata=[]
            
            data = [ self.convertSequenceToDictOdbc(rec) for rec in dfin.values]  
            #print(data)
            #print(type([data]))
            for i in data:
                #print(i)
                val = []
                for key,value in i.items():
                    val.append(value)
                newdata.append(val)
            data = newdata
            #print(data)
        else:
            data = [ self.convertSequenceToDict(rec) for rec in dfin.values] 
        #print(data)
        for chunk in self.chunks(data, 10000): #IMPORTANT
            result = self.ExecuteSqlRawMany(insert_sql, chunk)
        result[1].close()
        self.tepcore.debug(' multip_proc_insert_df Finished Insert')

        return result
    def SqlFastReplace(self,dfin, table,dtypes=None,replancenan=True):
        now = datetime.now()
        msg='start time insert :{now}'.format(now=now)
        self.tepcore.Debug(msg)
        start = timer()
        alqcon = self.engine.connect()
        #print(dfin.head())
        #if replancenan:
        #    dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table.lower(), con=alqcon, index=False,if_exists='replace',index_label=None,dtype=dtypes)
        alqcon.close()
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start))   
    def SqlFastAppend(self,dfin, table,dtypes=None,replancenan=True):
        now = datetime.now()
        msg='start time insert :{now}'.format(now=now)
        self.tepcore.Debug(msg)
        start = timer()
        alqcon = self.engine.connect()
        #print(dfin.head())
        #if replancenan:
        #    dfin.replace(np.nan, '', regex=True,inplace=True)
        dfin.to_sql(name=table.lower(), con=alqcon, index=False,if_exists='append',index_label=None,dtype=dtypes)
        alqcon.close()
        end = timer()#time.time()
        self.tepcore.Debug('finished {table}'.format(table=table))
        self.tepcore.Debug('time: {time}'.format(time=end-start)) 
        
        
class SQLSERVER11TepCoreData(tepcoredata):
    def __init__(self,user,password,dbname,debug=False,debugsql=False,DSN='SQL',SQLSCHEMA=None):
        self.user=user
        self.password=password
        self.dbname=dbname
        self.tepcore = tepcore(debug)
        self.debugsql = debugsql
        self.cnxn=None
        self.osiconn=False
        self.debug = debug
        self.odbcconnected=False
        self.postgressconnected = False
        #connection pool
        try:
            #self.db_pool = cx_Oracle.SessionPool(user=self.user, password=self.password, dsn=self.dbname, min=1, max=20, increment=1, threaded=True)
            #dbconn = db_pool.acquire()#cx_Oracle.Connection(dsn=dbname, pool=db_pool)
            #connection pool for sql qlchemy used for inserts
            self.dsn = DSN
            if 'SQL' in self.dsn:

                print('attempting to connect into DSN: ' + DSN + ' dbname ' + dbname + ' user ' + user)
                if SQLSCHEMA is None:
                    params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};SERVER='+dbname+';UID='+user+';PWD='+ password+';TrustServerCertificate=yes')
                else:
                    params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};SERVER='+dbname+';UID='+user+';PWD='+ password+';TrustServerCertificate=yes;' + 'Database={SQLSCHEMA};'.format(SQLSCHEMA=SQLSCHEMA))
                #print(params)
                self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

                self.odbcconnected=True 

        except (Exception ) as e:
            tepcore.Debug('Failed to connect to SQL SERVER')
            tepcore.Debug(str(e))
            
    def GetNewConnection(self):
        '''
        This overloads standard GetNewConnection this is ris remoivng autom commit flags as doesnt seem to be a param of native client 11.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        return self.engine.connect()
    def run_query_for_df(self,query,chunksize=100000):
        
        
        
        #query = 'SET NOCOUNT ON; ' + query
        #df = pd.read_sql_query(query , conn)
        #need to obfuscate these classes print(query)
        alcon = self.GetNewConnection()
        self.tepcore.debug('Aquired new connection')
        if self.debug:
            start = time.time()
            self.tepcore.debug(start)
        try:
            df = pd.DataFrame()
            for chunks in pd.read_sql(query, con=alcon, chunksize=100000):
                df = df.append(chunks)
        except Exception as e:
            print('failed to get DF!')
            print(str(e))
            return False
        finally:  
            alcon.close ()
        if self.debug:
            end = time.time()
            self.tepcore.debug('Query execution time seconds {seconds}'.format(seconds = end - start))
        return df