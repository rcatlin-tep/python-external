# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 13:37:11 2020

@author: aa58436
"""
import pandas as pd
from tepcore import tepcore
from timeit import default_timer as timer
from datetime import datetime,timedelta, date
import sys 
#from pythonnet import load
#load('coreclr')
import clr
import argparse
import os


if os.path.exists(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0'):
    print('Using af at C:/Program Files (x86)/PIPC/AF/PublicAssemblies/4.0 ''')
    sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')      
elif r'D:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0':
    print('Using af at D:/Program Files (x86)/PIPC/AF/PublicAssemblies/4.0 ''')
    sys.path.append(r'D:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')    
clr.AddReference('OSIsoft.AFSDK')  
from OSIsoft.AF import *  
from OSIsoft.AF.PI import *  
from OSIsoft.AF.Asset import *  
from OSIsoft.AF.Data import *  
from OSIsoft.AF.Time import *  
from OSIsoft.AF.UnitsOfMeasure import *
from OSIsoft.AF.Search import *
from dateutil.relativedelta import relativedelta
import math
import numbers
#clr lists for search criteri
clr.AddReference("System.Collections")
from System.Collections.Generic import List
from System import String
from System import Boolean

from System.Collections.Generic import Dictionary
from System import String
from System import Object

class tepcorepidata:
    T_BREAKR ='T_BREAKR'
    standardattributesToLoad = List[String]()
    standardattributesToLoad.Add(PICommonPointAttributes.Tag)
    standardattributesToLoad.Add(PICommonPointAttributes.Descriptor)
    standardattributesToLoad.Add(PICommonPointAttributes.PointType)
    standardattributesToLoad.Add(PICommonPointAttributes.DigitalSetName)
    standardattributesToLoad.Add(PICommonPointAttributes.TypicalValue)
    standardattributesToLoad.Add(PICommonPointAttributes.InstrumentTag)
    standardattributesToLoad.Add(PICommonPointAttributes.PointSource)
    
    
    
    def __init__(self,piserver='EMSWDPI01',debug=False,useAf=False,afdb='OATI_TEST',connectserverandaf=False,pisname=None):
        """
        This class contains funcitons for workign with pi data. It has wrappers using pythnnet to uses pi af sdk c# sdk.
        This requuires PI sdk software isntalled. It is looking in C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0 for the dlls.

        Parameters
        ----------
        piserver : TYPE, optional
            DESCRIPTION. The default is 'EMSWDPI01'. A pi server or afdb. If using AFDB place the piserver in the afdb arguemnt.
        debug : TYPE, optional
            DESCRIPTION. The default is False.
        useAf : TYPE, optional
            DESCRIPTION. The default is False.
        afdb : TYPE, optional
            DESCRIPTION. The default is 'OATI_TEST'. THis is an AF DF to connect to. The AFDB will have a list of pi servers to use. 
            Example ESPWPPIAF01 hs connections for TEPPIUPDS. If the useAf is set to ture place in pi server the af database and in afdb the piserver to search for in said afdb.

        Returns
        -------
        None.

        """
        self.useaf = useAf
        self.tepcore = tepcore(debug)
        self.debug=debug
        self.afdbs=[]
        self.afattributmapcache=None
        if useAf:
            self.tepcore.debug(' attempting to use pi af afdb {afdb}'.format(afdb=afdb))
           
            self.piServers = PISystems()
            #self.piServers = PIServers()
            #self.piServer = PIServer.FindPIServer(piserver)  
            print('piservers {piservers}'.format(piservers=self.piServers ))
            

            
            #afserver =  self.piServers.DefaultPISystem
            afserver =  self.piServers[piserver]
            

            if self.debug:
                 for db in afserver.Databases:
                     self.tepcore.debug('AF has multiple dbs {0}'.format(db))  
                     self.afdbs.append(db)


            self.piServer = afserver.Databases.get_Item(afdb)#afServers.DefaultPISystem
            self.tepcore.debug('afserver db connection is {afdconn}'.format(afdconn=str(self.piServer)))
            if self.piServer is not None:
                print('server name is :' + self.piServer.Name)
                print('server Description  is :' + self.piServer.Description)
                print('server connection info  is :' + str(self.piServer.PISystem.ConnectionInfo.IsConnected ))
                print('server connection info timeout is :' + self.piServer.PISystem.ConnectionInfo.IdleTimeout.ToString() )
            else:
                print('No pi server')
        else:
            self.piServers = PIServers()
            self.piServer = PIServer.FindPIServer(piserver) 
        if connectserverandaf:
            self.pi = PIServer.FindPIServer(pisname)
        else:
            self.pi = None
        self.tepcore.debug('Attemtping to connect to  {0}'.format(piserver))  
        
        #DB = afServer.Databases.DefaultDatabase    
        #throws CommunicationException: Cannot connect to server 'SCZWDDMSPDS01'.
        #self.DB = self.afServer.Databases.get_Item(piserver)
        
    
    def IsServerConnected(self):
        """
        Checks AFSDK connection info to see if server is connected.

        Returns
        -------
        connected : TYPE
            DESCRIPTION.

        """
        connected = False
        if self.useaf:
            connected = self.piServer.PISystem.ConnectionInfo.IsConnected 
            self.tepcore.debug('Pi server is connected {conn}'.format(conn=str(connected)))
            return connected
        else:
            connected =  self.piServer.ConnectionInfo.IsConnected
            self.tepcore.debug('Pi server is connected {conn}'.format(conn=str(connected)))
            return connected
        return connected
        
    def GetPiServer(self):
        """
        Returns the pi server object

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        return self.piServer
    
    def GetTimeStamp(self,pitimestring):
        """
        Converts a pi time string to python datatime object

        Parameters
        ----------
        pitimestring : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #6/23/2020 2:38:21 PM
        return datetime.strptime(pitimestring, '%m/%d/%Y %I:%M:%S %p')
    
    def GetTimeString(self,timestamp):
        """
        Converts python timestamp into format %m/%d/%Y %H:%M:%S %p' ex 6/23/2020 2:38:21 PM

        Parameters
        ----------
        timestamp : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #6/23/2020 2:38:21 PM
        return timestamp.strftime( '%m/%d/%Y %H:%M:%S %p')
    
    def GetTimeStringOati(self,timestamp):
        """
        Converts timestamp into timstamp sepcific for OATI interfaces

        Parameters
        ----------
        timestamp : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #6/23/2020 2:38:21 PM
       # print(timestamp)
        tmp = self.GetTimeStamp(str(timestamp))
        return tmp.strftime( '%m/%d/%Y %H:%M')
    
    def SortDFByTime(self,df):
        """
        Sorts a dataframe object by time ascending

        Parameters
        ----------
        df : TYPE
            DESCRIPTION.

        Returns
        -------
        retdf : TYPE
            Sorted ascending dataframe.

        """
        retdf = df.sort_index( inplace=False, ascending=True)
        return retdf
    
    def GetPiPoint(self,tag):
        """
        Trys to fetch a pi point object by its tag name.

        Parameters
        ----------
        tag : TYPE
            DESCRIPTION.

        Returns
        -------
        pt : TYPE
            None or a pi point object.

        """
        try: 
            pt = PIPoint.FindPIPoint(self.piServer, tag)
            name = pt.Name.lower()  
            # CurrentValue  
            self.tepcore.debug('\nShowing PI Tag CurrentValue from {0}'.format(name))
        except PIException:
            return None
        return pt
    
    
    
    
    def SupportedPointType(self, pointype):
        """
        # =============================================================================
#         	Null	0	The PIPoint's type is not defined.
# Int16	6	The PIPoint is numeric and is restricted to integer values. Its normal values are archived as 16-bit integers.
# Int32	8	The PIPoint is numeric and is restricted to integer values. Its normal values are archived as 32-bit integers.
# Float16	11	The PIPoint is numeric; its normal values are archived in 16-bit scaled fixed-point format.
# Float32	12	The PIPoint is numeric; its normal values are archived in single-precision floating-point format.
# Float64	13	The PIPoint is numeric; its normal values are archived in double-precision floating-point format.
# Digital	101	The PIPoint's normal values are members of a digital state set.
# Timestamp	104	The PIPoint's normal values are a time stamp.
# String	105	The PIPoint's normal values are character strings.
# Blob	102	The PIPoint's normal values are BLOBs (binary large objects).
# Support all numeric types
# =============================================================================

        Parameters
        ----------
        pointype : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """

        if pointype >0 and pointype <=101:
            return True
        else:
            return False
    
    def GetDataForTag(self,tag,start='*-3h',end="*"):
        """
        

        Parameters
        ----------
        tag : TYPE
            A python tag. First we use FindPiPoint to get point object then retrieve data from start to end.
        start : TYPE, optional
            DESCRIPTION. The start of data wrange .The default is '*-3h'.
        end : TYPE, optional
            DESCRIPTION. The end of data range. The default is "*".

        Returns
        -------
        df : TYPE
            DESCRIPTION.

        """
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        times=[]
        vals=[]
        #datapi=[]
        timerange = AFTimeRange(start, end)  
        pt = PIPoint.FindPIPoint(self.piServer, tag)
        name = pt.Name.lower()  
        # CurrentValue  
        self.tepcore.debug('\nShowing PI Tag CurrentValue from {0}'.format(name)) 
        recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  

    
        for event in recorded:
            #self.tepcore.debug('{0} value: {1}'.format(event.Timestamp.LocalTime, event.Value))  
            #datapi[str(event.Timestamp.LocalTime)]=int(event.Value)
            times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
            vals.append(event.Value)
        df = pd.DataFrame(data=vals,index=times,columns=['value'])  
        self.tepcore.debug(df.head())
        return df
    
    def GetDataForPoint(self,pt,start='*-3h',end="*"):
        """
        If you already have pi point object use GetDataForPoint otherwise pass in string of the tag and use GetDataForTag.

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE, optional
            DESCRIPTION. The default is '*-3h'.
        end : TYPE, optional
            DESCRIPTION. The default is "*".

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        times=[]
        vals=[]
        #datapi=[]
        timerange = AFTimeRange(start, end)  
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  
        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

    
        for event in recorded:
            #self.tepcore.debug('{0} value: {1}'.format(event.Timestamp.LocalTime, event.Value))  
            #datapi[str(event.Timestamp.LocalTime)]=int(event.Value)
           
            #print('values') 
            #print(vals)
            if not isinstance(event.Value,float):
                #print (type(event.Value))
                if isinstance(event.Value,AFEnumerationValue):
                    #print('Multiple values')
                    #print (event.Value.Value)
                    #print (event.Value.Name)
                    times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                    vals.append(event.Value.Value)


            #for v in event.Values:
            else:
                times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                vals.append(event.Value)


        
        df = pd.DataFrame(data=vals,index=times,columns=['value'])   
        #self.tepcore.debug(df.head())
        return df
        
    def GetDataForPoint_int(self,pt,start,end):
        """
        Gets pi data given a point, but only returns integeger values.

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        times=[]
        vals=[]
        #datapi=[]
        timerange = AFTimeRange(start, end)  
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  
        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

    
        for event in recorded:
            #self.tepcore.debug('{0} value: {1}'.format(event.Timestamp.LocalTime, event.Value))  
            #datapi[str(event.Timestamp.LocalTime)]=int(event.Value)
           
            #print(event.Value)
            if not isinstance(event.Value,int):
                #print (type(event.Value))
                if isinstance(event.Value,AFEnumerationValue):
                    #print('Multiple values')
                    #print (event.Value.Value)
                    #print (event.Value.Name)
                    times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                    vals.append(event.Value.Value)


            #for v in event.Values:
            else:
                times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                vals.append(event.Value)


        
        df = pd.DataFrame(data=vals,index=times,columns=['value'])   
        #self.tepcore.debug(df.head())
        return df   

    def GetDataForPoint_interpolated(self,pt,start,end,interval):
        """
        Gets pi point InterpolatedValues as floats.

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        times=[]
        vals=[]
        #datapi=[]
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  
            recorded = pt.InterpolatedValues(timerange, span, "", False)  
        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

    

        for event in recorded:
            #self.tepcore.debug('{0} value: {1}'.format(event.Timestamp.LocalTime, event.Value))  
            #datapi[str(event.Timestamp.LocalTime)]=int(event.Value)
           
            #print(event.Value)
            if not isinstance(event.Value,float):
                #print (type(event.Value))
                if isinstance(event.Value,AFEnumerationValue):
                    #print('Multiple values')
                    #print (event.Value.Value)
                    #print (event.Value.Name)
                    times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                    vals.append(event.Value.Value)


            #for v in event.Values:
            else:
                times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                vals.append(event.Value)


        
        df = pd.DataFrame(data=vals,index=times,columns=['value'])   
        #self.tepcore.debug(df.head())
        return df

    def GetDataForPoint_interpolated_int(self,pt,start,end,interval):
        """
        

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        times=[]
        vals=[]
        #datapi=[]
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  
            recorded = pt.InterpolatedValues(timerange, span, "", False)  
        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

    
        for event in recorded:
            #self.tepcore.debug('{0} value: {1}'.format(event.Timestamp.LocalTime, event.Value))  
            #datapi[str(event.Timestamp.LocalTime)]=int(event.Value)
           
            #print(event.Value)
            if not isinstance(event.Value,int):
                #print (type(event.Value))
                if isinstance(event.Value,AFEnumerationValue):
                    #print('Multiple values')
                    #print (event.Value.Value)
                    #print (event.Value.Name)
                    times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                    vals.append(event.Value.Value)


            #for v in event.Values:
            else:
                times.append(self.GetTimeStamp(str(event.Timestamp.LocalTime)))
                vals.append(event.Value)


        
        df = pd.DataFrame(data=vals,index=times,columns=['value'])   
        #self.tepcore.debug(df.head())
        return df    

    def PIAdvCalcDat_timeweighted(self,pt,start,end,interval):
        """
        

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.Summaries (timerange, span, 2, 0, 1)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)

        return finaldf
    
    
    def PIAdvCalcDat_timeweighted_enddate(self,pt,start,end,interval,timedltainterval=timedelta(hours=1)):
        """
        
        Provides the end time of the data as opposed to the start by adding timedelta to start value
        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.Summaries (timerange, span, 2, 0, 1)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp))+timedltainterval)
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)

        return finaldf
    

    def GetPointListByAttribute(self,attribute,searchstring,daysback=0,exactmatch=0,usestandardattributes = False): 
        """
        

        Parameters
        ----------
        attribute : TYPE
            DESCRIPTION.
        searchstring : TYPE
            DESCRIPTION.
        daysback : TYPE, optional
            DESCRIPTION. The default is 0.
        exactmatch : TYPE, optional
            DESCRIPTION. The default is 0.

        Returns
        -------
        results : TYPE
            DESCRIPTION.

        """
        #attribute = 'tag','instrumenttag', 'creationdate', 'descriptor'
        #exact match = 1, 0

        if exactmatch == 0:
            search = "*{searchstring}*".format(searchstring=searchstring)
        else:
            search = "{searchstring}".format(searchstring=searchstring)
        
        if attribute == 'tag':
            attributeFilter = PIPointQuery(PICommonPointAttributes.Tag, AFSearchOperator.Equal, search);
        elif attribute == 'instrumenttag':
            attributeFilter = PIPointQuery(PICommonPointAttributes.InstrumentTag, AFSearchOperator.Equal, search);
        elif attribute == 'creationdate':
            search = date.today() - timedelta(days = daysback)
            search = "{search}".format(search=search)
            attributeFilter = PIPointQuery(PICommonPointAttributes.CreationDate, AFSearchOperator.GreaterThanOrEqual, search);
        elif attribute == 'descriptor':
            attributeFilter = PIPointQuery(PICommonPointAttributes.Descriptor, AFSearchOperator.Equal, search);
        elif attribute =='pointsource':
            attributeFilter = PIPointQuery(PICommonPointAttributes.PointSource, AFSearchOperator.Equal, search);
        if usestandardattributes:
            resattributes = self.standardattributesToLoad
        else:
            resattributes = List[String]()
            resattributes.Add(PICommonPointAttributes.InstrumentTag)  
        li = List[PIPointQuery]()
        li.Add(attributeFilter)
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoints.htm
        results = PIPoint.FindPIPoints(self.piServer,li,resattributes)
        return results
    
    
    def GetPointListByAttributeAsDF(self,attribute,searchstring,daysback=0,exactmatch=0,usestandardattributes = False): 
        """
        

        Parameters
        ----------
        attribute : TYPE
            DESCRIPTION.
        searchstring : TYPE
            DESCRIPTION.
        daysback : TYPE, optional
            DESCRIPTION. The default is 0.
        exactmatch : TYPE, optional
            DESCRIPTION. The default is 0.

        Returns
        -------
        results : TYPE
            DESCRIPTION.

        """
        #attribute = 'tag','instrumenttag', 'creationdate', 'descriptor'
        #exact match = 1, 0
        #standardattributesToLoad.Add(PICommonPointAttributes.Tag)
        #standardattributesToLoad.Add(PICommonPointAttributes.Descriptor)
        #standardattributesToLoad.Add(PICommonPointAttributes.PointType)
        # standardattributesToLoad.Add(PICommonPointAttributes.DigitalSetName)
        #standardattributesToLoad.Add(PICommonPointAttributes.TypicalValue)
        #standardattributesToLoad.Add(PICommonPointAttributes.InstrumentTag)
        #standardattributesToLoad.Add(PICommonPointAttributes.PointSource)
    
        
        if usestandardattributes:
            columns = [PICommonPointAttributes.Tag,PICommonPointAttributes.Descriptor,PICommonPointAttributes.PointType,PICommonPointAttributes.DigitalSetName,PICommonPointAttributes.TypicalValue,PICommonPointAttributes.InstrumentTag,PICommonPointAttributes.PointSource]
        else:
            columns = [PICommonPointAttributes.Tag,PICommonPointAttributes.InstrumentTa]
        
        print(columns)
        if exactmatch == 0:
            search = "*{searchstring}*".format(searchstring=searchstring)
        else:
            search = "{searchstring}".format(searchstring=searchstring)
        
        if attribute == 'tag':
            attributeFilter = PIPointQuery(PICommonPointAttributes.Tag, AFSearchOperator.Equal, search);
        elif attribute == 'instrumenttag':
            attributeFilter = PIPointQuery(PICommonPointAttributes.InstrumentTag, AFSearchOperator.Equal, search);
        elif attribute == 'creationdate':
            search = date.today() - timedelta(days = daysback)
            search = "{search}".format(search=search)
            attributeFilter = PIPointQuery(PICommonPointAttributes.CreationDate, AFSearchOperator.GreaterThanOrEqual, search);
        elif attribute == 'descriptor':
            attributeFilter = PIPointQuery(PICommonPointAttributes.Descriptor, AFSearchOperator.Equal, search);
        elif attribute =='pointsource':
            attributeFilter = PIPointQuery(PICommonPointAttributes.PointSource, AFSearchOperator.Equal, search);
        if usestandardattributes:
            resattributes = self.standardattributesToLoad
        else:
            resattributes = List[String]()
            resattributes.Add(PICommonPointAttributes.InstrumentTag)  
        li = List[PIPointQuery]()
        li.Add(attributeFilter)
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoints.htm
        results = PIPoint.FindPIPoints(self.piServer,li,resattributes)
        
        points = []
        for pt in results:
            if usestandardattributes:
                
                
                points.append([pt.Name,pt.GetAttribute(PICommonPointAttributes.Descriptor),pt.GetAttribute(PICommonPointAttributes.PointType),pt.GetAttribute(PICommonPointAttributes.DigitalSetName),pt.GetAttribute(PICommonPointAttributes.TypicalValue),pt.GetAttribute(PICommonPointAttributes.InstrumentTag),pt.GetAttribute(PICommonPointAttributes.PointSource)])
            else:
                InstrumentTag = pt.GetAttribute(PICommonPointAttributes.InstrumentTag)
                points.append([pt.Name,InstrumentTag])
        return pd.DataFrame(points,columns=columns)
    
    
    def GetPointList(self, searchstring):
        """
        Searches pi for teh given searchstring and returns a list of pi point objects.

        Parameters
        ----------
        searchstring : TYPE
            DESCRIPTION. The value to search, both sides are wildcarded. *{searchstring}*

        Returns
        -------
        results : TYPE
            DESCRIPTION.

        """
        results = PIPoint.FindPIPoints(self.piServer,"*{searchstring}*".format(searchstring=searchstring), True)
        return results
    
    
    def GetPointListAsDF(self,searchstring):
        """
        Searches the searchstring for list of pi points and puts them into dataframe.

        Parameters
        ----------
        searchstring : TYPE
            DESCRIPTION. The value to search, both sides are wildcarded. *{searchstring}*

        Returns
        -------
        TYPE
            DESCRIPTION. dataframe of pi points by name and descriptor.

        """
        ptlist = self.GetPointList(searchstring)
        points = []
        for pt in ptlist:
            pt.LoadAttributes( None)
            descriptor = pt.GetAttribute(PICommonPointAttributes.Descriptor)
            points.append([pt.Name,descriptor])
   
        return pd.DataFrame(points,columns=['Name','descriptor'])
    
    def GetAllPointListAsDFWithStandardAttributes(self,searchstring):
        """
        Searches the searchstring for list of pi points and puts them into dataframe.

        Parameters
        ----------
        searchstring : TYPE
            DESCRIPTION. The value to search, both sides are wildcarded. *{searchstring}*

        Returns
        -------
        TYPE
            DESCRIPTION. dataframe of pi points by name and descriptor.

        """

        ptlist = PIPoint.FindPIPoints(self.piServer,"*{searchstring}*".format(searchstring=searchstring), True,self.standardattributesToLoad)
        points = []
        for pt in ptlist:
            #pt.LoadAttributes( None)
            #descriptor = pt.GetAttribute(PICommonPointAttributes.Descriptor)
            try:
               itag =  pt.GetAttribute(PICommonPointAttributes.InstrumentTag)
            except Exception as e:
                itag = None
            points.append([pt.Name, pt.GetAttribute(PICommonPointAttributes.Descriptor),pt.GetAttribute(PICommonPointAttributes.Tag),pt.GetAttribute(PICommonPointAttributes.PointType),pt.GetAttribute(PICommonPointAttributes.DigitalSetName),pt.GetAttribute(PICommonPointAttributes.TypicalValue),itag,pt.GetAttribute(PICommonPointAttributes.PointSource)])
   
        return pd.DataFrame(points,columns=['Name','descriptor','tag','pointtype','digitalsetname','typicalvalue','instrumenttag','pointsource'])
    def PointHasCurrentData(self,point):
        """
        Checks that Pi point has data and that data is not Pt*

        Parameters
        ----------
        point : TYPE
            DESCRIPTION.

        Returns
        -------
        bool
            DESCRIPTION.

        """
      
        if isinstance(point.CurrentValue().Value,AFEnumerationValue):
            value = point.CurrentValue().Value.Name
            print('val is {value}'.format(value=value))
            if 'Pt' in value:
                return False
        return True
    
    
    def PointHasValidData(self,point):
        """
        
        More robust implementation of PointHasCurrentData. Also checks if values is not No Data or Pt*
        Parameters
        ----------
        point : TYPE
            DESCRIPTION.

        Returns
        -------
        bool
            DESCRIPTION.

        """
      
        if isinstance(point.CurrentValue().Value,AFEnumerationValue):
            value = point.CurrentValue().Value.Name
            #print('val is {value}'.format(value=value))
            if 'Pt' in value or 'No Data' in value:
                return False
        return True
    def InsertPIData(self,df, point,dtypes=None):
        """
        
        #Takes in a dataframe dfin and table sql table and delete the current table data and reloads with the data in the dataframe.
        Parameters
        ----------
        df : TYPE
            DESCRIPTION.
        point : TYPE
            DESCRIPTION.
        dtypes : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        bool
            DESCRIPTION.

        """
        
        now = datetime.now()
        self.tepcore.debug('Starting to backfill {point} the data {now}'.format(now=now,point=point.Name),True)
        self.tepcore.debug(df.head(),True)
        writept = point 
        writeptname = writept.Name.lower()  
        df = self.SortDFByTime(df)
        self.tepcore.debug(df.head(),True)
        insertcnt = 0
        newValues =  AFValues();
        for index, row in df.iterrows():
            datavalue=row['value']
            
            time = self.GetTimeString(index)
            if insertcnt ==0:
                self.tepcore.debug('Inserting first value {datavalue}, {time} into {name}'.format(datavalue=datavalue, time=time,name=point.Name),True)
            insertcnt = insertcnt+1
            aftime = AFTime(time)
            #print('aftime is {aftime}'.format(aftime=aftime))
            val = AFValue() 
            val.Value = float(datavalue)
            val.Timestamp = aftime
            newValues.Add(val)
            #self.tepcore.Debug('backfill point time {aftime} value{val}'.format(aftime=aftime,val=val),True)
            #Cannot convert value to a PI equivalent type for writing. AFUpdateOption.Replace AFUpdateOption.Insert
        res = writept.UpdateValues(newValues, AFUpdateOption.NoReplace, AFBufferOption.BufferIfPossible)
        self.tepcore.debug ('res of insert')
        self.tepcore.debug(res)
        if res is not None:
            for error in res.Errors:
                self.tepcore.debug(error)
            self.tepcore.debug('Failed to insert data see error list')
            return False
        self.tepcore.debug('Inserted {insertcnt} values'.format(insertcnt=str(insertcnt)),True)
        now = datetime.now()
        self.tepcore.debug('Finished backfill the data {now}'.format(now=now),True)       
        return True       
    
    def RemovePIData(self,point,start,end):
        """
        Removes pi data for a point and start/end wrange. Firt fetches recorded values and then updates them with remove flag.

        Parameters
        ----------
        point : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        writept = point 
        timerange = AFTimeRange(start,end)  
        datatodelet = writept.RecordedValues(timerange, AFBoundaryType.Inside, "", False,2147483647)  
        writept.UpdateValues(datatodelet,AFUpdateOption.Remove,AFBufferOption.BufferIfPossible) 
    
    
    def GetBackFillDataFromPoint(self,admspoint, backpoint,start='-2y',end='*'):
        """
        

        Parameters
        ----------
        admspoint : TYPE
            DESCRIPTION.
        backpoint : TYPE
            DESCRIPTION.
        start : TYPE, optional
            DESCRIPTION. The default is '-2y'.
        end : TYPE, optional
            DESCRIPTION. The default is '*'.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
          #backpoint= 'ANALOG:03057013-BACKFILL'
        #admspoint = backpoint
        self.tepcore.Debug('Backpoint is {backpoint}, admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
        try:
            admsdata = self.GetPiPoint(admspoint)
            if admsdata is None:
                 self.tepcore.Debug('Adms point doesnt exsit admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
                 return False
                
        except PIException as p:
            self.tepcore.Debug('Adms point doesnt exsit admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
            return False
        
        self.tepcore.Debug('Adms point name {admsname}'.format(admsname=admsdata.Name),True)
        backdata = self.GetPiPoint(backpoint)
        if backdata is None:
             self.tepcore.Debug('BackData doesnt exsit', True)
             return False
        
        backdatacurvalue = backdata.CurrentValue()  
        self.tepcore.Debug('backdata point name {backdata} last value {backdatacurvalue}'.format(backdata=backdata.Name,backdatacurvalue=backdatacurvalue),True)
        if admspoint == admsdata.Name and backpoint == backdata.Name:
            self.tepcore.Debug('Both points exist',True)
            self.tepcore.Debug('Retriving date range {start}, {end}'.format(start = start, end=end))
            admsdf = self.GetDataForPoint(admsdata,start,end)
            print(admsdf.head())
            if not self.tepcore.isValidDf(admsdf):
                #return if data frame doesnt have an index
                if not math.isnan(admsdf.index.min()):
                    self.tepcore.Debug('failed to pull pi data for point',True)
                    return False
            self.tepcore.Debug(admsdf.index.min(),True)
            self.tepcore.Debug(admsdf.index.max(),True)
            backfilldf = self.GetDataForPoint(backdata,start,end)
            self.tepcore.Debug(backfilldf.index.min(),True)
            self.tepcore.Debug(backfilldf.index.max(),True)
           # print(admsdf.index.min())
            if isinstance(admsdf.index,int) or isinstance(backfilldf.index,int) :
                if math.isnan(admsdf.index.min()):
                    self.tepcore.Debug('No existing data overlap',True)
                    skipmerge = True
                elif math.isnan(backfilldf.index.min()):
                    self.tepcore.Debug('No backfill data that doesnt overlap',True)
                    return True
                    
                if admsdf.index.min() < backfilldf.index.max():
                    self.tepcore.Debug('reseting backfill data to minimum date',True)
                    #admsdf.index.min()
                    newmaxdt = self.GetTimeString(admsdf.index.min())
                    backfilldf = self.GetDataForPoint(backdata,start,newmaxdt)
                    self.tepcore.Debug (backfilldf.index.min(),True)
                    self.tepcore.Debug (backfilldf.index.max(),True)
            #print(admsdf.head())
            #print(backfilldf.head())
            #left merges the data frame then filters on teh _merge attribute that is added by indicator=True to give you only values in the backfill data frame that dont exist in adms data frame
            #if not skipmerge:
            mergedf = backfilldf.merge(admsdf, how = 'left' , left_index=True, right_index=True,indicator=True).loc[lambda x : x['_merge']=='left_only']  
            mergedf.rename(columns={'value_x':'value'},inplace=True)
            return mergedf
        
    
    def GetBackFillData(self,backpoint,start='-2y',end='*'):
        """
        

        Parameters
        ----------
        backpoint : TYPE
            DESCRIPTION.
        start : TYPE, optional
            DESCRIPTION. The default is '-2y'.
        end : TYPE, optional
            DESCRIPTION. The default is '*'.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
          #backpoint= 'ANALOG:03057013-BACKFILL'
        admspoint = backpoint.split('-')[0]
        self.tepcore.Debug('Backpoint is {backpoint}, admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
        try:
            admsdata = self.GetPiPoint(admspoint)
            if admsdata is None:
                 self.tepcore.Debug('Adms point doesnt exsit admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
                 return False
                
        except PIException as p:
            self.tepcore.Debug('Adms point doesnt exsit admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
            return False
        
        
        
        

        
        self.tepcore.Debug('Adms point name {admsname}'.format(admsname=admsdata.Name),True)
        backdata = self.GetPiPoint(backpoint)
        if backdata is None:
             self.tepcore.Debug('BackData doesnt exsit', True)
             return False
        
        backdatacurvalue = backdata.CurrentValue()  
        self.tepcore.Debug('backdata point name {backdata} last value {backdatacurvalue}'.format(backdata=backdata.Name,backdatacurvalue=backdatacurvalue),True)
        if admspoint == admsdata.Name and backpoint == backdata.Name:
            self.tepcore.Debug('Both points exist',True)
            self.tepcore.Debug('Retriving date range {start}, {end}'.format(start = start, end=end))
            admsdf = self.GetDataForPoint(admsdata,start,end)
            print(admsdf.head())
            if not self.tepcore.isValidDf(admsdf):
                #return if data frame doesnt have an index
                if not math.isnan(admsdf.index.min()):
                    self.tepcore.Debug('failed to pull pi data for point',True)
                    return False
            self.tepcore.Debug(admsdf.index.min(),True)
            self.tepcore.Debug(admsdf.index.max(),True)
            backfilldf = self.GetDataForPoint(backdata,start,end)
            self.tepcore.Debug(backfilldf.index.min(),True)
            self.tepcore.Debug(backfilldf.index.max(),True)
           # print(admsdf.index.min())
            if isinstance(admsdf.index,int) or isinstance(backfilldf.index,int) :
                if math.isnan(admsdf.index.min()):
                    self.tepcore.Debug('No existing data overlap',True)
                    skipmerge = True
                elif math.isnan(backfilldf.index.min()):
                    self.tepcore.Debug('No backfill data that doesnt overlap',True)
                    return True
                    
                if admsdf.index.min() < backfilldf.index.max():
                    self.tepcore.Debug('reseting backfill data to minimum date',True)
                    #admsdf.index.min()
                    newmaxdt = self.GetTimeString(admsdf.index.min())
                    backfilldf = self.GetDataForPoint(backdata,start,newmaxdt)
                    self.tepcore.Debug (backfilldf.index.min(),True)
                    self.tepcore.Debug (backfilldf.index.max(),True)
            #print(admsdf.head())
            #print(backfilldf.head())
            #left merges the data frame then filters on teh _merge attribute that is added by indicator=True to give you only values in the backfill data frame that dont exist in adms data frame
            #if not skipmerge:
            mergedf = backfilldf.merge(admsdf, how = 'left' , left_index=True, right_index=True,indicator=True).loc[lambda x : x['_merge']=='left_only']  
            mergedf.rename(columns={'value_x':'value'},inplace=True)
            return mergedf
            #return backfilldf
    
    def BackFillPoint(self,backpoint,start='-2y',end='*',usecreatedt=False):
        """
        

        Parameters
        ----------
        backpoint : TYPE
            DESCRIPTION.
        start : TYPE, optional
            DESCRIPTION. The default is '-2y'.
        end : TYPE, optional
            DESCRIPTION. The default is '*'.
        usecreatedt : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        bp = self.GetPiPoint(backpoint)
        bp.LoadAttributes( None)
        digitaltype = bp.GetAttribute(PICommonPointAttributes.DigitalSetName)
        creationdate = bp.GetAttribute(PICommonPointAttributes.CreationDate)
        pointype = bp.GetAttribute(PICommonPointAttributes.PointType)
        
        
        admspoint = backpoint.split('-')[0]
        self.tepcore.Debug('Backpoint is {backpoint}, admspoint is {admspoint}'.format(admspoint=admspoint,backpoint=backpoint),True)
        
        admspoint = self.GetPiPoint(admspoint)
        if admspoint is None:
            self.tepcore.debug('{name}, point is not found skipping'.format(pointype=pointype, name= backpoint.split('-')[0]),True)
            return True
        if 'STATUS' in admspoint.Name or 'ANALOG' in admspoint.Name:
            self.tepcore.debug('{name}, point is supported'.format(pointype=pointype, name=admspoint.Name),True)
            admspoint.LoadAttributes( None)
            admscreatingondt = admspoint.GetAttribute(PICommonPointAttributes.CreationDate)
        else:
            self.tepcore.debug('{name}, point is not supported skipping'.format(pointype=pointype, name=admspoint.Name),True)
            return True
        
        if not self.PointHasCurrentData(bp):
            self.tepcore.debug('Back point doesnt have any data skipping'.format(pointype=pointype, name=admspoint.Name),True)
            return True
            
            
            
        if not self.SupportedPointType(pointype):
            self.tepcore.debug('{name}, point type {pointype} is not supporeted skipping item'.format(pointype=pointype, name=backpoint.Name),True)
            return True
        if usecreatedt:
            end = self.GetTimeStamp(str(admscreatingondt))
            end = self.GetTimeString(end)
        mergedf = self.GetBackFillData(backpoint,start,end)
        if self.tepcore.isValidDf(mergedf):
            admspoint = backpoint.split('-')[0]
            admsdata = self.GetPiPoint(admspoint)
            status = self.InsertPIData(mergedf,admsdata)
            return status
        else:
            self.tepcore.Debug('failed or missing point data boo',True)
            return False
    
    def BackFillPointByTags(self,newtag,oldtag, start='-2y',end='*',usecreatedt=False):
        """
        

        Parameters
        ----------
        newtag : TYPE
            DESCRIPTION.
        oldtag : TYPE
            DESCRIPTION.
        start : TYPE, optional
            DESCRIPTION. The default is '-2y'.
        end : TYPE, optional
            DESCRIPTION. The default is '*'.
        usecreatedt : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        bp = self.GetPiPoint(oldtag)
        bp.LoadAttributes( None)
        digitaltype = bp.GetAttribute(PICommonPointAttributes.DigitalSetName)
        creationdate = bp.GetAttribute(PICommonPointAttributes.CreationDate)
        pointype = bp.GetAttribute(PICommonPointAttributes.PointType)
        
        
       
        self.tepcore.Debug('Backpoint is {oldtag}, admspoint is {newtag}'.format(oldtag=oldtag,newtag=newtag),True)
        
        admspoint = self.GetPiPoint(newtag)
        if admspoint is None:
            self.tepcore.debug('{name}, point is not found skipping'.format(pointype=pointype, name= backpoint.split('-')[0]),True)
            return True
        if 'STATUS' in admspoint.Name or 'ANALOG' in admspoint.Name:
            self.tepcore.debug('{name}, point is supported'.format(pointype=pointype, name=admspoint.Name),True)
            admspoint.LoadAttributes( None)
            admscreatingondt = admspoint.GetAttribute(PICommonPointAttributes.CreationDate)
        else:
            self.tepcore.debug('{name}, point is not supported skipping'.format(pointype=pointype, name=admspoint.Name),True)
            return True
        
        if not self.PointHasCurrentData(bp):
            self.tepcore.debug('Back point doesnt have any data skipping'.format(pointype=pointype, name=admspoint.Name),True)
            return True
            
            
            
        if not self.SupportedPointType(pointype):
            self.tepcore.debug('{name}, point type {pointype} is not supporeted skipping item'.format(pointype=pointype, name=backpoint.Name),True)
            return True
        if usecreatedt:
            end = self.GetTimeStamp(str(admscreatingondt))
            end = self.GetTimeString(end)
        mergedf = self.GetBackFillDataFromPoint(newtag,oldtag,start,end)
        if self.tepcore.isValidDf(mergedf):

            admsdata = self.GetPiPoint(newtag)
            status = self.InsertPIData(mergedf,admsdata)
            return status
        else:
            self.tepcore.Debug('failed or missing point data boo',True)
            return False
        
    
    
    def GetScadaKeysFromSubHierarch(self,afhierarch):
        res = []
        for substationhier in afhierarch:
            for element in substationhier[1]:
                
                for attribute in element.Attributes:
                    try:
                        #print(attribute.PIPoint)
                        #attribute.PIPoint.LoadAttributes (PICommonPointAttributes.InstrumentTag)
                        #.GetAttribute (PICommonPointAttributes.InstrumentTag)
                        if attribute.PIPoint is not None:
                            res.append([element.Name,attribute.PIPoint,attribute.Name])
                    except Exception as e:
                        print(str(e))
                    
        return res
                
    
    def GetSubstationHeirarchfromAf(self,heirarchy):
        rethierarchy = []
        hierarchylist =heirarchy.split('/')
        
        substationelements = self.GetAFElementList(hierarchylist[0])
        for substationelement in substationelements:
            rethierarchy.append([substationelement.Name,self.GetAFElementList(heirarchy+'\\'+substationelement.Name)])
        return rethierarchy
        
    def GetAFElementList(self,element):
        self.tepcore.debug('searching for heirarch of item' + element)
        retlist = []
        elementsLevel1= AFNamedCollectionList[AFElement]()
        searchitem = self.piServer.Elements.get_Item(element)
        print('found item' + searchitem.Name)
        searchitemlist = elementsLevel1.Add(searchitem)
        searchitemelements = AFElement.LoadElementReferences(searchitemlist)
        for ele in searchitem.Elements:
            #print(ele)
            retlist.append(ele)
        #heierarchy = AFElement.LoadElementsToDepth(searchitem.Elements, Boolean(True),3,1000)
        #for ele in heierarchy:
            #print(ele)

         #   retlist.append(ele.Name)
        #    for attr in ele.Attributes:
                #get the current value if you want to 
        #        attval = attr.GetValue()
        return retlist
    
    
    def GetDataForElements(self,elements,start,end):
        """
        

        Parameters
        ----------
        elements : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.

        Returns
        -------
        finaldf : TYPE
            DESCRIPTION.

        """
        self.tepcore.debug('pi elements {elements} for frame start: {start} to end {end}'.format(elements=elements, start=start, end=end))
        finaldf = pd.DataFrame()
        if self.useaf:
            for element in elements:
                element = self.piServer.Elements.get_Item(element)
                    
                for attr in element.Attributes:
                    #get the current value if you want to 
                    attval = attr.GetValue()
                    times=[]
                    vals=[]
                    timerange = AFTimeRange(start,end)
                    allvals = attr.GetValues(timerange,-1, attr.DefaultUOM)
                    for event in allvals:

                        if not isinstance(event.Value,float):
 
                            if isinstance(event.Value,AFEnumerationValue):
                                
                                #Handle Telemetry Failure mapping
                                if event.Value.Value == 8:
                                    times.append(self.GetTimeStamp(str(event.Timestamp)))
                                    vals.append([1, attr.Name, self.GetTimeStringOati(event.Timestamp.LocalTime)])
                                    
                                #Handle Estminated mapping
                                elif event.Value.Value == 16:
                                    times.append(self.GetTimeStamp(str(event.Timestamp)))
                                    vals.append([0, attr.Name, self.GetTimeStringOati(event.Timestamp.LocalTime)])
                                    
                                else:
                                    times.append(self.GetTimeStamp(str(event.Timestamp)))
                                    vals.append([event.Value.Value, attr.Name,self.GetTimeStringOati(event.Timestamp.LocalTime)])
                                    
                        #for v in event.Values:
                        else:
                            times.append(self.GetTimeStamp(str(event.Timestamp)))
                            #Fix format to 4 decimal places
                            vals.append(["{0:0.4f}".format(event.Value), attr.Name,self.GetTimeStringOati(event.Timestamp.LocalTime)])
                    df = pd.DataFrame(vals,index=times,columns=['value','Attribute','Time'])
                   
                    finaldf =finaldf.append(df)
        else:
            self.tepcore.debug('This method is only supported with a PI AF connection') 

        return finaldf
      
    def GetIntervalForElement(self, elements, start, end, interval):
        """
        

        Parameters
        ----------
        elements : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        finaldf : TYPE
            DESCRIPTION.

        """
        self.tepcore.debug('pi elements {elements} for frame start: {start} to end {end}'.format(elements=elements, start=start, end=end))
        finaldf = pd.DataFrame()
        if self.useaf:
            for element in elements:
                
                for ele in self.piServer.Elements: 
                    print(ele)
                element = self.piServer.Elements.get_Item(element)
                for attr in element.Attributes:
                    #get the current value if you want to 
                    attval = attr.GetValue()
                    times=[]
                    vals=[]
                    timerange = AFTimeRange(start,end)
                    span = AFTimeSpan.Parse(interval)
                    
                    #get values with new time interval or span and with timeweighted intervals
                    #Data.Summaries return a AFValues
                    allvals = attr.Data.InterpolatedValues(timerange,span, attr.DefaultUOM, "", True)
                    
                    for event in allvals:
                        values = event.Value
                        
                        #Adjust times for Pacific Standard Time and Webaccounting MST
                        times.append(self.GetTimeStamp(str(event.Timestamp)) + timedelta(hours=-1, minutes=5))
                        vals.append([event.Value, attr.Name,self.GetTimeStamp(str(event.Timestamp)) + timedelta(hours=-1, minutes=5)])
                    
                    df = pd.DataFrame(vals,index=times,columns=['value','Attribute','Time'])
                    
                    finaldf =finaldf.append(df)
        else:
            self.tepcore.debug('This method is only supported with a PI AF connection') 

        return finaldf
    
    def GetIntervalDataForElement(self, elements, start, end, interval):
        """
        m

        Parameters
        ----------
        elements : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        finaldf : TYPE
            DESCRIPTION.

        """
        self.tepcore.debug('pi elements {elements} for frame start: {start} to end {end}'.format(elements=elements, start=start, end=end))
        finaldf = pd.DataFrame()
        
        if self.useaf:
            for element in elements:
                element = self.piServer.Elements.get_Item(element)
                    
                for attr in element.Attributes:
                    #get the current value if you want to
                    
                    attval = attr.GetValue()
                    times=[]
                    vals=[]
                    timerange = AFTimeRange(start,end)
                    span = AFTimeSpan.Parse(interval)
                    
                    #get values with new time interval or span and with timeweighted intervals
                    #Data.Summaries return a AFValues
                    allvals = attr.Data.Summaries(timerange,span, 2, 0, 1)
                    
                    for event in allvals:
                        values = event.Value
                        
                        for value in values:
                            
                            vals.append([value.Value, attr.Name,self.GetTimeStamp(str(value.Timestamp))])
                            times.append(self.GetTimeStamp(str(value.Timestamp)))
                        
                
                    df = pd.DataFrame(vals,index=times,columns=['value','Attribute','Time'])
           
                    finaldf =finaldf.append(df)
        else:
            self.tepcore.debug('This method is only supported with a PI AF connection') 

        return finaldf
    
    def PIAdvCalcDat(self,pt,start,end,interval,calutype):
        """
        
        Provides the end time of the data as opposed to the start by adding timedelta to start value
        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.Summaries (timerange, span, calutype, 0, 1)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)

        return finaldf
    
    def PIFilteredCalcTotal_timeweighted(self,pt,filterstr,start,end,interval,conversionfactor=1):
        """
        

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.FilteredSummaries (timerange, span, filterstr, AFSummaryTypes.Total, AFCalculationBasis.TimeWeighted,AFSampleType.ExpressionRecordedValues,span,AFTimestampCalculation.MostRecentTime)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)
        
        #print(finaldf.head())
        #print(finaldf.dtypes)
       # print(conversionfactor)
       # print(type(conversionfactor))

        finaldf['value'] = finaldf['value'].apply(lambda x: x*conversionfactor)

        return finaldf
    
    
    def PISummariesCalc_WithConversion(self,pt,start,end,interval,calctype=None,conversionfactor=1):
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
        sumtype=0
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            if calctype is None:
                sumtype = AFSummaryTypes.Total
            elif calctype=='total':
                sumtype = AFSummaryTypes.Total
            elif calctype=='avg':
                sumtype = AFSummaryTypes.Average
            elif calctype=='min':
                sumtype = AFSummaryTypes.Minimum
            elif calctype=='max':
                sumtype = AFSummaryTypes.Maximum
            elif calctype=='StdDev':
                sumtype = AFSummaryTypes.StdDev  
                
            

            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.Summaries (timerange, span, sumtype, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.EarliestTime)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)
        finaldf['value'] = finaldf['value'].apply(lambda x: x*conversionfactor)
        return finaldf
    
    def PISummariesCalc(self,pt,start,end,interval,calctype=None):
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
        sumtype=0
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            if calctype is None:
                sumtype = AFSummaryTypes.Total
            elif calctype=='total':
                sumtype = AFSummaryTypes.Total
            elif calctype=='avg':
                sumtype = AFSummaryTypes.Average
            elif calctype=='min':
                sumtype = AFSummaryTypes.Minimum
            elif calctype=='max':
                sumtype = AFSummaryTypes.Maximum
            elif calctype=='StdDev':
                sumtype = AFSummaryTypes.StdDev  
                
            

            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            recorded = pt.Summaries (timerange, span, sumtype, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.EarliestTime)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)

        return finaldf
    def PIFilteredCalc(self,pt,filterstr,start,end,interval,conversionfactor=1,calctype=None):
        """
        

        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        finaldf = pd.DataFrame()
        times=[]
        vals=[]
        sumtype=0
       
        timerange = AFTimeRange(start, end)  
        span = AFTimeSpan.Parse(interval);
        #pt = PIPoint.FindPIPoint(self.piServer, 'ANALOG:03057028')
        
        try:
            #recorded = pt.Summaries (timerange, span, AFSummaryTypes.Average, AFCalculationBasis.TimeWeighted, AFTimestampCalculation.MostRecentTime)  
            if calctype is None:
                sumtype = AFSummaryTypes.Total
            elif calctype=='total':
                sumtype = AFSummaryTypes.Total
            elif calctype=='avg':
                sumtype = AFSummaryTypes.Average
            elif calctype=='min':
                sumtype = AFSummaryTypes.Minimum
            elif calctype=='max':
                sumtype = AFSummaryTypes.Maximum
            elif calctype=='StdDev':
                sumtype = AFSummaryTypes.StdDev  
                
            
            recorded = pt.FilteredSummaries (timerange, span, filterstr, sumtype, AFCalculationBasis.TimeWeighted,AFSampleType.ExpressionRecordedValues,span,AFTimestampCalculation.EarliestTime)  #Returns a Dictionary

        except Exception as e:
            self.tepcore.debug(str(e),True)
            return False

        for event in recorded: #should be just Average summary type as specified
            
            
            values = event.Value

            for val in values:
                times.append(self.GetTimeStamp(str(val.Timestamp)))
                vals.append([val.Value])
                    
        df = pd.DataFrame(vals,index=times,columns=['value'])   
                   
        finaldf =finaldf.append(df)

        finaldf['value'] = finaldf['value'].apply(lambda x: x*conversionfactor)

        return finaldf
    
    def GetPointFromTagName(self,tagname):
        return PIPoint.FindPIPoint(self.piServer, tagname)
    
    def GetPointStandardAttributes(self,point):
        ptlist = []
        retpoint = []
        ptlist.append(point)
        for pt in ptlist:
            #pt.LoadAttributes( None)
            #descriptor = pt.GetAttribute(PICommonPointAttributes.Descriptor)
            for item in self.standardattributesToLoad:
            
                try:
                   attribvalue =  pt.GetAttribute(item)
                except Exception as e:
                    attribvalue = None
                    itag = None
                retpoint.append([item,attribvalue])
        

            
            
        return retpoint
    
    def PrintStansrdItems(self):
        for item in self.standardattributesToLoad:
            print(item)
            
            
    def CreatePIPointWithAttibutes(self, descriptor, tag, pointtype, location1, location2, location3, location4, location5, digitalset, pointsource, ptclassname, typicalvalue):
        ptcreate=False
        d = Dictionary[String, Object]()
        d[PICommonPointAttributes.Descriptor] = descriptor
        d[PICommonPointAttributes.InstrumentTag] = tag
        d[PICommonPointAttributes.PointType] = pointtype
        d[PICommonPointAttributes.Location1] = location1
        d[PICommonPointAttributes.Location2] = location2
        d[PICommonPointAttributes.Location3] = location3
        d[PICommonPointAttributes.Location4] = location4
        d[PICommonPointAttributes.Location5] = location5
        d[PICommonPointAttributes.DigitalSetName] = digitalset
        d[PICommonPointAttributes.PointSource] = pointsource
        d[PICommonPointAttributes.PointClassName] = ptclassname
        d[PICommonPointAttributes.TypicalValue] = typicalvalue
    
        print('Tag to be created in Corp -', 'Name:', tag, 'Description:', descriptor)
    
        try:
            pointcreated = self.GetPiServer().CreatePIPoint(tag,d)
            print('Tag name created: ', pointcreated)
            ptcreate = True
        except Exception as exception:
            print("ERROR Tag name from EMS PI:", tag, exception)
    
        return ptcreate
    
    def CreatePIPointsWithAttibutesFromDataframe(self,idataframe):
        idataframe['pointinserted'] = idataframe.apply(lambda row : self.CreatePIPointWithAttibutes(row['descriptor'], row['tag'], row['pointtype'], row['location1'], row['location2'], row['location3'], row['location4'], row['location5'], row['digitalset'], row['pointsource'], row['ptclassname'], row['typicalvalue']), axis = 1) 
        return idataframe
    
    def IsEnumerationValue(self,value):
        '''
        

        Parameters
        ----------
        value : A pi tag value object
            DESCRIPTION.

        Returns
        -------
        bool
            True if is AFEnumerationValue object.

        '''
        if isinstance(value,AFEnumerationValue):
            return True
        return False
    
    
    
    def AddAfElement(self,element,template):
        
        try:
           newele = self.piServer.Elements.Add(element,template)
        
           return newele
        except Exception as e:
            print(str(e))
            return None
    
    def AddAfElementtoParent(self,parent,element,template):
        
        try:
           newele = parent.Elements.Add(element,template)
        
           return newele
        except Exception as e:
            print(str(e))
            return None
        
        
    def GetAfElement(self,element):
        '''
        Gets a PI AF element else return none.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        
        if self.piServer.Elements.get_Item(element) is None:
            return None
        else:
            return self.piServer.Elements.get_Item(element)
        
    def GetAfElementFromParent(self,parent,element):
        '''
        Gets a PI AF element else return none.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        
        if parent.Elements.get_Item(element) is None:
            return None
        else:
            return parent.Elements.get_Item(element)
    
    def GetAfElementTemplate(self,templatename):
        '''
        Gets a pi af element template given  the name if it doesnt exist returns none.

        Parameters
        ----------
        templatename : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        #for ele in self.piServer.ElementTemplates:
        #    print(ele.Name)
        
        if self.piServer.ElementTemplates.get_Item(templatename) is None:
            return None
        else:
            return self.piServer.ElementTemplates.get_Item(templatename)
        
        
    def CheckInAf(self):
        '''
        This is a bulk check in only need to call once.

        Returns
        -------
        None.

        '''
        self.piServer.CheckIn();
    
    def CreateAfElementWithTemplate(self,parent ,child, template):
        '''
        'Distribution' ,station, "Substation - TEP"

        Parameters
        ----------
        parent : TYPE
            DESCRIPTION.
        child : TYPE
            DESCRIPTION.
        template : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        getelementfromelementinpout = False
        #print(str(type(parent)))
        # if we pass in an afelement no need to look for its parent that is the parent af element. If we pass in a string we need to look it up.
        if 'AFElement' in str(type(parent)):
            #print('this is a paretn af element')
            getelementfromelementinpout= True
        
        #print('getelementfromelementinpout {getelementfromelementinpout}'.format(getelementfromelementinpout=getelementfromelementinpout))
            
        
        
        template = self.GetAfElementTemplate(template)
        if template is None:
            self.tepcore.log_error('tepcorepidata CreateAfElementWithTemplate failed for template {template}'.format(template=template))
            return False
        else:
            self.tepcore.log_info('tepcorepidata CreateAfElementWithTemplate found valid template {template}'.format(template=template))
            if getelementfromelementinpout:
                parent = parent #self.GetAfElementFromParent(parent,child)
            else: 
                parent = self.GetAfElement(parent)
            if parent is not None:
                
                if self.GetAfElement(child) is None:
                    self.tepcore.log_info('tepcorepidata CreateAfElementWithTemplate attempting to add child {child} element to parent {parent}'.format(parent=parent,child=child))
                    newele = self.AddAfElementtoParent(parent,child,template)
                    self.CheckInAf()
                    print(type(newele))
                    return newele
                else:
                    self.tepcore.log_error('tepcorepidata CreateAfElementWithTemplate child {child} element already exists  for parent {parent}'.format(parent=parent,child=child))
                    return False
                    
                
            else:
                self.tepcore.log_error('tepcorepidata CreateAfElementWithTemplate failed parent element doesnt exist parent {parent}'.format(parent=parent))
                return False
    
    def GetInstrumentTagFromPiPointDR(self,tagPiPointDR):
        #print(server)
        #print(tagfqdm.PIPoint)
        itag= None
        name = None
        try:
            pt = tagPiPointDR.PIPoint
            name = pt.Name.lower()
            
            try:
                pt.LoadAttributes( None)
                itag =  pt.GetAttribute(PICommonPointAttributes.InstrumentTag)
                #print(' itag {itag}'.format(itag=itag))
            except Exception as e:
                #print(str(e))
                itag = None
        except Exception as e:
            self.tepcore.debug('Unable to find pi point {tagPiPointDR}'.format(tagPiPointDR=tagPiPointDR))
            name = None
        return [name,itag]
    
    def GetAfToTagMapping(self,attribute='*',usealldbs=True):
        '''
        '''
        if self.afattributmapcache is None:
            print('GetAfToTagMapping')
            columns = ['afdbname','attributname','tagfqdnname','afconfigstring','tagname','instrumenttag']
            data = []
            print( self.afdbs)
            if usealldbs:
                dbs = self.afdbs
            else:
                dbs = [self.piServer]
                
            for db in dbs:
                self.tepcore.debug('working db {db}'.format(db=db))
                attsearch =  AFAttributeSearch(db, "All PI Tags", "Element:{Name:'" + "*{attribute}*'".format(attribute=attribute) + "}             PlugInName:'PI Point' ");
                #print(attsearch)
        
                #AFAtts = attsearch.FindObjects();
                for AFAtts in attsearch.FindObjects():
                    #print(AFAtts.Name)
                    #print(AFAtts.DataReference)
                    if AFAtts.DataReference is not None:
                        tagname,instrumenttag = self.GetInstrumentTagFromPiPointDR(AFAtts.DataReference)
                        #print(AFAtts.DataReference.ConfigString)
                        data.append([self.piServer,AFAtts.Name,AFAtts.DataReference,AFAtts.DataReference.ConfigString,tagname,instrumenttag])
                        
                    else:
                        data.append([self.piServer,AFAtts.Name,AFAtts.DataReference,None,tagname,instrumenttag])
                        
                print('loaded {cnt} tag mappings'.format(cnt=str(len(data))))
                self.afattributmapcache =  pd.DataFrame(data,columns=columns)
        return self.afattributmapcache
    
    def QueryAfTagMapping(self,tagname):
        
        self.GetAfToTagMapping()
        
        res = self.afattributmapcache.query('tagname==@tagname')
        return res
    def GetMaxForElementWithAtts(self,elements,start,end,interval,timedltainterval=timedelta(hours=1),attributes=None,getChildAttributes=False):
        """
        
        Provides the end time of the data as opposed to the start by adding timedelta to start value
        Parameters
        ----------
        pt : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        self.tepcore.debug('pi elements {elements} for frame start: {start} to end {end}'.format(elements=elements, start=start, end=end))
        finaldf = pd.DataFrame()
        if self.useaf:
            for element in elements:
                
                #for ele in self.piServer.Elements: 
                #    print(ele)
                
                pielement = self.piServer.Elements.get_Item(element[2])
                print('fetching data for {pielement}'.format(pielement=pielement))
                if attributes is None:
                    for attr in pielement.Attributes:
                        #get the current value if you want to 
                        attval = attr.GetValue()
                        times=[]
                        vals=[]
                        timerange = AFTimeRange(start,end)
                        span = AFTimeSpan.Parse(interval)
                        
                        #get values with new time interval or span and with timeweighted intervals
                        #Data.Summaries return a AFValues
                        #for event in allvals:
                        #values = event.Value
                        
                        #for value in values:
                            
                        #    vals.append([value.Value, attr.Name,self.GetTimeStamp(str(value.Timestamp))])
                        #    times.append(self.GetTimeStamp(str(value.Timestamp)))
                        allvals = attr.Data.Summaries(timerange,span, 8, 0 ,1)
                        for allval in allvals:
                            values = allval.Value
                            

                        
                            for event in values:
    
                                
                                #Adjust times for Pacific Standard Time and Webaccounting MST
                                times.append(self.GetTimeStamp(str(event.Timestamp))+timedltainterval)
                                vals.append([event.Value, attr.Name,self.GetTimeStamp(str(event.Timestamp)) + timedltainterval,pielement,element[0],element[1]])
                        
                        df = pd.DataFrame(vals,index=times,columns=['value','Attribute','Time','Element','aor','substation'])
                        
                        finaldf =finaldf.append(df)
                else:
                    attrlist = []
                    for attr in pielement.Attributes:
                        #print('wokring attributes {attr}'.format(attr= attr))
                        if str(attr) in attributes:
                            #get the current value if you want to 
                            attrlist.append(attr)
                            #print('Found attributes {attr}'.format(attr= attr))
                            if getChildAttributes:
                                for x in attr.Attributes:
                                    attrlist.append(x)
                            
                    if len(attrlist)> 0:  
                        for attr in attrlist:
                            #get the current value if you want to 
                            attval = attr.GetValue()
                            times=[]
                            vals=[]
                            timerange = AFTimeRange(start,end)
                            span = AFTimeSpan.Parse(interval)
                            
                            #get values with new time interval or span and with timeweighted intervals
                            #Data.Summaries return a AFValues
                            allvals = attr.Data.Summaries(timerange,span, 8, 0 ,1)
                            for allval in allvals:
                                values = allval.Value
                                
    
                            
                                for event in values:
        
                                    
                                    #Adjust times for Pacific Standard Time and Webaccounting MST
                                    times.append(self.GetTimeStamp(str(event.Timestamp))+timedltainterval)
                                    vals.append([event.Value, attr.Name,self.GetTimeStamp(str(event.Timestamp)) + timedltainterval,pielement,element[0],element[1]])
                            
                            df = pd.DataFrame(vals,index=times,columns=['value','Attribute','Time','Element','aor','substation'])
                            
                            finaldf =finaldf.append(df)
        else:
            self.tepcore.debug('This method is only supported with a PI AF connection') 
    
        return finaldf

        