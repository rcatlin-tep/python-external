# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 10:48:08 2022

@author: UA58436
"""

import pandas as pd
class DataUtil:
    def __init__(self,tc,pidata,settings):
        self.tc = tc
        self.pidata = pidata
        self.settings = settings
    
    def get_point_time_weighted_data_df_avg_withfilters(self,tag_name,start,end,interval,filters,conversionfactor=1):
        self.tc.debug('get_point_time_weighted_data_df tag {tag_name} start {start} to end  {end}'.format(tag_name=tag_name,start=start,end=end))
        tag = self.pidata.GetPiPoint(tag_name)
        #tag_data_df = self.pidata.PIAdvCalcDat_timeweighted(tag,self.yesterday_1am,self.today_1am,self.interval) 
        tag_data_df = self.pidata.PIFilteredCalc(tag,filters,start,end,interval,conversionfactor,'avg')
        return tag_data_df
    def get_point_time_weighted_data_df_total_withfilters(self,tag_name,start,end,interval,filters,conversionfactor=1):
        self.tc.debug('get_point_time_weighted_data_df tag {tag_name} start {start} to end  {end}'.format(tag_name=tag_name,start=start,end=end))
        tag = self.pidata.GetPiPoint(tag_name)
        #tag_data_df = self.pidata.PIAdvCalcDat_timeweighted(tag,self.yesterday_1am,self.today_1am,self.interval) 
        tag_data_df = self.pidata.PIFilteredCalc(tag,filters,start,end,interval,conversionfactor,'total')
        return tag_data_df
    
    def get_point_time_weighted_data_calc_withfilters(self,tag_name,start,end,interval,filters,conversionfactor=1,calc=None):
        self.tc.debug('get_point_time_weighted_data_calc_withfilters tag {tag_name} start {start} to end  {end}, calc {calc}'.format(tag_name=tag_name,start=start,end=end,calc=calc))
        tag = self.pidata.GetPiPoint(tag_name)
        #tag_data_df = self.pidata.PIAdvCalcDat_timeweighted(tag,self.yesterday_1am,self.today_1am,self.interval) 
        tag_data_df = self.pidata.PIFilteredCalc(tag,filters,start,end,interval,conversionfactor,calc)
        #print(tag_data_df.head())
        return tag_data_df
    
    def get_point_time_weighted_data_df(self,tag_name,start,end,interval,cacl):
        self.tc.debug('get_point_time_weighted_data_df tag {tag_name} start {start} to end  {end} interval {interval}'.format(tag_name=tag_name,start=start,end=end,interval=interval))
        tag = self.pidata.GetPiPoint(tag_name)
        #tag_data_df = self.pidata.PIAdvCalcDat_timeweighted(tag,self.yesterday_1am,self.today_1am,self.interval) 
        #PISummariesCalc(self,pt,start,end,interval,calctype=None):
        tag_data_df = self.pidata.PISummariesCalc(tag,start,end,interval,cacl)
        return tag_data_df


    def get_point_time_weighted_data_df_w_conversion(self,tag_name,start,end,interval,cacl,conversion):
        self.tc.debug('get_point_time_weighted_data_df tag {tag_name} start {start} to end  {end} interval {interval}'.format(tag_name=tag_name,start=start,end=end,interval=interval))
        tag = self.pidata.GetPiPoint(tag_name)
        #tag_data_df = self.pidata.PIAdvCalcDat_timeweighted(tag,self.yesterday_1am,self.today_1am,self.interval) 
        #PISummariesCalc(self,pt,start,end,interval,calctype=None):
        tag_data_df = self.pidata.PISummariesCalc_WithConversion(tag,start,end,interval,cacl,conversion)
        return tag_data_df
    

    def CreateDateTimeColumns(self,frmt):
        columns = []
        if frmat =='AB_DATE_TIME':
            columns.append(['A','DATE'])
            columns.append(['B','TIME'])
            
        return columns
        

    def GetDataForPointsInConfig(self,start,end,interval=None):
        retdf = None
        columns = []
        warnings = []
        columns.append(['A','DATE'])
       # columns.append(['B','TIME'])
        for configpoint in self.settings.pi_point_map:
            self.tc.debug('Working config item {configpoint}'.format(configpoint=str(configpoint)))
            pointname = configpoint['point']
            pointdisplay = configpoint['point_name_display']
            reportcolumn = configpoint['report_column']
            if interval is None:
                interval = configpoint['interval']
            picalctype = configpoint['picalctype']
            pifilter = configpoint['pifilter']
            piconversionfactor = configpoint['piconversionfactor']
            
            if picalctype.lower()=='total' and  len(pifilter)<=0 and len(piconversionfactor)>=0:
            
                tagdf = self.get_point_time_weighted_data_df_w_conversion(pointname,start,end,interval,picalctype.lower(),float(piconversionfactor))
            elif (picalctype.lower()=='total' or picalctype.lower()=='avg'  or picalctype.lower()=='min'  or picalctype.lower()=='max')  and  len(pifilter)>=0 or len(piconversionfactor)>=0:
                if len(piconversionfactor)<=0:
                    piconversionfactor = 1
                if len(pifilter)>0:
                    tagdf = self.get_point_time_weighted_data_calc_withfilters(pointname,start,end,interval,pifilter,float(piconversionfactor),picalctype)
                else:
                    tagdf = self.get_point_time_weighted_data_df(pointname,start,end,interval,picalctype)
                    #print(tagdf)
            
            
            if self.tc.isValidDf(tagdf):
               # tagdf['point_name_display'] = pointdisplay
                #tagdf['report_column'] =reportcolumn
                #tagdf['date'] = tagdf.index
                #date is the innitial index
                #tagdf.reset_index(inplace=True)
                columns.append([reportcolumn,pointdisplay])
                tagdf.rename(columns={"value": pointdisplay},inplace=True)
                #print(tagdf.dtypes)
                #print(tagdf.head())
                #dfs.append(tagdf)
                if retdf is None:
                    retdf = tagdf
                else:
                    #for index to merge timestamps returned from pi need to be the same such as by using AFTimestampCalculation.MostRecentTime
                    retdf =pd.merge(retdf, tagdf, left_index=True, right_index=True)
            
            else:
                self.tc.debug('Failed to get data for tag {pointname}'.format(pointname=pointname))
                warnings.append(['Failed to get data for pi point' ,str(configpoint)])
        
        if self.tc.isValidDf(retdf):

            #print('convert index to column date')
            #print(columns)
            #retdf.loc['Total']= retdf.sum(numeric_only=True, axis=0)

          #  for configpoint in self.settings.calulated_column_map:
          #      columns.append([configpoint['report_column'],configpoint['column_dispaly']])
            
            
            
            columns = (sorted(columns, key=lambda x: x[0], reverse=False))
            colorder = [row[1] for row in columns]
            retdf['DATE'] = retdf.index
            retdf = retdf[colorder]
        
        return [retdf,warnings]
    
    
    def GetDataForPointsInConfigForMultipleReports(self,pointmap,start,end,interval=None):
        retdf = None
        columns = []
        warnings = []
        columns.append(['A','DATE'])
       # columns.append(['B','TIME'])
        for configpoint in pointmap:
            self.tc.debug('Working config item {configpoint}'.format(configpoint=str(configpoint)))
            pointname = configpoint['point']
            pointdisplay = configpoint['point_name_display']
            reportcolumn = configpoint['report_column']
            if interval is None:
                interval = configpoint['interval']
            picalctype = configpoint['picalctype']
            pifilter = configpoint['pifilter']
            piconversionfactor = configpoint['piconversionfactor']
            exceltab = configpoint['exceltab'] 
            
            if picalctype.lower()=='total' and  len(pifilter)<=0 and len(piconversionfactor)>=0:
            
                tagdf = self.get_point_time_weighted_data_df_w_conversion(pointname,start,end,interval,picalctype.lower(),float(piconversionfactor))
            elif (picalctype.lower()=='total' or picalctype.lower()=='avg'  or picalctype.lower()=='min'  or picalctype.lower()=='max')  and  len(pifilter)>=0 or len(piconversionfactor)>=0:
                if len(piconversionfactor)<=0:
                    piconversionfactor = 1
                if len(pifilter)>0:
                    tagdf = self.get_point_time_weighted_data_calc_withfilters(pointname,start,end,interval,pifilter,float(piconversionfactor),picalctype)
                else:
                    tagdf = self.get_point_time_weighted_data_df(pointname,start,end,interval,picalctype)
                    #print(tagdf)
            
            
            if self.tc.isValidDf(tagdf):
               # tagdf['point_name_display'] = pointdisplay
                #tagdf['report_column'] =reportcolumn
                #tagdf['date'] = tagdf.index
                #date is the innitial index
                #tagdf.reset_index(inplace=True)
                columns.append([reportcolumn,pointdisplay,exceltab])
                tagdf.rename(columns={"value": pointdisplay},inplace=True)
                #print(tagdf.dtypes)
                #print(tagdf.head())
                #dfs.append(tagdf)
                if retdf is None:
                    retdf = tagdf
                else:
                    #for index to merge timestamps returned from pi need to be the same such as by using AFTimestampCalculation.MostRecentTime
                    retdf =pd.merge(retdf, tagdf, left_index=True, right_index=True)
            
            else:
                self.tc.debug('Failed to get data for tag {pointname}'.format(pointname=pointname))
                warnings.append(['Failed to get data for pi point' ,str(configpoint)])
        
        if self.tc.isValidDf(retdf):

            #print('convert index to column date')
            #print(columns)
            #retdf.loc['Total']= retdf.sum(numeric_only=True, axis=0)

          #  for configpoint in self.settings.calulated_column_map:
          #      columns.append([configpoint['report_column'],configpoint['column_dispaly']])
            
            
            
            columns = (sorted(columns, key=lambda x: x[0], reverse=False))
            colorder = [row[1] for row in columns]
            retdf['DATE'] = retdf.index
            retdf = retdf[colorder]
        
        return [retdf,warnings]