# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 14:51:13 2022

@author: UA58436
"""








from datetime import  datetime, timedelta

numhrsinday = 24
numsecondsinday=60*60*24
numsecondsinhour = 60*60

def GetNumDaysPriorMonth():
        today = datetime.now()
        start = today.replace(day=1,minute=0, hour=0, second=0, microsecond=0)
              
        last_month =( start - timedelta(days=1)).replace(day=1,minute=0, hour=0, second=0, microsecond=0)
        
        timinseconds = (start -last_month).total_seconds()
        return timinseconds/numsecondsinday


def GetNumHrsInPriorMonth():

    return GetNumDaysPriorMonth() * numhrsinday
        


def GetNumHrsInMonth(indatedttm):
    #first day of input date
    firstdayofcurmonth = indatedttm.replace(day=1,minute=0, hour=0, second=0, microsecond=0)
    #get first day of next month 00:00
    next_month = (indatedttm.replace(day=28) + timedelta(days=4)).replace(day=1,minute=0, hour=0, second=0, microsecond=0)
    return ((  next_month- firstdayofcurmonth).total_seconds()/numsecondsinday) * numhrsinday
    

def GetNumHoursBetweenDates(end,start):
    diff = end - start
    # Get interval between two timstamps in hours
    diff_in_hours = diff.total_seconds() / numsecondsinhour
    return diff_in_hours

def GetFullDateForFileStr():
    startdt = datetime.now()
    return startdt.strftime('%Y%m%d_%H%M%S')

def GetFileCompativleTimeStamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")
