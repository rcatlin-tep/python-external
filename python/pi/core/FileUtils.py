# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 12:27:34 2022

@author: UA58436
"""

import os
from pi.filetransfer.tepsftp import tepsftp 
from teppwcore import teppwcore
from pi.filetransfer.tepinsecureftp import tepinsecureftp
from datetime import datetime,timedelta
class FileUtils:
    def __init__(self,settings=None):
        self.settings = settings
        self.dbinfo = teppwcore(self.settings.domain)
        self.warnings = []
    
    
    def UploadReportFromSettingconfig(self,reportsources,configitem):
        self.settings.tc.debug('UploadReportFromSettingconfig')
        print(configitem)
        print(reportsources)
            #today = date.today()
            #exceldate = today.strftime("%m%d%Y")
            
        for destdir in configitem:
            for reportsource in reportsources:
                filename = None
                if len(destdir['filefilter'])>1:
                    print('check filter')
                    if destdir['filefilter'] not in reportsource :
                        reportsource = None
                if reportsource is not None:
                    
                    hxdir = destdir['location']
                    if destdir['renamefileto']:
                        if len(destdir['renamefileto']) > 0:
                            filename = os.path.join(hxdir,destdir['renamefileto'])
                    if filename is None:
                        filename = os.path.join(hxdir,os.path.basename(reportsource))
                    
                    if destdir['isnetworkdrive']:
                        username = self.dbinfo.GetUsername(destdir['networkuser'])
                        pw = self.dbinfo.GetPwd(destdir['networkuser'])
                        self.settings.tc.unmountNetworkDrive(destdir['networkdrive'])
                        self.settings.tc.mountNetworkDrive( destdir['networklocation'], username, pw,destdir['networkdrive'])
                        
                        self.settings.tc.CopyFile(reportsource, filename)
                        self.settings.tc.unmountNetworkDrive(destdir['networkdrive'])
                    elif destdir['issftp']:
                        print('sending to sftp')
                        
                        ft = tepsftp(self.settings.domain,destdir['networkuser']) 
                        files = ft.SendFile(reportsource,filename)
                        if not files:
                            self.warnings.append(['Failed to upload {reportsource} csv file'.format(reportsource=reportsource) + filename ,destdir['networkuser']])
                    else:
                        self.settings.tc.CopyFile(reportsource, filename)

        
        return True
    
    
    def HandlFileTransfers(self,sourcesettings,tempdir):
        self.settings.tc.debug('HandleFileTransfers')
        for sourcedirsetting in sourcesettings:
            #only send matching domain configurations
            if sourcedirsetting['domain'] == self.domain:
                destinations = sourcedirsetting['destinations']
                if sourcedirsetting['isnetworkdrive']:
                    hxdir = sourcedirsetting['location']
                    username = self.dbinfo.GetUsername(sourcedirsetting['networkuser'])
                    pw = self.dbinfo.GetPwd(sourcedirsetting['networkuser'])
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                    self.settings.tc.mountNetworkDrive( sourcedirsetting['networklocation'], username, pw,sourcedirsetting['networkdrive'])
                    filename = os.path.join(hxdir,os.path.basename(sourcedirsetting["filefilter"]))
                    self.UploadReportFromSettingconfig([filename],destinations)
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                
                elif sourcedirsetting['issftp']:
                    tempfilelist = []
                    self.settings.tc.debug('Retreiving from sftp')
                    hxdir = sourcedirsetting['location']
                    
                    if len(sourcedirsetting['filefilter']) > 0:
                        filefilter = self.BuildFileFilter(sourcedirsetting['filefilter'])
                    else:
                        filefilter = "*"
                    self.settings.tc.debug('Finding sftp files in {hxdir} {filefilter} to temp dir {temp}'.format(hxdir=hxdir,filefilter=filefilter,temp=self.settings.report_sources_tsr_workingdir))
                    ft = tepsftp(self.domain,sourcedirsetting['networkuser']) 
                    files = ft.GetFiles(hxdir, filefilter ,tempdir)
                    print(files)
                    for resfil in files:
                        print(resfil)
                        if resfil[0]:
                            tempfilelist.append(os.path.join(tempdir,resfil[1]))
                        else:
                            self.warnings.append(['Failed to download file',''])
                    print(tempfilelist)
                    if len(tempfilelist)>0:
                        uploadres = self.UploadReportFromSettingconfig(tempfilelist,destinations)
                        if not uploadres:
                            self.warnings.append(['Failed to download file',str(destinations)])
                else:
                    print('we are not setup to copy from a local drive to another local dirve')
                    hxdir = sourcedirsetting['location']

                return True
    
    def HandleFileTransfersInConfig(self,checkdate=None):
        self.settings.tc.debug('HandleFileTransfers')
        for sourcedirsetting in self.settings.report_sources:
            #only send matching domain configurations
            if sourcedirsetting['domain'] == self.domain:
                destinations = sourcedirsetting['destinations']
                if sourcedirsetting['isnetworkdrive']:
                    hxdir = sourcedirsetting['location']
                    username = self.dbinfo.GetUsername(sourcedirsetting['networkuser'])
                    pw = self.dbinfo.GetPwd(sourcedirsetting['networkuser'])
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                    self.settings.tc.mountNetworkDrive( sourcedirsetting['networklocation'], username, pw,sourcedirsetting['networkdrive'])
                    filename = os.path.join(hxdir,os.path.basename(sourcedirsetting["filefilter"]))
                    self.UploadReportFromSettingconfig([filename],destinations)
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                
                elif sourcedirsetting['issftp']:
                    tempfilelist = []
                    self.settings.tc.debug('Retreiving from sftp')
                    hxdir = sourcedirsetting['location']
                    
                    if len(sourcedirsetting['filefilter']) > 0:
                        filefilter = self.BuildFileFilter(sourcedirsetting['filefilter'])
                    else:
                        filefilter = "*"
                    self.settings.tc.debug('Finding sftp files in {hxdir} {filefilter} to temp dir {temp}'.format(hxdir=hxdir,filefilter=filefilter,temp=self.settings.report_sources_tsr_workingdir))
                    ft = tepsftp(self.domain,sourcedirsetting['networkuser']) 
                    files = ft.GetFiles(hxdir, filefilter ,self.settings.report_sources_tsr_workingdir)
                    print(files)
                    for resfil in files:
                        print(resfil)
                        if resfil[0]:
                            tempfilelist.append(os.path.join(self.settings.report_sources_tsr_workingdir,resfil[1]))
                        else:
                            self.warnings.append(['Failed to download file',''])
                    print(tempfilelist)
                    if len(tempfilelist)>0:
                        uploadres = self.UploadReportFromSettingconfig(tempfilelist,destinations)
                        if not uploadres:
                            self.warnings.append(['Failed to download file',str(destinations)])
                else:
                    print('we are not setup to copy from a local drive to another local dirve')
                    hxdir = sourcedirsetting['location']

        return True
            
    def BuildFileFilter(self,filterparam,curres = None):
        
        if ',' in filterparam:
            
            for param in filterparam.split(','):
                if curres is None:
                    curres =  self.BuildFileFilter(param)
                else:
                    print('curres is {curres}'.format(curres=curres))
                    curres += self.BuildFileFilter(param,curres)
            
        else:
            #print('working filterparam{filterparam}'.format(filterparam=filterparam))
            if 'timestampsuffix' in filterparam:
                params = filterparam.split(':')
                curres = datetime.now().strftime(params[1])
                return curres
            elif 'priordaysuffix' in filterparam:
                params = filterparam.split(':')
                curres = (datetime.now()-timedelta(days=1)).strftime(params[1])
                return curres
            elif 'filenameprefix' in filterparam:
                params = filterparam.split(':')
                return params[1]
            elif 'filenamesuffix' in filterparam:
                params = filterparam.split(':')
                return params[1]
            else:
                return filterparam
                
        return curres       
    def HandleFileTransfersInConfigUsingFTP(self):
        localwarns = []
        results = []
        self.settings.tc.debug('HandleFileTransfers uisng old FTPs as sources')
        for sourcedirsetting in self.settings.report_source_dest_map:
            #only send matching domain configurations
            if sourcedirsetting['domain'] == self.settings.domain:
                destinations = sourcedirsetting['destinations']
                if sourcedirsetting['isnetworkdrive']:
                    hxdir = sourcedirsetting['location']
                    username = self.dbinfo.GetUsername(sourcedirsetting['networkuser'])
                    pw = self.dbinfo.GetPwd(sourcedirsetting['networkuser'])
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                    self.settings.tc.mountNetworkDrive( sourcedirsetting['networklocation'], username, pw,sourcedirsetting['networkdrive'])
                    filename = os.path.join(hxdir,os.path.basename(sourcedirsetting["filefilter"]))
                    self.UploadReportFromSettingconfig([filename],destinations)
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                
                elif sourcedirsetting['issftp']:
                    tempfilelist = []
                    self.settings.tc.debug('Retreiving from sftp')
                    hxdir = sourcedirsetting['location']
                    
                    if len(sourcedirsetting['filefilter']) > 0:
                        filefilter = self.BuildFileFilter(sourcedirsetting['filefilter'])
                    else:
                        filefilter = "*"
                    self.settings.tc.debug('Finding sftp files in {hxdir} {filefilter} to temp dir {temp}'.format(hxdir=hxdir,filefilter=filefilter,temp=self.settings.report_working_path))
                    #ft = tepsftp(self.domain,sourcedirsetting['networkuser']) 
                    ft = tepinsecureftp(self.settings.domain,sourcedirsetting['networkuser'],None,None,True) 
                    files = ft.GetFiles(hxdir, filefilter ,self.settings.report_working_path)
                    print(files)
                    for resfil in files:
                        print(resfil)
                        if resfil[0]:
                            tempfilelist.append(os.path.join(self.settings.report_working_path,os.path.basename(resfil[1])))
                            results.append([True,tempfilelist])
                        else:
                            localwarns.append(['Failed to download file',''])
                            results.append([False,tempfilelist])
                    
                    if len(tempfilelist)>0:
                        uploadres = self.UploadReportFromSettingconfig(tempfilelist,destinations)
                        results.append([uploadres,destinations])
                        if not uploadres:
                            localwarns.append(['Failed to download file',str(destinations)])
                else:
                    print('we are not setup to copy from a local drive to another local dirve')
                    hxdir = sourcedirsetting['location']

        return results
    
    
    def HandleFileTransfersInSourceDestMap(self):
        localwarns = []
        results = []
        self.settings.tc.debug('HandleFileTransfersInSourceDestMap')
        for sourcedirsetting in self.settings.report_source_dest_map:
            #only send matching domain configurations
            if sourcedirsetting['domain'] == self.settings.domain:
                destinations = sourcedirsetting['destinations']
                if sourcedirsetting['isnetworkdrive']:
                    hxdir = sourcedirsetting['location']
                    username = self.dbinfo.GetUsername(sourcedirsetting['networkuser'])
                    pw = self.dbinfo.GetPwd(sourcedirsetting['networkuser'])
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                    self.settings.tc.mountNetworkDrive( sourcedirsetting['networklocation'], username, pw,sourcedirsetting['networkdrive'])
                    filename = os.path.join(hxdir,os.path.basename(sourcedirsetting["filefilter"]))
                    self.UploadReportFromSettingconfig([filename],destinations)
                    self.settings.tc.unmountNetworkDrive(sourcedirsetting['networkdrive'])
                
                elif sourcedirsetting['issftp']:
                    tempfilelist = []
                    self.settings.tc.debug('Retreiving from sftp')
                    hxdir = sourcedirsetting['location']
                    
                    if len(sourcedirsetting['filefilter']) > 0:
                        filefilter = self.BuildFileFilter(sourcedirsetting['filefilter'])
                    else:
                        filefilter = "*"
                    self.settings.tc.debug('Finding sftp files in {hxdir} {filefilter} to temp dir {temp}'.format(hxdir=hxdir,filefilter=filefilter,temp=self.settings.report_working_path))
                    #ft = tepsftp(self.domain,sourcedirsetting['networkuser']) 
                    ft = tepinsecureftp(self.settings.domain,sourcedirsetting['networkuser'],None,None,True) 
                    files = ft.GetFiles(hxdir, filefilter ,self.settings.report_working_path)
                    print(files)
                    for resfil in files:
                        print(resfil)
                        if resfil[0]:
                            tempfilelist.append(os.path.join(self.settings.report_working_path,os.path.basename(resfil[1])))
                            results.append([True,tempfilelist])
                        else:
                            localwarns.append(['Failed to download file',''])
                            results.append([False,tempfilelist])
                    
                    if len(tempfilelist)>0:
                        uploadres = self.UploadReportFromSettingconfig(tempfilelist,destinations)
                        results.append([uploadres,destinations])
                        if not uploadres:
                            localwarns.append(['Failed to download file',str(destinations)])
                else:
                    print('we are not setup to copy from a local drive to another local dirve')
                    hxdir = sourcedirsetting['location']

        return results