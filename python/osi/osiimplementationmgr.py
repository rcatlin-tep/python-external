# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 07:35:44 2024

@author: UA58436
"""
if __name__ == '__main__':
    from tepinterface import tepinterface as interface
    from tepcore import tepcore
    import socket
    from tepcoredata import tepcoredata
    import argparse
    from teppwcore import teppwcore
    import pandas as pd
    from tepemapcore import tepemapcore

    
    
    parser = argparse.ArgumentParser(description='Run a TEP interface python script')
    parser.add_argument("-p","--path", help="The path to use if needed")
    parser.add_argument("-u","--user", help="The desired database user")
    parser.add_argument("-x","--pwd", help="The desired database pwd")
    parser.add_argument("-d","--db", help="The desired database")
    parser.add_argument("-i","--interface", help="The desired interface (jenkinsfailover/")
    parser.add_argument("-n","--domain", help="The desired domain")
    parser.add_argument("-l","--log", help="The desired log level t/f default f")
    parser.add_argument("-f","--autofail", help="The jenkins interface failover t/f default f")
    parser.add_argument("-a","--admsdomain", help="The Adms Domain")
    parser.add_argument("-c","--checkosionline", help="The Adms Domain")
    parser.add_argument("-j","--jiracomponenet", help="The Jira Componenet to deploy")
    parser.add_argument("-b","--circuitlist", help="The circuits to use for a circut list interface seperate with commas")
    parser.add_argument("-m","--mode", help="The mode to run various things")
    parser.add_argument("-o","--proc", help="An oracle procedure to run for a oraclproc inteface")
    parser.add_argument("-e","--extractortype", help="Maven or Leitmotif default is mavene")
    args = parser.parse_args()
    
    
    
    
    if args.path:
        filein=args.path
        
    if args.user:
        userin=args.user
    
    if args.pwd:
        pwdin=args.pwd
    if args.db:
        dbname=args.db
    
    if args.domain:
        domain=args.domain
    debugind=False
    if args.log:
        if args.log.lower() =='t':
            debugind=True
    managerinterface=None
    if args.interface:
        managerinterface=args.interface
    autofail = False
    if args.autofail:
        if args.autofail.lower()=='t':
            autofail= True
    if args.admsdomain:
        dmsdomain =args.admsdomain
    else:
        dmsdomain='PDS'
    tepcore = tepcore(debugind)
    
    if args.checkosionline:
        if 't' in args.checkosionline:
            useonline=True
        else:
            useonline=False
    else:
        useonline=False
    if args.circuitlist:
        circuitlist = args.circuitlist.split(',')
    else:
        circuitlist=None
        
    if args.jiracomponenet:
        componenet = args.jiracomponenet
    else:
        componenet = 'Python_Interface'
    #tepcore.Debug('Starting interface {interface} for domain {d}'.format(interface=managerinterface, d=domain))
    if args.mode:
        mode=args.mode
    else:
        mode=None
    
    
    if args.proc:
        proc = args.proc
        
    if args.extractortype:
        extractortype = args.extractortype
    else:
        extractortype ='maven'
        
      
    alarminfo = 'ADMS GIS Interface'
    alarm_aor = 10
    alarmclass=469

    def DoGetPoweronAbnormals(domain,mode):
   
        if mode=='export':
            dbinfo = teppwcore(domain)
            server=socket.gethostname()
            #electradata = tepcoredata('ELECTRA',dbinfo.GetPwd('ELECTRA'),dbinfo.GetDSNInKeePass('ELECTRA'))
            podata = tepcoredata('ELECTRA',dbinfo.GetPwd('ELECTRA'),dbinfo.GetDSNInKeePass('ELECTRA'))
            admsgisinterface = interface(domain, server,podata)
            admsgisinterface.GISCoreData =  tepcoredata('ADMS',dbinfo.GetPwd('ADMS'),dbinfo.GetDSNInKeePass('ADMS'))
            tepcore.debug('fetching abnormals')
            results = admsgisinterface.GetPoweronAbnormals()
            tepcore.debug(results[1])
            #print(results[0])
            #'FUSE:T.',gisid,urdid,positionid,desc,statea,stateb,statec
            dfres = pd.DataFrame(data=results[0],columns=['type','gisid','urdid','positionid','desc','statea','stateb','statec'])
            tepcore.debug(dfres.head())
            tepcore.SafeFileDelete('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\' + 'poabnormals.cache')
            dfres.to_csv('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\' + 'poabnormals.cache')
            
            #notfoundids.append(['SWCH:T.',gisid,urdid,positionid,desc])
            dfres1 = pd.DataFrame(data=results[1],columns=['type','gisid','urdid','positionid','desc'])
            dfres1.to_csv('D:\\osi\\osi_cust\\tep\\scripts\\wrkdir\\' + 'poabnormalsmismmatch.cache')
            
        elif   mode=='import':
            emapcore = tepemapcore(domain)
            emapcore.UpdateAbnormalsFromPoweron()
        elif mode=='clearabnormals':
            emapcore = tepemapcore(domain)
            emapcore.ResetAbnormals()
            
            
DoGetPoweronAbnormals(domain,mode)