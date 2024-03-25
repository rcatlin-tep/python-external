from tepcorepidata import tepcorepidata
from tepcoredata import tepcoredata
from tepcore import tepcore
from teppwcore import teppwcore



def Iniilize(debug=True,domain='test',CONFIG=None):
    '''
    Initiilze similar to __init__ method but not a class

    Parameters
    ----------
    debug : TYPE, optional
        DESCRIPTION. The default is True.
    domain : TYPE, optional
        DESCRIPTION. The default is 'test'.
    CONFIG : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    bool
        DESCRIPTION script will exit if false, otherwise true

    '''
    
    global conifg
    global tc
    global afdataconn
    global corepidat
    global areaaormaps
    global tepregiontemplate
    global UNSELE
    
    
    tc = tepcore(debug,CONFIG)
    dbinfo = teppwcore(domain)
    tepbreakertemplate = tc.GetConfigItem('tepbreakertemplate')
    tepsubstationtemplate = tc.GetConfigItem('tepsubstationtemplate')
    tepregiontemplate = tc.GetConfigItem('tepregiontemplate')
    outputafserver = tc.GetConfigItem('outputafserver')
    outpiafserver = tc.GetConfigItem('outpiafserver')
    
    tc.debug('outpiafserver {outpiafserver}'.format(outpiafserver=outpiafserver))
    
    
    areas = [ 'Kingman',  'Tucson']
    areaaormaps = tc.GetConfigItem('areaaormaps') 
    tc.debug(areaaormaps)
    #[['Tucson','DISTR','Distribution','DMS','Substation - TEP',tepbreakertemplate],['Tucson','TRANS','Transmission','EMS','Substation - TEP',tepbreakertemplate],['Kingman','KGM','Distribution','DMS', 'Substation - UES','Circuit Breaker-Standerdized UES 3 phase DMS Lookups -EMSSEC']]
    
    dbuser = dbinfo.GetUsername('ITESDWCORP')
    sqldb = dbinfo.GetDSNInKeePass('ITESDWCORP')
    tc.log_info('sql dbuser {dbuser} sqldb {sqldb}'.format(dbuser=dbuser,sqldb=sqldb))
    
    afdataconn = tepcoredata(dbuser,dbinfo.GetPwd('ITESDWCORP'),sqldb,True,False,'PIAFSQL')  
    corepidat = tepcorepidata(piserver=outpiafserver,debug=True,useAf=True,afdb=outputafserver)
    try:
        UNSELE = corepidat.GetAfElement('UNS')
    except Exception as e:
        UNSELE = None
        print(str(e))
    
    if UNSELE is None:
        tc.debug('base element not found exiting complete AF prereq build')
        exit(1)
    else:
        return True

def DoAFCreate():
    '''
    Loops over the config setup in areaaormaps 
    queries for data for the ocnfig
    creates af for the config

    Returns
    -------
    None.

    '''
    for areaaormap in areaaormaps:
        tc.debug('woring areaaormap {areaaormap}'.format(areaaormap=str(areaaormap)))
        distele = None
        
        
        #Compayn Division Tucson/Kingman
        compdiv = corepidat.GetAfElementFromParent(UNSELE,areaaormap['areaname'])
        
       
        if compdiv is None:
            tc.debug('Distribution element not setup exiting')
                #exit(1)
            compdiv = corepidat.CreateAfElementWithTemplate(UNSELE, areaaormap['areaname'], tepregiontemplate)
        if compdiv is None:
            tc.debug('failed')
            exit(1)
            
            
        tc.debug('Working compdiv {compdiv} to find division {div}'.format(compdiv=compdiv.Name,div=areaaormap['areaaorname']))
        #Compayn Division Voltage (Trans / Dist)
        divvolt = corepidat.GetAfElementFromParent(compdiv,areaaormap['areaaorname'])
    
               
        if divvolt is None:
            tc.debug('divvolt element not setup creating')
                        #exit(1)
            divvolt = corepidat.CreateAfElementWithTemplate(compdiv, areaaormap['areaaorname'], tepregiontemplate)
        if divvolt is None:
            tc.debug('failed')
            exit(1)
    
                
        
        tc.debug('found were working {divvolt}'.format(divvolt=divvolt.Name))
        
        
        brkertemplate =areaaormap['breakertemplate']
        subtemplate =areaaormap['parenttemplatename']
        #pi wont support these in names so zzz *** junk wont work An invalid character '*' was used in the name 'zzz**JUNK DRAWER *2'. The following characters are not allowed: *?;{}[]|\`'"
        #Parameter name: name  
        stationquery = '''
    SELECT distinct STATION_ID, STATION_NAME
      FROM  [ITESDW].[dbo].[ADMS_POINT_DESC_TO_TAG_REF]
      WHERE OSI_SYSTEM = '{system}'
      and aor_name ='{aorgroup}'  '''.format(aorgroup = areaaormap['areaaor'],system=areaaormap['areasource'] )
    
        stationequip ='''
          SELECT distinct STATION_ID, STATION_NAME, EQUIP_NAME
          FROM  [ITESDW].[dbo].[ADMS_POINT_DESC_TO_TAG_REF]
          WHERE OSI_SYSTEM = '{system}'
          and aor_name ='{aorgroup}'     '''.format(aorgroup= areaaormap['areaaor'],system=areaaormap['areasource'])
          
          
        stationdata =   afdataconn.run_query_for_df(stationquery)
        stationequipdf = afdataconn.run_query_for_df(stationequip)
        
            
    
        
        
        if tc.isValidDf(stationdata) and tc.isValidDf(stationequipdf) :
            tc.debug(stationdata.head())
            stationlist = stationdata['STATION_NAME'].tolist()
            for station in stationlist:
                tc.debug(station)
                tc.debug('Creating AF Element for Distribution')
                
                childexists =  corepidat.GetAfElementFromParent(divvolt,station.strip())
                if childexists is None:
                    tc.debug('did not find station so we are creating {station} for parent {divvolt} using template {subtemplate}'.format(station=station, subtemplate=subtemplate, divvolt=divvolt.Name))
                
                    parele = corepidat.CreateAfElementWithTemplate(divvolt ,station.strip(), subtemplate)
                    tc.debug(parele)
                else:
                    parele = childexists
                tc.debug(parele.Name)
                stationgroups = stationequipdf.query('STATION_NAME ==@station')
                if tc.isValidDf(stationgroups):
                    tc.debug(stationgroups.head())
                    for eqip in stationgroups['EQUIP_NAME'].tolist() :
                        childexists =  corepidat.GetAfElementFromParent(parele,eqip)
                        if childexists is None:
                            #tc.debug('did not find station so we are creating {station}'.format(station=station))
                            tc.debug('CreateAfElementWithTemplate for {parele} child {eqip}'.format(parele=parele,eqip=eqip ))
                            corepidat.CreateAfElementWithTemplate(parele ,eqip, brkertemplate)
                        else:
                            tc.debug('Element already exists for {parele} child {eqip}'.format(parele=parele,eqip=eqip ))
                        
                        
if __name__ == '__main__':
    import argparse
    import os
    print("Interface is running")
    
    parser = argparse.ArgumentParser(description='Run a TEP interface python script')
    parser.add_argument("-p","--path", help="The path for the CONFIG file")
    parser.add_argument("-u","--user", help="The desired database user")
    parser.add_argument("-x","--pwd", help="The desired database pwd")
    parser.add_argument("-d","--db", help="The desired database")
    parser.add_argument("-i","--interface", help="The desired interface (upcproductiontofile/upcproductiontopi/upcpointsfromcsv")
    parser.add_argument("-n","--domain", help="The desired domain")
    parser.add_argument("-l","--log", help="The desired log level t/f default f")
    parser.add_argument("-f","--autofail", help="The jenkins interface failover t/f default f")
    parser.add_argument("-a","--osisystem", help="The dms system name corp/ems/dms name")
    parser.add_argument("-c","--checkosionline", help="The OSI Domain is online on this server")
    args = parser.parse_args()
    
    
    
    
    if args.path:
        CONFIG=args.path
    else:
        CONFIG = 'C:/Users/UA58436/Documents/GitHub/python-external/python/config/AFCreateFromSCADATEPcorp.json'
        
    if args.user:
        userin=args.user
    
    if args.pwd:
        pwdin=args.pwd
    if args.db:
        dbname=args.db
    
    if args.domain:
        domain=args.domain
    else:
        domain = 'test'
        
    if args.log:
        if args.log =='t':
            debugind=True
        else:
            debugind=False
    else:
        debugind=True
    if args.interface:
        interface = args.interface
    else:
        interface = 'afcreate'
        
    if interface =='afcreate':
        init = Iniilize(debugind,domain,CONFIG)
        print('init {init}'.format(init=init))
        DoAFCreate()
        
