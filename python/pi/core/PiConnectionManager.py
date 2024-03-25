# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 08:58:53 2023

@author: UA58436
"""
from pi.core.settings import MultiDomainPiSettings
from teppwcore import teppwcore
from tepcore import tepcore
from tepcorepidata import tepcorepidata
class PiConnectionManager:
    def __init__(self,debug ,domain='dev',osisystem='ems',config='piconnectionmanager.json',usepi=True):
        '''
        

        Parameters
        ----------
        debug : TYPE
            DESCRIPTION.
        domain : TYPE, optional
            DESCRIPTION. The default is 'dev'.
        osisystem : TYPE, optional
            DESCRIPTION. The default is 'ems'.
        config : TYPE, optional
            DESCRIPTION. The default is 'piconnectionmanager.json'.
        usepi : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        None.

        '''
        self.warnings = []
        self.dbinfo = teppwcore(domain)
        self.tc = tepcore(debug)
        self.osisystem = osisystem
        self.domain = domain
        self.pidatacorp = None
        self.pidataems = None
        self.debug = debug
        self.settings = MultiDomainPiSettings(debug,domain,osisystem,config)
   
            
            
    def ConnectCorpPI(self):
        '''
        

        Returns
        -------
        bool
            DESCRIPTION.

        '''
        if self.pidatacorp is None:
            for piserver in self.settings.piservers:
                if piserver['type'] =='corp' and piserver['domain'] ==self.domain:
                    self.pidatacorp =tepcorepidata(piserver['name'],self.debug)
                    if self.pidatacorp.GetPiServer() is None:
                        self.settings.tc.debug('Pi data is none')
                        self.pidatacorp = None

            if self.pidatacorp is None:
                self.warnings.append(['unable to connect to pi database tried' + str(self.settings.piservers),'Failed'])  
            return True
        else:
            return True
        return False
            
    def ConnectCorpPIAF(self):
        '''
        

        Returns
        -------
        bool
            DESCRIPTION.

        '''
        if  self.piafdbcorp is None:
            for piserver in self.settings.piafservers:
                if piserver['type'] =='corp':
                    self.piafdbcorp =tepcorepidata(piserver['afserver'],self.debug,True,piserver['afdb'])
                    print(self.piafdbcorp)
                    if self.piafdbcorp.GetPiServer() is None:
                        self.settings.tc.debug('Pi data is none')
                        self.piafdbcorp = None

            if self.piafdbcorp is None:
                self.warnings.append(['unable to connect to pi af database tried' + str(self.settings.piafservers),'Failed'])  
            return True
        else:
            return True
        return False        
        
    def ConnectEMSSECPI(self):
        '''
        

        Returns
        -------
        bool
            DESCRIPTION.

        '''
        if self.pidataems is None:
             for piserver in self.settings.piservers:
                if piserver['type'] =='ems'  and piserver['domain'] ==self.domain:
                    self.pidataems =tepcorepidata(piserver['name'],self.debug)
                    if self.pidataems.GetPiServer() is None:
                        self.tc.debug('Pi data is none')
                        self.pidataems = None

             if self.pidataems is None:
                self.warnings.append(['unable to connect to pi database tried' + str(self.settings.piservers),'Failed'])  
             return True
        else:
            return True
        return False
    
    
    
    def GetCorpPiConnection(self):
        '''
        

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        if self.pidatacorp is None:
            self.ConnectCorpPI()
            return self.pidatacorp
        else:
            return self.pidatacorp
    def GetEmsPiConnection(self):
        '''
        

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        if self.pidataems is None:
            self.ConnectEMSSECPI()
            return self.pidataems
        else:
            return self.pidataems
            
        