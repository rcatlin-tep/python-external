# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 13:22:10 2022

@author: UA58436
"""


from tepcore import tepcore
class settings:
    def __init__(self,debug,domain='dev',osisystem='ems',config='settings.json'):

        self.tc = tepcore(debug,config)
        self.domain=domain
        self.debug=debug
        self.piservers = self.tc.GetConfigItem('piservers')
        self.piafservers = self.tc.GetConfigItem('piafservers')
        self.recevingemaillist = self.tc.GetConfigItem('recevingemaillist')
        self.warningemailuser = self.tc.GetConfigItem('warningemailuser')
        self.prunehxdays = self.tc.GetConfigItem('prunehxdays')
        self.report_working_path = self.tc.GetConfigItem('report_working_path')
        self.pi_point_map = self.tc.GetConfigItem('pi_point_map')
        self.report_source_dest_map = self.tc.GetConfigItem('report_source_dest_map')
        self.calulated_column_map =  self.tc.GetConfigItem('calulated_column_map')
        self.additional_column_map =  self.tc.GetConfigItem('additional_column_map')
        self.effluent_ccf_cost =  self.tc.GetConfigItem('effluent_ccf_cost')
        self.effluent_admin_fees =  self.tc.GetConfigItem('effluent_admin_fees')
        
        
        

class MultiDomainPiSettings:
    def __init__(self,debug,domain='dev',osisystem='ems',config='backfillsettings.json'):

        self.tc = tepcore(debug,config)
        self.domain=domain
        self.debug=debug
        self.piservers = self.tc.GetConfigItem('piservers')
        self.piafservers = self.tc.GetConfigItem('piafservers')
        self.recevingemaillist = self.tc.GetConfigItem('recevingemaillist')
        self.warningemailuser = self.tc.GetConfigItem('warningemailuser')
        self.cachedir = self.tc.GetConfigItem('cachedir')
        self.pi_point_map = self.tc.GetConfigItem('pi_point_map')  
        
class PiInterfaceMonitorSettings:
    def __init__(self,debug,domain='dev',osisystem='ems',config='backfillsettings.json'):

        self.tc = tepcore(debug,config)
        self.domain=domain
        self.debug=debug
        self.piservers = self.tc.GetConfigItem('piservers')
        self.piafservers = self.tc.GetConfigItem('piafservers')
        self.recevingemaillist = self.tc.GetConfigItem('recevingemaillist')
        self.warningemailuser = self.tc.GetConfigItem('warningemailuser')
        self.cachedir = self.tc.GetConfigItem('cachedir')
        self.piinterfacesetags = self.tc.GetConfigItem('piinterfacesetags')  
        self.piinteractivesessionuser = self.tc.GetConfigItem('piinteractivesessionuser') 
        self.piinterfaceservers = self.tc.GetConfigItem('piinterfaceservers') 
        self.p4emailuser = self.tc.GetConfigItem('p4emailuser')
        self.p1emailuser = self.tc.GetConfigItem('p1emailuser')