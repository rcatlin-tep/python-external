# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 12:28:22 2022

@author: UA58436
"""


from tepcore import tepcore
import pandas as pd
class EmailUtils:
    def __init__(self,debug=True,domain='dev',osissystem='ems'):
        self.tc = tepcore(True)
        self.domain = debug
        self.osissystem = osissystem
    
    
    def HandleWarnings(self,emailto,emailsubject,warnings):
        
        """
        Emails any errors from the eim 216 interface
    
        Returns
        -------
        bool
            DESCRIPTION.
    
        """
        
        msglist1=[]
        emailto = emailto
        emailsubject ='Generation Renwable Daily  Interfaace Warnings from domain {domain}'.format(domain=self.domain)
        emailbody='The following warnings have been triggered ' + self.osissystem
        if len(warnings)>0:
            #need to use tepcore of normal interface manager for email settings
            
            dferrors = pd.DataFrame(warnings,columns=['warning','details'])
            msglist1.append([dferrors.to_html(),'html']) 
            tepcore.debug('We have generation renewable report interface errors !')
            self.tc.SendMail(emailto,emailsubject,msglist1,emailbody )
        return True  
    
