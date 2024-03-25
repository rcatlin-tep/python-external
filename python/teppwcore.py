import platform
import os
from os import path
from pykeepass import PyKeePass
from tepcore import tepcore
from encryption.AES_TEP import AES_TEP
from datetime import datetime


class teppwcore:
    def __init__(self,domain,keepasskey='ft/tPh0tiGW9G3RIWS7ocA==',keepassdb=False):
        """
        A class for retrieving and managing passwords in a database.

        Parameters
        ----------
        domain : TYPE
            DESCRIPTION.
        keepasskey : TYPE, optional
            DESCRIPTION. The default is 'JNTqltag6qsRfh/LUACy2Q=='.
        keepassdb : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        """
        self.keepasskey=keepasskey
        self.domain=domain
        self.keepassdb= keepassdb
        #try:
        #   self.pythondir = os.environ['TEP_PYTHON']
        #except:
        if os.name == 'nt': 
            if not path.exists('D:\\osi\\osi_cust\\tep\\config'):
                self.pythondir='c:\\osi\\osi_cust\\tep\\config'
            else:
                self.pythondir = 'D:\\osi\\osi_cust\\tep\\config'
        else:
            self.pythondir = '/osi/osi_cust/tep/config'
        self.tc = tepcore(False,'interfacemanager.json') 
        aes = AES_TEP(self.tc.GetConfigItem('corekey'))
        self.kpk = aes.decrypt(keepasskey)
          
        #print(self.pythondir)
        #print(platform.system())
        if platform.system()=='Windows':
        	self.slasher = '\\'
        else:
        	self.slasher = '/'
        if self.keepassdb:
           self.keepassdb=keepassdb 
        else:
            self.keepassdb=self.pythondir + self.slasher +'demo.kdbx'
        #print(self.keepassdb)
        
        self.kp = PyKeePass( self.keepassdb, password=self.kpk)

    def GetPwd(self,lkptitle):

    	group = self.kp .find_groups(name=self.domain, first=True)
    	pw = ''
    	if group:
    		entry = self.kp .find_entries(title=lkptitle, first=True, group=group)
    		if entry:
    			pw= entry.password
    	return pw
	
    def GetDSNInKeePass(self,lkptitle):
    	
    	group = self.kp .find_groups(name=self.domain, first=True)
    	dsn = ''
    	if group:
    		entry = self.kp .find_entries(title=lkptitle, first=True, group=group)
    		if entry:
    			dsn= entry.url
    	return dsn	
	
    def GetUsername(self,lkptitle):

    	group = self.kp .find_groups(name=self.domain, first=True)
    	username = ''
    	if group:
    		entry = self.kp .find_entries(title=lkptitle, first=True, group=group)
    		if entry:
    			username= entry.username
    	return username	
	
    
    def GetNotes(self,lkptitle):
  
    	group = self.kp .find_groups(name=self.domain, first=True)
    	dsn = ''
    	if group:
    		entry = self.kp .find_entries(title=lkptitle, first=True, group=group)
    		if entry:
    			dsn= entry.notes
    	return dsn
    
    
    def GetExpirey(self,lkptitle):
        group = self.kp .find_groups(name=self.domain, first=True)
        dsn = None
        if group:
            entry = self.kp.find_entries(title=lkptitle, first=True, group=group)
            if entry:
                dsn= entry.expires
        return dsn
    
    def GetExperiationDate(self,lkptitle):
        """
        Gets the expiration time from keepass

        Parameters
        ----------
        lkptitle : TYPE
            DESCRIPTION.

        Returns
        -------
        dsn : TYPE
            DESCRIPTION.

        """
        group = self.kp .find_groups(name=self.domain, first=True)
        dsn = None
        if group:
            entry = self.kp .find_entries(title=lkptitle, first=True, group=group)
            if entry:
                dsn= entry.expiry_time
        return dsn
    
    def IsPWCloseToExpiration(self,lkptitle,closeseconds=5*60*60*24):
        #2023-02-01 07:00:00+00:00
        expstring = self.GetExperiationDate(lkptitle)
        print(expstring)
        if expstring is not None:
            numseconds = (expstring.replace(tzinfo=None) -datetime.now()).total_seconds()
            if numseconds <=0:
                return True
            elif closeseconds>=numseconds:
                return True
            
           
            
            return False
        else:
            return False
            