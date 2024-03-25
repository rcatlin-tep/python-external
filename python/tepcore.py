# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 13:29:44 2020

@author: aa58436
"""
import json
import smtplib
import os
import shutil
import sys
from os.path import basename
from os import path
from zipfile import ZipFile
from xml.etree import ElementTree as et
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPResponseException
import glob
import filecmp
import logging
#import logging.handlers
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
class tepcore:
    #GIS RELATED
    IMPORT_CONFIG = '_ImportConfig.xml'
    SUBDIRECTORY = 'xml'
    UTF8 = 'utf-8'

    # Common XML tags
    CONTAINER = 'container'
    GROUP = 'group'
    ID = 'id'
    NAME = 'name'
    CONFIG_DIR_NAME = 'config'
    SSL_DIR_NAME ='ssl'
    PYTHON_DIR_NAME = 'python'
    OSI_CUST_DIR_NAME = 'osi_cust'
    TEP_CORE_CONFIG_FILE = 'interfacemanager.json'
    def __del__(self):
        for handler in self.log.handlers:
            handler.close()
            self.log.removeFilter(handler)
    def __init__(self,debugflg=False, configfile='interfacemanager.json',logfile=None):
        self.debugflg=debugflg
        self.keypasskey='x'
        now = datetime.now()
        logdate = now.strftime("%m%d%Y%H%M%S")
        self.log = None
        self.ssldir = None
        
        if 'D:/osi/osi_cust/tep/config/' in configfile or 'c:/osi/osi_cust/tep/config/' in configfile or 'C:/Users/UA58436/Documents/GitHub/python-external/python' in configfile:
            print('using legacy config directory will be depreicated please remove hard coded configuration directory and supply just a json file argument')
            #using the legacy config dir
            self.configdir = os.path.basename(configfile)
        else:
            self.configdir = self.GetConfigDir()
        self.configfile = os.path.join(self.configdir,configfile)
        self.log = logging.getLogger("tepcore")

        if os.name == 'nt':
            #print(logfile)
            if logfile is None:
                if not path.exists('D:/osi'):
                    self.logfile = 'C:/osi/monarch/log/adms_interface.log'
                else:
                    self.logfile = 'D:/osi/monarch/log/adms_interface.log'
            else:
                self.logfile = logfile.format(logdate=logdate)
            if not path.exists(self.configfile.strip()):
                print('no config file at{configfile}'.format(configfile=self.configfile))

            #need to move stuff like this below logger init self.log_info('curlogfilesetting {logfile}'.format(logfile=logfile))   
            try:
                with open(self.configfile) as f:
                    self.config = json.load(f)
                    if debugflg:
                        print('Config loaded from {configfile}'.format(configfile=self.configfile))
            except Exception as e:
                    print('Failed to laod config file')
                    self.config = None
                    print(str(e))
                    
            try:
                self.ssldir = self.GetSSLDir()
            except Exception as e:
                    print('Failed to laod ssl dir')
                    self.ssldir = None
                    print(str(e))
        else:
            
            self.logfile = '/osi/osi_cust/tep/log/adms.log'
            if not path.exists(self.configfile.strip()):
                print('no config file at{configfile}'.format(configfile=configfile))
            try:
                with open(self.configfile) as f:
                    self.config = json.load(f)
                    if debugflg:
                        print('Config loaded from {configfile}'.format(configfile=configfile))
            except Exception as e:
                    print('Failed to laod config file')
                    self.config = None
                    print(str(e))
            try:
                self.ssldir = self.GetSSLDir()
            except Exception as e:
                    print('Failed to laod ssl dir')
                    self.ssldir = None
                    print(str(e))
        logging.basicConfig(
           
            level=logging.INFO,
            format="%(asctime)s:%(levelname)s:%(message)s",
            datefmt="%Y-%m-%d %I:%M:%S%p", 
            handlers=[logging.StreamHandler()]
        )
        logging.getLogger().setLevel(logging.INFO)
        #logging.debug("Log with debug-level severity")
        #logging.info("Log with info-level severity")
        #logging.warning("Log with warning-level severity")
        #logging.error("Log with error-level severity")
        #logging.critical("Log with error-level severity")
        #logger = logging.getLogger(__name__)

        # Custom logger
        self.logger = logging.getLogger("tepcorelogger")
        #self.logger.addHandler(RotatingFileHandler(self.logfile, maxBytes=52000, backupCount=10))
            

    def GetConfigDir(self,pythondir=None):
        '''
        Returns the config dir after identifying the python dir. Starts with cirectory from where python program is run.

        Parameters
        ----------
        istestdir : BOOLEAN, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        BASE_DIR : TYPE
            DESCRIPTION.

        '''
        if pythondir is None:
            BASE_DIR = Path(__file__).resolve().parent
        else:
            BASE_DIR = Path(pythondir).parent
            #print(BASE_DIR)
        
        if BASE_DIR.name == self.PYTHON_DIR_NAME:
            return os.path.join(BASE_DIR,self.CONFIG_DIR_NAME)
        else:
            if BASE_DIR.parent.name is None or BASE_DIR.parent.name=="":
                self.debug('Reached end of directory input no python directory found')
                return None
            return self.GetConfigDir(BASE_DIR)
    def GetSSLDir(self,pythondir=None):
        '''
        Returns the config dir after identifying the python dir. Starts with cirectory from where python program is run.

        Parameters
        ----------
        istestdir : BOOLEAN, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        BASE_DIR : TYPE
            DESCRIPTION.

        '''
        if pythondir is None:
            BASE_DIR = Path(__file__).resolve().parent
        else:
            BASE_DIR = Path(pythondir).parent
            #print(BASE_DIR)
        
        if BASE_DIR.name == self.PYTHON_DIR_NAME:
            return os.path.join(BASE_DIR,self.SSL_DIR_NAME)
        else:
            if BASE_DIR.parent.name is None or BASE_DIR.parent.name=="":
                self.debug('Reached end of directory input no python directory found')
                return None
            return self.GetConfigDir(BASE_DIR)
    def GetTestResourcesDir(self,pythondir=None):
        '''
        Retruns python test resource dir for unit testing with test resources.

        Parameters
        ----------
        pythondir : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        if pythondir is None:
            BASE_DIR = Path(__file__).resolve().parent
        else:
            BASE_DIR = Path(pythondir).parent
            #print(BASE_DIR)
        
        if BASE_DIR.name == self.PYTHON_DIR_NAME:
            return os.path.join(BASE_DIR,'tests','resources')
        else:
            if BASE_DIR.parent.name is None or BASE_DIR.parent.name=="":
                self.debug('Reached end of directory input no python directory found')
                return None
            return self.GetTestResourcesDir(BASE_DIR)
            

    def Debug(self,msg,tofile=False):
        if tofile:
            try:
                with open(self.logfile, 'a+') as f:
                    f.write(str(msg) + "\n")
            except Exception as e:
                print('Failed to write to log file')
                print(str(e))
        else:
            print(msg)
    def debug(self,msg,tofile=False):
        self.Debug(msg,tofile=False)
        
    def log_info(self,msg):

        self.logger.info(msg)
        return True
    def log_error(self,msg):

        self.logger.error(msg)
        return True
    def log_warning(self,msg):

        self.logger.warning(msg)
        return True
    
    def log_critical(self,msg):

        self.logger.critical(msg)
        return True
    def log_debug(self,msg):

        self.logger.debug(msg)
        return True
    def GetConfigItem(self,key):
        #self.Debug(self.config)
        configitem = None 
        if self.config is not None:
            try:
                configitem = self.config[key]
            except Exception as e:
                print(str(e))
        return configitem
    
    def SplitArray(self,l,n):
        """ SplitArray splits a list into chunks
    		- l       - The List
    		- n       - size of chunks  """
        for i in range(0, len(l), n):  
            yield l[i:i + n] 
            
    def SendMail(self,recipients,message,messages=None,Suject='Python Interface Mesage'):
        """
        

        Parameters
        ----------
        recipients : TYPE
            DESCRIPTION.
        message : TYPE
            DESCRIPTION.
        messages : TYPE, optional
            DESCRIPTION. The default is None.
        Suject : TYPE, optional
            DESCRIPTION. The default is 'Python Interface Mesage'.

        Returns
        -------
        bool
            DESCRIPTION.

        """
        try:
            
            
            sender = self.GetConfigItem('emailsender')
            if sender is None:
                sender = 'ADMSITES@tep.com'
            defaultrecipients=''
            server=self.GetConfigItem('emailserver')
            #print('Mail server {server}'.format(server=server))
            
            outer = MIMEMultipart()
            outer['Subject'] = Suject
            outer['To'] = recipients#COMMASPACE.join(recipients)
            outer['From'] = sender
            outer.preamble = 'You will not see this in a MIME-aware mail reader.\n' 
            
            
            if messages is None:
                part1 = MIMEText(message, 'plain')
            else:
                for msg, msgtype in messages:
                    #print(msg)
                    #print(msgtype)
                    part1 = MIMEText(msg, msgtype)
                    outer.attach(part1)
            s = smtplib.SMTP(server)
            recipients = recipients # + defaultrecipients
            self.Debug('starting sending to {recipients}'.format(recipients=recipients))
 
            
            status = s.sendmail(sender, recipients.split(","), outer.as_string())
            self.Debug(status)
            s.quit()
        except SMTPResponseException as e:
            error_code = e.smtp_code
            error_message = e.smtp_error 
            self.Debug('Email exception code {code}, {message}'.fomrat(code = error_code, message=error_message))
            return False
        except ConnectionRefusedError as e:
            
            error_code = e.smtp_code
            error_message = e.smtp_error 
            self.Debug('Email server connection not setup code {code}, {message}'.fomrat(code = error_code, message=error_message))
            return False
        return True
    
    
    def SendMailAttachement(self,recipients,Suject='Python Interface Mesage',attchfilename=None):
        """
        

        Parameters
        ----------
        recipients : TYPE
            DESCRIPTION.
        message : TYPE
            DESCRIPTION.
        messages : TYPE, optional
            DESCRIPTION. The default is None.
        Suject : TYPE, optional
            DESCRIPTION. The default is 'Python Interface Mesage'.

        Returns
        -------
        bool
            DESCRIPTION.

        """
        try:
            
            
            sender = self.GetConfigItem('emailsender')
            if sender is None:
                sender = 'ADMSITES@tep.com'
           # defaultrecipients=''
            server=self.GetConfigItem('emailserver')
            #print('Mail server {server}'.format(server=server))
            
            outer = MIMEMultipart()
            outer['Subject'] = Suject
            outer['To'] = recipients#COMMASPACE.join(recipients)
            outer['From'] = sender
            outer.preamble = 'You will not see this in a MIME-aware mail reader.\n' 
            
            
            
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attchfilename, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="{filname}"'.format(filname=os.path.basename(attchfilename)))
            outer.attach(part)

            s = smtplib.SMTP(server)
            #recipients = recipients + defaultrecipients
            self.Debug('starting sending to {recipients}'.format(recipients=str(recipients)))
 
            
            status = s.sendmail(sender, recipients.split(","), outer.as_string())
            self.Debug(status)
            s.quit()
        except SMTPResponseException as e:
            error_code = e.smtp_code
            error_message = e.smtp_error 
            self.Debug('Email exception code {code}, {message}'.fomrat(code = error_code, message=error_message))
            return False
        except ConnectionRefusedError as e:
            
            error_code = e.smtp_code
            error_message = e.smtp_error 
            self.Debug('Email server connection not setup code {code}, {message}'.fomrat(code = error_code, message=error_message))
            return False
        return True
    
    def merge_xml_files(self,src, dest):
        """
        Merges the xml from the src path into the dest path
        :param src: File path, source of extra xml
        :param dest: File path, will contain the all elements at the end
        """
        src_xml = None
        dest_xml = None
        try:
            src_xml = et.parse(src)
            dest_xml = et.parse(dest)
        except (OSError, et.ParseError) as e:
            self.debug('Cannot merge files {} and {}: {}'.format(src, dest, e))
            return False
    
        if src_xml.getroot().get('type') != dest_xml.getroot().get('type'):
            self.debug('Cannot merge {}, incompatible times'.format(src))
    
        dest_ids = [e.get('id') for e in dest_xml.findall('./groups/group/element')]
        dest_groups_elem = dest_xml.find('./groups')
        for g in src_xml.findall('./groups/group'):
            dest_group = dest_groups_elem.find('./group[@id=\'{}\']'
                                               .format(g.get(self.ID)))
            if dest_group is None:
                dest_groups_elem.append(g)
            else:
                for e in g:
                    if e.get(self.ID) not in dest_ids:
                        dest_group.append(e)
    
        self.debug(dest_xml.getroot())
        dest_xml.write(dest, encoding=self.UTF8, xml_declaration=True)
    
    def move_xml_files(self,xml_path, output_dir):
        """
        Manages the moving of files from the input to the output paths. If a file
        already exists, they will be merged.
        :param xml_path: Directory containing all the input xml files
        :param output_dir: Path where files will be placed
        """
        for f in os.listdir(xml_path):
            p = os.path.join(xml_path, f)
            if not os.path.isfile(p): #or self.IMPORT_CONFIG == f:
                continue
    
            dest = os.path.join(output_dir, f)
            if os.path.isfile(dest):
                self.merge_xml_files(p, dest)
            else:
                shutil.copy2(p, dest)
        return os.path.isdir(output_dir)
    
    def SafeDeleteDirNw(self, input_dir):
        """
        Deletes the directory if it exsts
        """
        dirpath = os.path.join(input_dir)
        try:
            shutil.rmtree(dirpath)
            return True
        except:
            return False
        
        
    def SafeDeleteDir(self, input_dir):
        """
        Deletes the directory if it exsts
        """
        dirpath = os.path.join(input_dir)
        if os.path.exists(dirpath) and os.path.isdir(dirpath):
            shutil.rmtree(dirpath)
    def SafeFileDelete(self,infile):
        if os.path.exists(infile):
            os.remove(infile)
        else:
            pass
        return True
    def MakeDir(self,input_dir,delifexist=False):
        """
        Makes the directory if it doesmt exist
        :param 
        """
        dirpath = os.path.join(input_dir)
        if not os.path.exists(dirpath) and not os.path.isdir(dirpath):
            #print('making dir {dirpath}'.fomrat(dirpath=dirpath))
            os.mkdir(dirpath) 
        elif delifexist:
            self.SafeDeleteDir(input_dir)
            self.MakeDir(input_dir)
        return os.path.isdir(dirpath)
            
    def merge_xml_dirs(self, dirs,output_dir):
        errcnt=0
        errorind= False
        for curdir in dirs:
            res = self.move_xml_files(curdir,output_dir)
            if not res:
                self.debug('Failed to move {curdir}'.format(curdir=curdir))
                errcnt+=1
            else:
                self.debug('Successfully moved {curdir}'.format(curdir=curdir))
        if errcnt == 0:
            errorind = True
        return errorind
    def zipdir(self,path, outfile):
        """"ziph is zipfile handle"""
        try:
            shutil.make_archive(outfile, 'zip', path)
            return True
        except Exception as e:
            self.debug('Excption unable to zip' + str(e))
            return False
    def zipfile(self,archive,file,removefile=False):
        ziper = ZipFile(archive, 'w')
        ziper.write(file,basename(file))
        ziper.close()
        if removefile:
            self.debug('Deleteing file {file}'.format(file=file))
            self.SafeFileDelete(file)
    
    def unzipdir(self,zipfile,outputdir):
        with ZipFile(zipfile, 'r') as zip_ref:
            zip_ref.extractall(outputdir)
        return True
            
            
    def isValidDf(self,df):
        try:
            if df.empty:
                return False
            else :
                return True
        except Exception as e:
            self.debug(str(e))
            return False
        
        
    def CopyFiles(self,src,dest):
        src_files = os.listdir(src)
        for file_name in src_files:
            full_file_name = os.path.join(src, file_name)
            #self.debug(file_name)
            if os.path.isfile(full_file_name):
                try:
                    shutil.copy2(full_file_name,dest)
                except Exception as e:
                    self.debug(str(e))
                    return False
        return True
    
    
    def CopyDirRecursive(self,src,dest):
        return shutil.copytree(src, dest) 
    def CopyDirs(self,srcs,dest):
        noerrors = True
        for src in srcs:
            res = self.CopyFiles(src,dest)
            if not res:
                noerrors = False
        return noerrors
            
    #seems to work on networked dirve    
    def MakeNetworkDirs(self,direc):
        try:
            os.makedirs(direc,exist_ok = True)
        except Exception as e:
            print(str(e))
        return True
            
        
        
    def CopyFileAndMakeDir(self,filename,destination):
        direc = os.path.dirname(os.path.abspath(destination))
        print(filename)
        print(destination)
        print(direc)
        os.makedirs(direc,exist_ok = True)
        self.CopyFile(filename,destination)
        return True
        
    def CopyFile(self,filename,destination):
        try:
              self.debug('copying {filename} to {destination}'.format(filename=filename,destination=destination))
              shutil.copyfile(filename,destination)
        except Exception as e:
            self.debug(str(e))
            return False
        return True
        
    def CopyandClearFiles(self,src,dest):
        #self.SafeDeleteDir(dest)
        self.MakeDir(dest,True)
        self.CopyFiles(src,dest)
        return True
    def GetFileList(self,direcotry,filename='*',filetype='*'):
        return glob.glob("{direcotry}/*{filename}*.{filetype}".format(direcotry=direcotry,filename=filename,filetype=filetype))
    
    def DfToHtmlReport(self,df,outfile):
        html = df.to_html()
        text_file = open(outfile, "w")
        text_file.write(html)
        text_file.close()
        
    def FileChanged(self,working,source):
        if not os.path.exists(working) or not os.path.exists(source):
            return True
        if filecmp.cmp(working, source, shallow=True):
            return False
        else:
            return True
    def FileExist(self,infile):
        '''
        For compatibilyt with opython .

        Parameters
        ----------
        infile : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        '''
        return os.path.exists(infile)    
        
    def FileExists(self,infile):
        return os.path.exists(infile)
    
    def GetPathSep(self):
        return os.path.sep
    

    def safe_open_w(self,path):
        ''' Open "path" for writing, creating any parent directories as needed.
        '''
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return open(path, 'wb')
    
    def isFile(self,file):
        return os.path.isfile(file)
    def isDir(self,directory):
        """
        

        Parameters
        ----------
        directory : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            Checks if directory exits.

        """
        return os.path.isdir(directory)
    
    def pathExists(self,filepath):
         return path.exists(filepath)
    
    def unmountNetworkDrive(self,driveLetter=None):
        """
        

        Parameters
        ----------
        driveLetter : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        bool
            DESCRIPTION.

        """
    	#unmount the mapped Drive
        self.debug('NET USE ' + driveLetter +': /DELETE')
        try:
            os.system(r'NET USE ' + driveLetter + ': /DELETE')
            return True
        except Exception as e:
            self.debug('Not able to unmount' + str(e))
            return False
        

    def mountNetworkDrive(self, path, user, pw,driveLetter=None):
        """
    

        Parameters
        ----------
        path : String
            A path string such as //10.128.11.205/corpdata.
        user : String
            The user name to mount.
        pw : String
            Password.
        driveLetter : String, optional
            DESCRIPTION. The default is None. The drivelettter to use, this is not neccisary in many cases you can just mount the path

        Returns
        -------
        BOOLEAN
            Returns false if there is an exception or the path doesnt exist after attempting to mount.

        """
        path = path.rstrip('\\')
        try:
            if driveLetter is None:
                self.debug('net use "' +path+ '" /USER:'+ user +' [password] ')
                os.system(r'net use "' +path+ '" /USER:'+ user +' '+ pw +'')
                
            else:
                self.debug('net use '+driveLetter+': "' +path+ '" /USER:'+ user +' [password] ' )
                os.system(r'net use '+driveLetter+': "' +path+ '" /USER:'+ user +' '+ pw +'')
            return os.path.exists(path)
        except Exception as e:
            self.debug('Not able to unmount path: ' + str(path) + str(e))
            return False