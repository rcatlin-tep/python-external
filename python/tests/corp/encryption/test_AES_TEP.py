# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 08:12:08 2022

@author: UA58436
"""



from encryption.AES_TEP import AES_TEP
from tepcore import tepcore


tc = tepcore(True,'C:/Users/AA58436/Documents/GitHub/python-external/python/config/interfacemanager.json')
aestep = AES_TEP(tc.GetConfigItem("corekey"))

def test_encypte():
    string = 'taco'
    encypted = aestep.encrypt(string)
    print(encypted)
    assert encypted == 'ft/tPh0tiGW9G3RIWS7ocA=='
    

    
def test_decrypte():
    
    encypted = aestep.decrypt('ft/tPh0tiGW9G3RIWS7ocA==')
    print(encypted)
    assert encypted == 'taco'

test_encypte()
test_decrypte()