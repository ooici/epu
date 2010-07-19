import xmlrpclib,os    
from SimpleXMLRPCServer import SimpleXMLRPCServer
try:
    from pysnmp.entity.rfc3413.oneliner import cmdgen
    PysnmpImported = True
except ImportError:
    PysnmpImported = False


### REQUIREMENTS:
###
### This module requires pyasn1 and pysnmp
###


class HostStatus:
    """
    Represents the status of the local host as retrieved using
    SNMP and RFC1213 and RFC2790 SNMP MIB definitions.
    """
    def __init__(self, host, port, agentName, communityName, timeout=1.5, retries=3):
        self.timeout = timeout
        self.retries = retries
        self.reader = SnmpReader(host, port, agentName, communityName, timeout, retries)
        

    """
    Produces a dictionary from the getNetworkInterfaces, getStorage,
    and getProcesses methods.
    """
    def getAll(self):
        ret = {}
        ret['Base']              = self.getBase()
        ret['NetworkInterfaces'] = self.getNetworkInterfaces()
        ret['Storage']           = self.getStorage()
        ret['Processes']         = self.getProcesses()
        return ret


    """
    """
    def getBase(self):
        uname = os.uname()

        ret = {}
        ret['python_SystemName'] = uname[0]
        ret['python_NodeName']   = uname[1]
        ret['python_Release']    = uname[2]
        ret['python_Version']    = uname[3]
        ret['python_Machine']    = uname[4]

        ret['rfc1213_SystemDescr']    = self.reader.get(Rfc1213Mib.system_sysDescr)
        ret['rfc1213_SystemContact']  = self.reader.get(Rfc1213Mib.system_sysContact)
        ret['rfc1213_SystemName']     = self.reader.get(Rfc1213Mib.system_sysName)
        ret['rfc1213_SystemLocation'] = self.reader.get(Rfc1213Mib.system_sysLocation)
        ret['rfc1213_UpTime']         = self.reader.get(Rfc1213Mib.system_sysUpTime)
        ret['rfc2790_UpTime'] = self.reader.get(Rfc2790Mib.hrSystemUptime)

        return ret
        
    """
    Gets information about the host's network interfaces.
    """
    def getNetworkInterfaces(self):
        return self.reader.getTable(
                Rfc1213Mib.interfaces_ifTable,
                [
                    ('Descr'     , Rfc1213Mib.interfaces_ifTable_ifDescr)
                    ,('Speed'     , Rfc1213Mib.interfaces_ifTable_ifSpeed)
                    ,('InOctets'  , Rfc1213Mib.interfaces_ifTable_ifInOctets)
                    ,('InErrors'  , Rfc1213Mib.interfaces_ifTable_ifInErrors)
                    ,('OutOctets' , Rfc1213Mib.interfaces_ifTable_ifOutOctets) 
                    ,('OutErrors' , Rfc1213Mib.interfaces_ifTable_ifOutErrors)                 
                ]
           )

    
    def getStorage(self):
        """
        Gets information about the host's storage, including disk drives and memory.
        """
        return self.reader.getTable(
                Rfc2790Mib.hrStorageTable,
                [
                    ('Descr'               , Rfc2790Mib.hrStorageDescr)
                    ,('AllocationUnits'     , Rfc2790Mib.hrStorageAllocationUnits)
                    ,('StorageSize'         , Rfc2790Mib.hrStorageSize)
                    ,('StorageUse'          , Rfc2790Mib.hrStorageUsed)
                    ,('AllocationFailuers'  , Rfc2790Mib.hrStorageAllocationFailures)
                ]
           )


    def getProcesses(self):
        """
        Gets information about processes currently running on the host.
        """
        runtable = self.reader.getTable(
                Rfc2790Mib.hrSWRunTable,
                [
                    ('RunIndex'      , Rfc2790Mib.hrSWRunIndex)
                    ,('RunName'       , Rfc2790Mib.hwSWRunName)
                    ,('RunID'         , Rfc2790Mib.hrSWRunID)
                    ,('RunPath'       , Rfc2790Mib.hrSWRunPath)
                    ,('RunParameters' , Rfc2790Mib.hrSWRunParameters)
                    ,('RunType'       , Rfc2790Mib.hrSWRunType)
                    ,('RunStatus'     , Rfc2790Mib.hrSWRunStatus)
                ],
                True
           )
        
        perftable = self.reader.getTable(
                Rfc2790Mib.hrSWRunPerfTable,
                [
                    ('CPU' , Rfc2790Mib.hrSWRunPerfCPU)
                    ,('Mem' , Rfc2790Mib.hrSWRunPerfMem)                ],
                True
            )
        # not sure why SWRunPerf and SWRun tables are separate in 
        # the MIB.  They make more sense as one happy table.  So we'll
        # stitch them together.
        ret = [];
        for rkey in runtable:
            row = runtable[rkey]
            if perftable.has_key(rkey):
                for pkey in perftable[rkey]:
                    row[pkey] = perftable[rkey][pkey]
            ret.append(row)
            
        return ret
        



class SnmpReader:
    """
    Reads common SNMP data from the specified host.  RFC1213 and Rfc2790
    MIBs are specifically targeted, more information available here 
    http://www.ietf.org/rfc/rfc1213.txt and also here
    http://portal.acm.org/citation.cfm?id=Rfc2790Mib  
    """            

    def __init__(self, host, port, agentName, communityName, timeout=1.5, retries=3):
        self.agentName = agentName
        self.communityName = communityName
        self.host = host
        self.port = port
        self.timeout = timeout
        self.retries = 3
        self.supportsSNMP = True
        if not PysnmpImported:
            self.supportsPysnmp = False
            self.supportsRfc2790 = False
            self.supportsRfc1213 = False
        else:    
            self.supportsRfc2790 = self.get(Rfc2790Mib.hrSystemNumUsers) != None
            self.supportsRfc1213 = self.get(Rfc1213Mib.system_sysDescr) != None
            self.supportsPysnmp = True
        self.supportsSNMP = PysnmpImported and (self.supportsRfc1213 or self.supportsRfc2790)  
 

 
    def get(self, oid):
        """ 
        Gets an SNMP single value and converts it into a more mainstream
        value (i.e. gets rid of ANS1).
        """
        if not self.supportsSNMP:
            return None
        
        shot = self._get(oid)
        try:
            return shot[3][0][1]._value
        except:
            return None
    
        
    def getTable(self, tableOid, fields, includeId = False):
        """
        Gets an SNMP table value and converts it into a more mainstream
        value (i.e. gets rid of ASN1 and converts key-value pairs into
        a list of dictionaries.  Result should be JSON ready.)
        """        
        if not self.supportsSNMP:
            return []

        table = self._getNext(tableOid)
        
        # SNMP can return non-sequential row numbers.  So we check which
        # rows are available explicitly.
        oid = list(fields[0][1])
        ids = []
        for row in table:
            if row[:-1] == fields[0][1]:
                ids.append(row[-1])

        # Actually put all the loose values into a real table.  
        if includeId == True:
            ret = {}
        else:
            ret = []
        for i in ids:
            row = {}
            for f in fields:
                oid = list(f[1])
                oid.append(i)
                if (table.has_key(tuple(oid))):
                    row[f[0]] = table[tuple(oid)]._value
            if includeId:
                ret[i] = row
            else:
                ret.append(row)
        return ret



    def _get(self, object):
        """ 
        Implements SNMP's get function.
        """
        
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().getCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget(
                                      (self.host, self.port), 
                                      timeout=self.timeout, 
                                      retries=self.retries ),
            object
        )
        return errorIndication, errorStatus, errorIndex, varBinds



    def _getNext(self, object):
        """
        Implements the SNMP getNext function.
        """
        
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().nextCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget((self.host, self.port), timeout=self.timeout, retries=self.retries),
            object
        )
        ret = {}
        for row in varBinds:
            ret[eval(str(row[0][0]))] = row[0][1]
        return ret



class Rfc2790Mib:
    """
    RFC 2790 MIB OIDs which of are interest to the OOICI project.
    """
    hrSystemUptime    = (1,3,6,1,2,1,25,1,1,0)
    hrSystemDate      = (1,3,6,1,2,1,25,1,2,0)
    hrSystemNumUsers  = (1,3,6,1,2,1,25,1,5,0)
    hrSystemProcesses = (1,3,6,1,2,1,25,1,6,0)

    hrStorageTable              = (1,3,6,1,2,1,25,2,3)
    hrStorageDescr              = (1,3,6,1,2,1,25,2,3,1,3)                            
    hrStorageAllocationUnits    = (1,3,6,1,2,1,25,2,3,1,4)
    hrStorageSize               = (1,3,6,1,2,1,25,2,3,1,5)
    hrStorageUsed               = (1,3,6,1,2,1,25,2,3,1,6)
    hrStorageAllocationFailures = (1,3,6,1,2,1,25,2,3,1,7)

    hrSWRunPerfTable = (1,3,6,1,2,1,25,5,1)
    hrSWRunPerfCPU   = (1,3,6,1,2,1,25,5,1,1,2)
    hrSWRunPerfMem   = (1,3,6,1,2,1,25,5,1,1,1)
                        
    hrSWRunTable =      (1,3,6,1,2,1,25,4,2,1)
    hrSWRunIndex =      (1,3,6,1,2,1,25,4,2,1,1)
    hwSWRunName =       (1,3,6,1,2,1,25,4,2,1,2)
    hrSWRunID =         (1,3,6,1,2,1,25,4,2,1,3)
    hrSWRunPath =       (1,3,6,1,2,1,25,4,2,1,4)
    hrSWRunParameters = (1,3,6,1,2,1,25,4,2,1,5)
    hrSWRunType =       (1,3,6,1,2,1,25,4,2,1,6)
    hrSWRunStatus =     (1,3,6,1,2,1,25,4,2,1,7)


    
class Rfc1213Mib:
    """
    RFC 1213 MIB OIDs which of are interest to the OOICI project.
    """
    system_sysDescr =     (1,3,6,1,2,1,1,1,0)
    system_sysUpTime =    (1,3,6,1,2,1,1,3,0)
    system_sysContact =   (1,3,6,1,2,1,1,4,0)
    system_sysName =      (1,3,6,1,2,1,1,5,0)
    system_sysLocation =  (1,3,6,1,2,1,1,6,0)

    interfaces_ifNumber = (1,3,6,1,2,1,2,1,0)
    interfaces_ifTable =  (1,3,6,1,2,1,2,2)
    interfaces_ifTable_ifDescr = (1,3,6,1,2,1,2,2,1,2)
    interfaces_ifTable_ifSpeed = (1,3,6,1,2,1,2,2,1,5)
    interfaces_ifTable_ifInOctets  = (1,3,6,1,2,1,2,2,1,10)
    interfaces_ifTable_ifInErrors  = (1,3,6,1,2,1,2,2,1,14)
    interfaces_ifTable_ifOutOctets = (1,3,6,1,2,1,2,2,1,16)
    interfaces_ifTable_ifOutErrors = (1,3,6,1,2,1,2,2,1,20)
