import os
import os.path
import sys
import json
import logging
import datetime
import time
import zipfile

import numpy as np
import tango


class Constants:
    DEFAULT_HOST = "192.168.161.74"
    DEFAULT_PORT = "10000"
    DEFAULT_DEV = "binp/nbi/adc0"
    DEFAULT_AVG = 100

    DEFAULT_MARK_X_VALUE = None
    DEFAULT_MARK_Y_VALUE = 0.0

    PROP_VAL_DELIMITER = " = "
    PROP_VAL_DELIMITER_OLD = ": "

    ZERO_NAME = "zero"
    ZERO_MARK_NAME = "Zero"
    MARK_NAME = "mark"

    START_SUFFIX = "_start"
    LENGTH_SUFFIX = "_length"
    NAME_SUFFIX = "_name"

    UNIT = "unit"
    LABEL = "label"
    DISPLAY_UNIT = "display_unit"
    SAVE_DATA = "save_data"
    SAVE_AVG = "save_avg"
    SAVE_LOG = "save_log"
    SHOT_ID = "Shot_id"

    CHAN = "chan"
    PARAM = "param"

    EXTENSION = ".txt"

    XY_DELIMITER = " "
    X_FORMAT = "%f"
    Y_FORMAT = X_FORMAT
    XY_FORMAT = X_FORMAT + XY_DELIMITER + Y_FORMAT

    CRLF = "\r\n"

    LOG_DELIMITER = " "
    LOG_FORMAT = "%s = %7.3f %s"
    LOG_CONSOLE_FORMAT = "%10s = %7.3f %s\n"


class ADC:
    def __init__(self, host='192.168.1.41', port=10000, dev='binp/nbi/adc0', avg=100, folder="ADC_0"):
        self.host = host
        self.port = port
        self.name = dev
        self.folder = folder
        self.avg = avg
        self.active = False
        self.shot = -8888
        self.timeout = time.time()
        self.devProxy = None
        self.db = None

    def getName(self):
        return self.host + ":" + self.port + "/" + self.name

    def init(self):
        try:
            self.db = tango.Database()
            self.devProxy = tango.DeviceProxy(self.getName())
            self.active = True
        except:
            self.active = False
            self.timeout = time.time() + 10000

    def readShot(self):
        try:
            da = self.devProxy.read_attribute("Shot_id")
            newShot = da.value
            return newShot
        except:
            return -1


class Channel:
    def __init__(self, adc, name):
        self.dev = adc
        try:
            i = int(name)
            self.name = 'chany' + str(i)
        except:
            self.name = name
        self.prop = None
        self.attr = None

    def readProperties(self):
        # read signal properties
        ap = self.dev.db.get_device_attribute_property(self.dev.name, self.name)
        self.prop = ap[self.name]

    def readData(self):
        self.attr = self.dev.devProxy.read_attribute(self.name)
        return self.attr.value

    def readXData(self):
        if not self.name.startswith('chany'):
            self.xvalue = np.arange(len(self.attr.value))
        else:
            self.xvalue = self.dev.devProxy.read_attribute(self.name.replace('y', 'x')).value
        return self.xvalue

    def getPropAsBoolean(self, propName):
        propVal = None
        try:
            propString = self.getProp(propName).lower()
            if propString == "true":
                propVal = True
            elif propString == "on":
                propVal = True
            elif propString == "1":
                propVal = True
            else:
                propVal = False
            return propVal
        except:
            return propVal

    def getPropAsInt(self, propName):
        try:
            return int(self.getProp(propName))
        except:
            return None

    def getPropAsFloat(self, propName):
        try:
            return float(self.getProp(propName))
        except:
            return None

    def getProp(self, propName):
        ps = None
        try:
            if self.prop is None:
                self.readProperties()
            ps = self.prop[propName][0]
            return ps
        except:
            return ps

    def get_marks(self):
        if self.prop is None:
            self.readProperties()
        if self.attr is None:
            self.readData()
        ml = {}
        for pk in self.prop:
            if pk.endswith(Constants.START_SUFFIX):
                pv = int(self.prop[pk][0])
                pn = pk.replace(Constants.START_SUFFIX, "")
                pln = pn + Constants.LENGTH_SUFFIX
                if pln in self.prop:
                    pl = int(self.prop[pln][0])
                else:
                    pl = 1
                ml[pn] = self.attr.value[pv:pv+pl].mean()
        return ml


class LoggerDumper:
    def __init__(self):
        # configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.log_formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                               datefmt='%H:%M:%S')
        self.console_handler = logging.StreamHandler()
        # self.console_handler.setLevel(logging.WARNING)
        self.console_handler.setFormatter(self.log_formatter)
        self.logger.addHandler(self.console_handler)

        self.progName = "Adlink DAQ-2204 PyTango Logger"
        self.progNameShort = "LoggerDumperPy"
        self.progVersion = "0.1"
        self.configFileName = self.progNameShort + ".json"

        self.outRootDir = ".\\data\\"
        self.outFolder = ".\\data\\"
        self.devList = []
        self.lockFile = "lock.lock"
        self.locked = False

    def readConfig(self):
        # no command line parameters
        if len(sys.argv) <= 1:
            self.restoreSettings()
            return
        # first command line parameter - config file
        if sys.argv[1].endswith(".json"):
            self.configFileName = sys.argv[1]
            self.restoreSettings()
            return
        # host port device averaging in command line parameters
        self.devList = []
        d = ADC()
        try:
            d.folder = "ADC_0"
            d.host = sys.argv[1]
            d.port = sys.argv[2]
            d.dev = sys.argv[3]
            d.avg = int(sys.argv[4])
            self.outRootDir = sys.argv[5]
        except:
            pass
        self.devList.append(d)
        self.logger.log(logging.DEBUG, "ADC %s", d.getName())

    def restoreSettings(self, folder=''):
        try :
            fullName = os.path.join(str(folder), self.configFileName)
            with open(fullName, 'r') as configfile:
                s = configfile.read()
            self.conf = json.loads(s)

            # restore log level
            s = logging.DEBUG
            if 'Loglevel' in self.conf:
                s = self.conf['Loglevel']
            self.logger.setLevel(s)
            self.logger.log(logging.DEBUG, "Log level %s set", s)

            # read output directory
            self.outRootDir = self.conf["outDir"]

            # number of ADCs
            self.devList = []
            n = 0
            if 'ADCCount' in self.conf:
                n = self.conf["ADCCount"]
            if n <= 0:
                self.logger.log(logging.WARNING, "No ADC declared")
                return
            # read ADCs
            for j in range(n):
                d = ADC()
                section = "ADC_" + j
                d.host = self.conf[section]["host"]
                d.port = self.conf[section]["port"]
                d.dev = self.conf[section]["device"]
                d.folder = self.conf[section]["folder"]
                d.avg = self.conf[section]["avg"]
                self.devList.append(d)
                self.logger.log(logging.DEBUG, "ADC %s", d.getName())

            # print OK message and exit
            self.logger.info('Configuration restored from %s'%fullName)
            return True
        except :
            # print error info
            self.printExceptionInfo()
            self.logger.info('Configuration restore error from %s'%fullName)
            return False

    def printExceptionInfo(self):
        #excInfo = sys.exc_info()
        #(tp, value) = sys.exc_info()[:2]
        #self.logger.log(level, 'Exception %s %s'%(str(tp), str(value)))
        self.logger.error("Exception ", exc_info=True)

    def process(self) :
        self.logFile = None
        self.zipFile = None

        if len(self.devList) <= 0 :
            self.logger.log(logging.CRITICAL, "No ADC found")
            return

        # fill AdlinkADC in deviceList
        # active ADC count
        count = 0
        for d in self.devList :
            try :
                d.init()
                count += 1
            except :
                self.logger.log(logging.INFO, "ADC %s initialization error", d.getName())
        if count == 0 :
            self.logger.log(logging.WARNING, "No active ADC found")
            return

        shotNew = 0

        while True :
            try :
                for d in self.devList:
                    try :
                        if not d.active:
                            if d.timeout > time.time():
                                continue
                            d.init()
                            self.logger.log(logging.DEBUG, "ADC %s activated", d.fullName())
                        shotNew = d.readShot()
                        if shotNew <= d.shot:
                            if self.locked:
                                continue
                            else:
                                break

                        d.shot = shotNew
                        print("\n%s New Shot %d\n" % (self.timeStamp(), shotNew))
                        if not self.locked:
                            self.makeFolder()
                            self.lockDir(self.outFolder)
                            self.logFile = self.openLogFile(self.outFolder)
                            # write date and time
                            self.logFile.write(self.dateTimeStamp())
                            # wrie shot number
                            fmt = '; ' + "Shot" + '=' + "%5d"
                            self.logFile.write(fmt % shotNew)
                            # open zip file
                            self.zipFile = self.openZipFile(self.outFolder)

                        print("Saving from " + d.getName())
                        self.dumpDataAndLog(d, self.zipFile, self.logFile)
                    except :
                        d.active = False
                        d.timeout = time.time() + 10000
                        self.logger.log(logging.INFO, "ADC %s inactive, timeout for 10 seconds", d.getName())

                if self.locked:
                    self.zipFile.close()
                    # write zip file name
                    fmt = '; ' + "File" + '=' + "%s"
                    zfn = self.zipFile.filename
                    self.logFile.write(fmt % zfn)
                    self.logFile.write('\r\n')
                    self.logFile.close()
                    self.unlockDir()
                    print("\n%s Waiting for next shot ..." % self.timeStamp())
            except:
                self.logger.log(logging.CRITICAL, "Unexpected exception")
                self.printExceptionInfo()
                return
            time.sleep(1)

    def makeFolder(self):
        if not self.outRootDir.endswith("\\"):
            self.outRootDir = self.outRootDir + "\\"
        self.outFolder = os.path.join(self.outRootDir, self.getLogFolderName())
        try:
            if not os.path.exists(self.outFolder):
                os.makedirs(self.outFolder)
                self.logger.log(logging.DEBUG, "Folder %s created", self.outFolder)
                return True
        except:
            self.logger.log(logging.CRITICAL, "Output folder %s not created", self.outFolder)
            return False

    def getLogFolderName(self):
        ydf = datetime.datetime.today().strftime('%Y')
        mdf = datetime.datetime.today().strftime('%Y-%m')
        ddf = datetime.datetime.today().strftime('%Y-%m-%d')
        folder = os.path.join(ydf, mdf, ddf)
        return folder

    def lockDir(self, folder):
        self.lockFile = open(os.path.join(folder, "lock.lock"), 'w+')
        self.locked = True
        self.logger.log(logging.DEBUG, "Directory %s locked", folder)

    def openLogFile(self, folder=''):
        self.logFileName = os.path.join(folder, self.getLogFileName())
        logf = open(self.logFileName, 'a')
        return logf

    def getLogFileName(self):
        logfn = datetime.datetime.today().strftime('%Y-%m-%d.log')
        return logfn

    def dateTimeStamp(self):
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    def openZipFile(self, folder):
        fn = datetime.datetime.today().strftime('%Y-%m-%d_%H%M%S.zip')
        zipFileName = os.path.join(folder, fn)
        zipFile = zipfile.ZipFile(zipFileName, 'a')
        return zipFile

    def unlockDir(self):
        self.lockFile.close()
        os.remove(self.lockFile.name)
        self.locked = False
        self.logger.log(logging.DEBUG, "Directory unlocked")

    def timeStamp(self):
        return datetime.datetime.today().strftime('%H:%M:%S')

    def dumpDataAndLog(self, adc, zipFile, logFile):
        atts = adc.devProxy.get_attribute_list()
        #retry_count = 0
        for a in atts:
            if a.startswith("chany"):
                retry_count = 3
                while retry_count > 0:
                    try :
                        chan = Channel(adc, a)
                        # read save_data and save_log flags
                        save_data_flag = chan.getPropAsBoolean(Constants.SAVE_DATA)
                        saveLogFlag = chan.getPropAsBoolean(Constants.SAVE_LOG)
                        # save signal properties
                        if save_data_flag or saveLogFlag:
                            self.saveSignalProp(zipFile, chan)
                            if save_data_flag:
                                chan.readData()
                                self.saveSignalData(zipFile, chan)
                            if saveLogFlag:
                                self.saveSignalLog(logFile, chan)
                        retry_count = -1
                    except:
                        self.printExceptionInfo()
                        self.logFile.flush()
                        #self.zipFile.close()
                        retry_count -= 1
                    if retry_count > 0:
                        print("Retry reading channel %s" % a.name)
                    if retry_count == 0:
                        print("Error reading channel %s" % a.name)

    def convertToBuf(self, x, y, avgc):
        xs = 0.0
        ys = 0.0
        ns = 0.0
        fmt = '%f; %f'
        s = ''
        outbuf = ''

        if y is None or x is None :
            return outbuf
        if len(y) <= 0 or len(x) <= 0 :
            return outbuf
        if len(y) > len(x):
            return outbuf

        if avgc < 1:
            avgc = 1

        for i in range(len(y)):
            xs += x[i]
            ys += y[i]
            ns += 1.0
            if ns >= avgc:
                if i >= avgc:
                    outbuf += '\r\n'
                s = fmt % (xs / ns, ys / ns)
                outbuf += s.replace(",", ".")
                xs = 0.0
                ys = 0.0
                ns = 0.0
        if ns > 0:
            outbuf += '\r\n'
            s = fmt % (xs / ns, ys / ns)
            outbuf += s.replace(",", ".")
            xs = 0.0
            ys = 0.0
            ns = 0.0
        return outbuf

    def saveSignalData(self, zipFile, chan):
        entryName = chan.dev.folder + "/" + chan.name + Constants.EXTENSION
        saveAvg = chan.getPropAsInt(Constants.SAVE_AVG)
        if saveAvg < 1:
            saveAvg = 1
        #// print("saveAvg: %d\r\n", saveAvg)
        buf = self.convertToBuf(chan.readXData(), chan.attr.value, saveAvg)
        zipFile.writestr(entryName, buf)

    def saveSignalProp(self, zipFile, chan):
        entryName = chan.dev.folder + "/" + Constants.PARAM + chan.name + Constants.EXTENSION
        outbuf = "Name=%s/%s\r\n" % (chan.dev.getName(), chan.name)
        outbuf += "Shot=%d\r\n" % chan.dev.shot
        propList = ['%s=%s'%(k,chan.prop[k][0]) for k in chan.prop]
        for prop in propList:
            outbuf += "%s\r\n" % prop
        zipFile.writestr(entryName, outbuf)

    def saveSignalLog(self, logFile, chan):
        #// Get signal label = default mark name
        label = chan.getProp(Constants.LABEL)
        #//print("label = %s\n", label)

        #// Get unit name
        unit = chan.getProp(Constants.UNIT)
        #//print("unit = %s\n", unit)

        #// Get calibration coefficient for conversion to unit
        coeff = chan.getPropAsFloat(Constants.DISPLAY_UNIT)
        if coeff is None or coeff == 0.0:
            coeff = 1.0
        #//print("coeff = %g\n", coeff)

        marks = chan.get_marks()

        # Find zero value
        zero = 0.0
        if Constants.ZERO_NAME in marks:
            zero = marks[Constants.ZERO_NAME]
        # Find all marks and log (mark - zero)*coeff
        for mark in marks:
            firstLine = True
            if not Constants.ZERO_NAME == mark:
                logMarkValue = (marks[mark] - zero) * coeff
                logMarkName = mark
                if logMarkName == Constants.MARK_NAME:
                    logMarkName = label

                # Print saved mark value
                #//print(Constants.LOG_CONSOLE_FORMAT, logMarkName, logMarkValue, unit)
                if firstLine:
                    print("%7s " % chan.name)
                else:
                    print("%7s " % "  ")

                if abs(logMarkValue) >= 1000.0:
                    print("%10s = %7.0f %s\n" % (logMarkName, logMarkValue, unit))
                elif abs(logMarkValue) >= 100.0:
                    print("%10s = %7.1f %s\n" % (logMarkName, logMarkValue, unit))
                elif abs(logMarkValue) >= 10.0:
                    print("%10s = %7.2f %s\n" % (logMarkName, logMarkValue, unit))
                else:
                    print("%10s = %7.3f %s\n" % (logMarkName, logMarkValue, unit))
                firstLine = False

                fmt = '; ' + Constants.LOG_FORMAT
                self.logFile.write(fmt % (logMarkName, logMarkValue, unit))


if __name__ == '__main__':
    lgd = LoggerDumper()
    try:
        lgd.readConfig()
        lgd.process()
    except:
        lgd.logger.log(logging.CRITICAL, "Exception in LoggerDumper")
        lgd.printExceptionInfo()


