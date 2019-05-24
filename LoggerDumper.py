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
        d = Device()
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
                d = Device()
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
            self.logger.log(logging.CRIRICAL, "No ADC found")
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
                        print("\n%s New Shot %d\n", self.timeStamp(), shotNew)
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

                        print("Saving from " + d.fullName())
                        self.dumpADCDataAndLog(d, self.zipFile, self.logFile, d.folder)
                    except :
                        d.active = False
                        d.timeout = time.time() + 10000
                        self.logger.log(logging.INFO, "ADC %s inactive, timeout for 10 seconds", d.fullName())

                if self.locked:
                    self.zipFile.close()
                    # write zip file name
                    fmt = '; ' + "File" + '=' + "%s"
                    zfn = self.zipFile.filename
                    self.logFile.write(fmt % zfn)
                    self.logFile.write(b'\r\n')
                    self.logFile.close()
                    self.unlockDir()
                    print("\n%s Waiting for next shot ..." % self.timeStamp())
            except:
                self.logger.log(logging.CRIRICAL, "Unexpected exception")
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
            self.logger.log(logging.CRIRICAL, "Output folder %s not created", self.outFolder)
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
        os.remove(self.lockFile)
        self.locked = False
        self.logger.log(logging.DEBUG, "Directory unlocked")


    def timeStamp(self):
        return datetime.datetime.today().strftime('%H:%M:%S')


    def dumpADCDataAndLog(self, adc, zipFile, logFile, folder):
    	atts = adc.devProxy.get_attribute_info()
        retryCount = 0
        for i in range(len(atts)):
            if atts[i].name.startswith("chany"):
                try :
                    chan = Channel(adc, atts[i].name)
                    saveDataFlag = chan.getPropertyAsBoolean(Constants.SAVE_DATA)
                    saveLogFlag = chan.getPropertyAsBoolean(Constants.SAVE_LOG)
                    if saveDataFlag or saveLogFlag:
                        sig = Signal(chan)
                        self.saveSignalProp(zipFile, sig, folder)
                        if saveDataFlag:
                            sig.readData()
                            self.saveSignalData(zipFile, sig, folder)
                        if saveLogFlag:
                            self.saveSignalLog(logFile, sig)
                    retryCount = 0
                except:
                    self.logFile.flush()
                    self.zipFile.closeEntry()
                    retryCount += 1
                if retryCount > 0 or retryCount < 3:
                    print("Retry reading channel %s" % atts[i].name)
                    i -= 1
                if retryCount >= 3:
                    print("Error reading channel %s" % atts[i].name)


    def saveToZip(self, zipFile, x, y, avgc):
        xs = 0.0
        ys = 0.0
        ns = 0.0
        fmt = '%f; %f'
        s = ''

        if y == None or x == None :
            return
        if len(y) <= 0 or len(x) <= 0 :
            return
        if len(y) > len(x):
            return

        if avgc < 1:
            avgc = 1

        for i in range(len(y)):
            xs += x[i]
            ys += y[i]
            ns += 1
            if ns >= avgc:
                if i >= avgc:
                    zipFile.writestr(b'\r\n')
                s = fmt % (xs / ns, ys / ns)
                zipFile.writestr(s.replace(",", "."))
                xs = 0.0
                ys = 0.0
                ns = 0.0
        if ns > 0:
            zipFile.writestr(b'\r\n')
            s = fmt % (xs / ns, ys / ns)
            zipFile.wrte(s.replace(",", "."))
            xs = 0.0
            ys = 0.0
            ns = 0.0
        zipFile.flush()

    def saveSignalData(self, zipFile, sig, folder):
        entryName = folder + sig.name + Constants.EXTENSION
        zipFile.putNextEntry(entryName)
        saveAvg = sig.getPropInteger(Constants.SAVE_AVG)
        if saveAvg < 10:
            saveAvg = 10
        #// print("saveAvg: %d\r\n", saveAvg)
        self.saveToZip(zipFile, sig.x.data, sig.y.data, saveAvg)
        zipFile.flush()
        zipFile.closeEntry()

    def saveSignalProp(self, zipFile, sig, folder):
        zipFile.flush()
        entryName = folder + Constants.PARAM + sig.name + Constants.EXTENSION
        zipFile.putNextEntry(entryName)
        zipFile.writestr(entryName, "Name=%s\r\n" % sig.fullName())
        zipFile.writestr(entryName, "Shot=%d\r\n" % sig.shot())
        propList = sig.getPropValList()
        if len(propList) > 0:
            for prop in propList:
                zipFile.writestr(entryName,"%s\r\n" % prop)
        zipFile.closeEntry()

    def saveSignalLog(self, logFile, sig):
        #// Get signal label = default mark name
        label = sig.getPropString(Constants.LABEL)
        #//print("label = %s\n", label)

        #// Get unit name
        unit = sig.getPropString(Constants.UNIT)
        #//print("unit = %s\n", unit)

        #// Get calibration coefficient for conversion to unit
        coeff = sig.getPropDouble(Constants.DISPLAY_UNIT)
        if coeff == 0.0:
            coeff = 1.0
        #//print("coeff = %g\n", coeff)

        marks = sig.getMarkList()

        #// Find zero value
        zero = 0.0
        for mark in marks:
            if Constants.ZERO_NAME.equals(mark.name):
                zero = mark.yValue
                #//print("zero = %g\n", zero)
                break
        #// Find all marks and log (mark - zero)*coeff
        for mark in marks:
            firstLine = True
            if not Constants.ZERO_NAME.equals(mark.name):
                logMarkValue = (mark.yValue - zero) * coeff

                logMarkName = mark.name
                if logMarkName.equals(Constants.MARK_NAME):
                    logMarkName = label
                #//print(Constants.LOG_CONSOLE_FORMAT, logMarkName, logMarkValue, unit)
                if firstLine:
                    print("%7s " % sig.name())
                else :
                    print("%7s " % "  ")

                if abs(logMarkValue) >= 1000.0:
                    print("%10s = %7.0f %s\n" % (logMarkName, logMarkValue, unit))
                elif abs(logMarkValue) >= 100.0:
                    print("%10s = %7.1f %s\n", logMarkName, logMarkValue, unit)
                elif abs(logMarkValue) >= 10.0:
                    print("%10s = %7.2f %s\n", logMarkName, logMarkValue, unit)
                else:
                    print("%10s = %7.3f %s\n", logMarkName, logMarkValue, unit)
                firstLine = False

                fmt = '; ' + Constants.LOG_FORMAT
                self.logFile.write(fmt % (logMarkName, logMarkValue, unit))

if __name__ == '__main__':
    lgd = LoggerDumper()
    try:
        lgd.readConfig()
        lgd.process()
    except:
        lgd.logger.log(logging.CRIRICAL, "Exception in LoggerDumper")
        lgd.printExceptionInfo()


class Device:
    def __init__(self, host='192.168.1.41', port=10000, dev='binp/nbi/adc0', avg=100, folder="ADC_0"):
        self.host = host
        self.port = port
        self.dev = dev
        self.folder = folder
        self.avg = avg
        self.active = False
        self.shot = -8888
        self.timeout = time.time()
        self.devProxy = None


    def getName(self):
        return self.host + ":" + self.port + "/" + self.dev


    def init(self):
        try:
            self.devProxy = tango.DeviceProxy(self.getName())
            self.active = True
        except:
            self.active = False
            self.timeout = time.time() + 10000


    def readShot(self):
        newShot = -1
        try :
            da = self.devProxy.read_attribute("Shot_id")
            newShot = da.value
            return newShot
        except :
            return -2