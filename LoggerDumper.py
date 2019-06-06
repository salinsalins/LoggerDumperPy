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

# configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                       datefmt='%H:%M:%S')
console_handler = logging.StreamHandler()
# self.console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)


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

    def get_name(self):
        return "%s:%d/%s" % (self.host, self.port, self.name)

    def init(self):
        try:
            self.db = tango.Database()
            self.devProxy = tango.DeviceProxy(self.get_name())
            self.active = True
            logger.log(logging.DEBUG, "ADC %s activated" % self.get_name())
        except:
            self.active = False
            self.timeout = time.time() + 10000
            logger.log(logging.DEBUG, "ADC %s activation errror" % self.get_name())

    def read_shot(self):
        try:
            da = self.devProxy.read_attribute("Shot_id")
            newShot = da.value
            return newShot
        except:
            return -1


class Channel:
    def __init__(self, adc, name):
        self.dev = adc
        if type(name) is int:
            self.name = 'chany' + str(name)
        else:
            self.name = name
        self.prop = None
        self.attr = None

    def read_properties(self):
        # read signal properties
        ap = self.dev.db.get_device_attribute_property(self.dev.name, self.name)
        self.prop = ap[self.name]

    def read_data(self):
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
            propString = self.get_prop(propName).lower()
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
            return int(self.get_prop(propName))
        except:
            return None

    def getPropAsFloat(self, propName):
        try:
            return float(self.get_prop(propName))
        except:
            return None

    def get_prop(self, propName):
        ps = None
        try:
            if self.prop is None:
                self.read_properties()
            ps = self.prop[propName][0]
            return ps
        except:
            return ps

    def get_marks(self):
        #print(self.name)
        #print(self.prop)
        if self.prop is None:
            self.read_properties()
        if self.attr is None:
            self.read_data()
        ml = {}
        for pk in self.prop:
            if pk.endswith(Constants.START_SUFFIX):
                pn = pk.replace(Constants.START_SUFFIX, "")
                try:
                    #print(pk, self.prop[pk])
                    pv = int(self.prop[pk][0])
                    pln = pn + Constants.LENGTH_SUFFIX
                    if pln in self.prop:
                        pl = int(self.prop[pln][0])
                    else:
                        pl = 1
                    ml[pn] = self.attr.value[pv:pv+pl].mean()
                except:
                    ml[pn] = self.attr.value[0]
        return ml


class LoggerDumper:
    def __init__(self):
        self.progName = "Adlink DAQ-2204 PyTango Logger"
        self.progNameShort = "LoggerDumperPy"
        self.progVersion = "1.1"
        self.configFileName = self.progNameShort + ".json"

        self.outRootDir = ".\\data\\"
        self.outFolder = ".\\data\\"
        self.devList = []
        self.lockFile = "lock.lock"
        self.locked = False

    def read_config(self):
        # no command line parameters
        if len(sys.argv) <= 1:
            logger.log(logging.DEBUG, "No command line config")
            self.restore_settings()
            return
        # first command line parameter - config file
        if sys.argv[1].endswith(".json"):
            self.configFileName = sys.argv[1]
            self.restore_settings()
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
        logger.log(logging.DEBUG, "Configuration set from command line")
        self.devList.append(d)
        logger.log(logging.DEBUG, "ADC %s" % d.get_name())

    def restore_settings(self, folder=''):
        fullName = os.path.join(str(folder), self.configFileName)
        logger.log(logging.DEBUG, "Reading config from file %s" % fullName)
        try :
            with open(fullName, 'r') as configfile:
                s = configfile.read()
            self.conf = json.loads(s)

            # restore log level
            s = logging.DEBUG
            if 'Loglevel' in self.conf:
                s = self.conf['Loglevel']
            logger.setLevel(s)
            logger.log(logging.DEBUG, "Log level %d set" % s)

            # read output directory
            self.outRootDir = self.conf["outDir"]

            # number of ADCs
            self.devList = []
            n = 0
            if 'ADCCount' in self.conf:
                n = self.conf["ADCCount"]
            if n <= 0:
                logger.log(logging.WARNING, "No ADC declared")
                return
            # read ADCs
            for j in range(n):
                d = ADC()
                section = "ADC_" + str(j)
                d.host = self.conf[section]["host"]
                d.port = self.conf[section]["port"]
                d.dev = self.conf[section]["device"]
                d.folder = self.conf[section]["folder"]
                d.avg = self.conf[section]["avg"]
                self.devList.append(d)
                logger.log(logging.DEBUG, "ADC %s" % d.get_name())

            # print OK message and exit
            logger.info('Configuration restored from %s' % fullName)
            return True
        except :
            # print error info
            self.printExceptionInfo()
            logger.info('Configuration restore error from %s'%fullName)
            return False

    def printExceptionInfo(self):
        #excInfo = sys.exc_info()
        #(tp, value) = sys.exc_info()[:2]
        #logger.log(level, 'Exception %s %s'%(str(tp), str(value)))
        logger.error("Exception ", exc_info=True)

    def process(self) :
        self.logFile = None
        self.zipFile = None

        if len(self.devList) <= 0 :
            logger.log(logging.CRITICAL, "No ADC found")
            return

        # fill AdlinkADC in deviceList
        # active ADC count
        count = 0
        for d in self.devList :
            try :
                d.init()
                count += 1
            except :
                logger.log(logging.INFO, "ADC %s initialization error" % d.get_name())
        if count == 0 :
            logger.log(logging.WARNING, "No active ADC found")
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
                            logger.log(logging.DEBUG, "ADC %s was activated", d.fullName())
                        shotNew = d.read_shot()
                        if shotNew <= d.shot:
                            if self.locked:
                                continue
                            else:
                                break

                        d.shot = shotNew
                        print("\n%s New Shot %d\n" % (self.timeStamp(), shotNew))
                        if not self.locked:
                            self.make_folder()
                            self.lockDir(self.outFolder)
                            self.logFile = self.openLogFile(self.outFolder)
                            # Write date and time
                            self.logFile.write(self.dateTimeStamp())
                            # Wrie shot number
                            self.logFile.write('; Shot=%5d' % shotNew)
                            # Open zip file
                            self.zipFile = self.openZipFile(self.outFolder)

                        print("Saving from ADC " + d.get_name())
                        self.dumpDataAndLog(d, self.zipFile, self.logFile)
                    except :
                        d.active = False
                        d.timeout = time.time() + 10000
                        logger.log(logging.INFO, "ADC %s inactive, timeout for 10 seconds", d.get_name())

                if self.locked:
                    self.zipFile.close()
                    # Write zip file name
                    zfn = os.path.basename(self.zipFile.filename)
                    self.logFile.write('; File=%s' % zfn)
                    self.logFile.write('\n')
                    self.logFile.close()
                    self.unlockDir()
                    print("\n%s Waiting for next shot ..." % self.timeStamp())
            except:
                logger.log(logging.CRITICAL, "Unexpected exception")
                self.printExceptionInfo()
                return
            time.sleep(1)

    def make_folder(self):
        if not self.outRootDir.endswith("\\"):
            self.outRootDir = self.outRootDir + "\\"
        self.outFolder = os.path.join(self.outRootDir, self.getLogFolderName())
        try:
            if not os.path.exists(self.outFolder):
                os.makedirs(self.outFolder)
                logger.log(logging.DEBUG, "Folder %s created", self.outFolder)
                return True
        except:
            logger.log(logging.CRITICAL, "Output folder %s not created", self.outFolder)
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
        logger.log(logging.DEBUG, "Directory %s locked", folder)

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
        zipFile = zipfile.ZipFile(zipFileName, 'a', compression=zipfile.ZIP_DEFLATED)
        return zipFile

    def unlockDir(self):
        self.lockFile.close()
        os.remove(self.lockFile.name)
        self.locked = False
        logger.log(logging.DEBUG, "Directory unlocked")

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
                                chan.read_data()
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
        outbuf = "Signal_Name=%s/%s\r\n" % (chan.dev.get_name(), chan.name)
        outbuf += "Shot=%d\r\n" % chan.dev.shot
        propList = ['%s=%s'%(k,chan.prop[k][0]) for k in chan.prop]
        for prop in propList:
            outbuf += "%s\r\n" % prop
        zipFile.writestr(entryName, outbuf)

    def saveSignalLog(self, logFile, chan):
        # Signal label = default mark name
        label = chan.get_prop('label')
        if label is None or '' == label:
            label = chan.get_prop('name')
        if label is None or '' == label:
            label = chan.name
        # Units
        unit = chan.get_prop('unit')
        # Calibration coefficient for conversion to units
        coeff = chan.getPropAsFloat(Constants.DISPLAY_UNIT)
        if coeff is None or coeff == 0.0:
            coeff = 1.0

        marks = chan.get_marks()

        # Find zero value
        zero = 0.0
        if Constants.ZERO_NAME in marks:
            zero = marks[Constants.ZERO_NAME]

        # Find all marks and calculate mark_value = (mark - zero)*coeff
        for mark in marks:
            first_line = True
            # if it is not zero mark
            if not Constants.ZERO_NAME == mark:
                mark_value = (marks[mark] - zero) * coeff
                mark_name = mark
                # Default mark renamed to label
                if mark_name == Constants.MARK_NAME:
                    mark_name = label

                # Print mark name = value
                if first_line:
                    print("%10s " % chan.name, end='')
                else:
                    print("%10s " % "  ", end='')
                pmn = mark_name
                if len(mark_name) > 14:
                    pmn = mark_name[:5] + '...' + mark_name[-6:]
                if abs(mark_value) >= 1000.0:
                    print("%14s = %7.0f %s\r\n" % (pmn, mark_value, unit), end='')
                elif abs(mark_value) >= 100.0:
                    print("%14s = %7.1f %s\r\n" % (pmn, mark_value, unit), end='')
                elif abs(mark_value) >= 10.0:
                    print("%14s = %7.2f %s\r\n" % (pmn, mark_value, unit), end='')
                else:
                    print("%14s = %7.3f %s\r\n" % (pmn, mark_value, unit), end='')
                first_line = False

                fmt = "; %s = %7.3f %s"
                self.logFile.write(fmt % (mark_name, mark_value, unit))


if __name__ == '__main__':
    lgd = LoggerDumper()
    try:
        lgd.read_config()
        lgd.process()
    except:
        lgd.logger.log(logging.CRITICAL, "Exception in LoggerDumper")
        lgd.printExceptionInfo()


