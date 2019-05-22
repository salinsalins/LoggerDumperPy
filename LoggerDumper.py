import os.path
import sys
import json
import logging
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
        logFile = None
        zipFile = None

        if len(self.devList) <= 0 :
            self.logger.log(logging.CRIRICAL, "No ADC found")
            return

        # fill AdlinkADC in deviceList
        count = 0
        for d in self.devList :
            try :
                d.init()
                d.timeout = time.time()
                d.active = True
                count += 1
            except :
                self.logger.log(logging.INFO, "ADC %s initialization error", d.getName())
                d.active = False
                d.timeout = time.time() + 10000
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
                            d.timeout = time.time()
                            d.active = True
                            self.logger.log(logging.DEBUG, "ADC %s activated", d.fullName())
                        shotNew = d.readShot()
                        if shotNew <= d.shot:
                            if not locked:
                                break
                            else:
                                continue
                        d.shot = shotNew
                        print("\n%s New Shot %d\n", self.timeStamp(), shotNew)

                        if not locked:
                            self.makeFolder()
                            self.lockDir(self.outFolder)
                            logFile = self.openLogFile(self.outFolder)
                            # write date and time
                            logFile.format("%s", self.dateTimeStamp())
                            # wrie shot number
                            fmt = Constants.LOG_DELIMETER + "Shot" + Constants.PROP_VAL_DELIMETER + "%5d"
                            logFile.format(fmt, shotNew)
                            # open zip file
                            zipFile = self.openZipFile(outFolder)

                        print("Saving from " + d.fullName())
                        self.dumpADCDataAndLog(d, zipFile, logFile, d.folder)
                        zipFile.flush()
                    except DevFailed :
                        d.active = False
                        d.timeout = time.time() + 10000
                        self.logger.log(logging.INFO, "ADC %s inactive, timeout for 10 seconds", d.fullName())

                if locked:
                    zipFile.flush()
                    zipFile.close()
                    # write zip file name
                    fmt = Constants.LOG_DELIMETER + "File" + Constants.PROP_VAL_DELIMETER + "%s"
                    zipFileName = zipFile.getName()
                    self.logFile.format(fmt, zipFileName)
                    self.logFile.format(Constants.CRLF)
                    self.logFile.flush()
                    self.logFile.close()
                    self.unlockDir()
                    print("\n%s Waiting for next shot ...", self.timeStamp())
            except:
                self.logger.log(logging.CRIRICAL, "Unexpected exception")
                return
            self.delay(1000)


if __name__ == '__main__':
    lgd = LoggerDumper()
    try:
        lgd.readConfig()
        lgd.process()
    except:
        lgd.logger.log(logging.CRIRICAL, "Exception in LoggerDumper")
        lgd.logger.log(logging.INFO, "Exception info", ex)




    def makeFolder(self):
        if not self.outRootDir.endswith("\\"):
            self.outRootDir = self.outRootDir + "\\"
        outFolder = outRootDir + getLogFolderName()
        file = File(outFolder)
        if file.mkdirs():
            self.logger.log(logging.DEBUG, "Folder %s created", outFolder)
            return True
        else:
            self.logger.log(logging.CRIRICAL, "Output folder %s not created", outFolder)
            return False

    def getLogFolderName(self):
        ydf = SimpleDateFormat("yyyy")
        mdf = SimpleDateFormat("yyyy-MM")
        ddf = SimpleDateFormat("yyyy-MM-dd")
        now = Date()
        folder = ydf.format(now) + "\\" + mdf.format(now) + "\\" + ddf.format(now)
        return folder

    def getLogFileName(self):
        now = Date()
        dayFmt = SimpleDateFormat("yyyy-MM-dd")
        logFileName = dayFmt.format(now) + ".log"
        return logFileName

    def openLogFile(self, folder=''): #throws IOException :
        logFileName = folder + "\\" + getLogFileName()
        fw = FileWriter(logFileName, True)
        logFile = Formatter(fw)
        return logFile

    def dateTimeStamp(self):
        return dateTimeStamp(Date())

    def dateTimeStamp(self, now):
        fmt = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return fmt.format(now)

    def timeStamp(self):
        now = Date()
        logTimeFmt = SimpleDateFormat("HH:mm:ss")
        return logTimeFmt.format(now)

    def openZipFile(self, folder):
        now = Date()
        timeFmt = SimpleDateFormat("yyyy-MM-dd_HHmmss")
        zipFileName = folder + "\\" + timeFmt.format(now) + ".zip"
        zipFile = ZipFormatter(zipFileName)
        #//print("Created file %s\r\n", zipFileName)
        return zipFile

    def saveToZip(ZipFormatter zipFile, double[] x, double[] y, int avgc): # throws IOException :
        xs = 0.0
        ys = 0.0
        ns = 0.0
        fmt = Constants.XY_FORMAT
        s = ''

        zipFile.flush()

        if y == None or x == None :
            return
        if y.length <= 0 or x.length <= 0 :
            return
        if y.length > x.length :
            return

        if avgc < 1):
            avgc = 1

        #// print("y: %d x: %d avgc: %d\r\n", y.length, x.length, avgc)
        for (int i = 0 i < y.length i++):
            xs += x[i]
            ys += y[i]
            ns += 1
            if ns >= avgc):
                if i >= avgc):
                    zipFile.format(Constants.CRLF)
                s = String.format(fmt, xs / ns, ys / ns)
                zipFile.format(s.replace(",", "."))
                xs = 0.0
                ys = 0.0
                ns = 0.0
        if ns > 0):
            s = String.format(Constants.CRLF + fmt, xs / ns, ys / ns)
            zipFile.format(s.replace(",", "."))
            xs = 0.0
            ys = 0.0
            ns = 0.0
        zipFile.flush()

    def saveSignalData(ZipFormatter zipFile, Signal sig, String folder): # throws IOException :
        entryName = folder + sig.name + Constants.EXTENSION
        zipFile.putNextEntry(entryName)
        saveAvg = sig.getPropInteger(Constants.SAVE_AVG)
        if saveAvg < 10):
            saveAvg = 10
        #// print("saveAvg: %d\r\n", saveAvg)
        saveToZip(zipFile, sig.x.data, sig.y.data, saveAvg)
        zipFile.flush()
        zipFile.closeEntry()

    def saveSignalProp(ZipFormatter zipFile, Signal sig, String folder): # throws IOException, DevFailed :
        zipFile.flush()
        entryName = folder + Constants.PARAM + sig.name + Constants.EXTENSION
        zipFile.putNextEntry(entryName)
        zipFile.format("Name%s%s\r\n", Constants.PROP_VAL_DELIMETER, sig.fullName())
        zipFile.format("Shot%s%d\r\n", Constants.PROP_VAL_DELIMETER, sig.shot())
        propList = sig.getPropValList()
        if propList.length > 0):
            for (String prop : propList):
                // print("%s\r\n", prop)
                zipFile.format("%s\r\n", prop)
        zipFile.flush()
        zipFile.closeEntry()

    def saveSignalLog(Formatter logFile, Signal sig): # throws IOException, DevFailed :
        #// Get signal label = default mark name
        label = sig.getPropString(Constants.LABEL)
        #//print("label = %s\n", label)

        #// Get unit name
        unit = sig.getPropString(Constants.UNIT)
        #//print("unit = %s\n", unit)

        #// Get calibration coefficient for conversion to unit
        coeff = sig.getPropDouble(Constants.DISPLAY_UNIT)
        if coeff == 0.0):
            coeff = 1.0
        #//print("coeff = %g\n", coeff)

        List<Mark> marks = sig.getMarkList()

        // Find zero value
        double zero = 0.0
        for (Mark mark : marks:
            if Constants.ZERO_NAME.equals(mark.name):
                zero = mark.yValue
                //print("zero = %g\n", zero)
                break
            }
        }
        // Find all marks and log (mark - zero)*coeff
        for (Mark mark : marks:
            boolean firstLine = True
            if not Constants.ZERO_NAME.equals(mark.name):
                double logMarkValue = (mark.yValue - zero) * coeff

                String logMarkName = mark.name
                if logMarkName.equals(Constants.MARK_NAME):
                    logMarkName = label
                }
                //print(Constants.LOG_CONSOLE_FORMAT, logMarkName, logMarkValue, unit)
                if firstLine:
                    print("%7s ", sig.name())
                } else :
                    print("%7s ", "  ")
                }

                if Math.abs(logMarkValue) >= 1000.0:
                    print("%10s = %7.0f %s\n", logMarkName, logMarkValue, unit)
                } else if Math.abs(logMarkValue) >= 100.0:
                    print("%10s = %7.1f %s\n", logMarkName, logMarkValue, unit)
                } else if Math.abs(logMarkValue) >= 10.0:
                    print("%10s = %7.2f %s\n", logMarkName, logMarkValue, unit)
                } else :
                    print("%10s = %7.3f %s\n", logMarkName, logMarkValue, unit)
                }
                firstLine = False

                String fmt = Constants.LOG_DELIMETER + Constants.LOG_FORMAT
                logFile.format(fmt, logMarkName, logMarkValue, unit)
            }
        }

    def dumpADCDataAndLog(adc, zipFile, logFile, folder):
    	atts = adc.devProxy.get_attribute_info()
        retryCount = 0
        for i in range(len(atts)) :
            if atts[i].name.startswith("chany"):
                try :
                    Channel chan = Channel(adc, atts[i].name)
                    Boolean saveDataFlag = chan.getPropertyAsBoolean(Constants.SAVE_DATA)
                    Boolean saveLogFlag = chan.getPropertyAsBoolean(Constants.SAVE_LOG)
                    if saveDataFlag or saveLogFlag:
                        Signal sig = Signal(chan)
                        saveSignalProp(zipFile, sig, folder)
                        if saveDataFlag:
                            sig.readData()
                            saveSignalData(zipFile, sig, folder)
                        }
                        if saveLogFlag:
                            saveSignalLog(logFile, sig)
                        }
                    } // if save_auto or save_log is on
                    retryCount = 0
                } // try
                catch (Exception ex:
                    //print("Channel saving exception : " + ex )
                    //e.printStackTrace()
                    logFile.flush()
                    zipFile.flush()
                    zipFile.closeEntry()
                    retryCount++
                } // catch
                if retryCount > 0 && retryCount < 3:
                    print("Retry reading channel " + atts[i].name)
                    i--
                }
                if retryCount >= 3:
                    print("Error reading channel " + atts[i].name)

    def delay(self, ms):
        try :
            Thread.sleep(ms)
        except:
            pass

    def lockDir(self, folder) : #throws FileNotFoundException :
        lockFile = File(folder + "\\lock.lock")
        self.lockFileOS = FileOutputStream(lockFile)
        self.locked = True
        self.logger.log(logging.DEBUG, "Directory locked")

    def unlockDir(self) : # throws IOException :
        lockFileOS.close()
        lockFile.delete()
        locked = False
        self.logger.log(logging.DEBUG, "Directory unlocked")
    }


class Device :
    def __init__(self, host='', port=10000, dev='', avg=100, folder="ADC_0"):
        self.host = host
        self.port = port
        self.dev = dev
        self.folder = folder
        self.avg = avg
        self.active = False
        self.timeout = 0
        self.shot = -8888
        self.timeout = time.time()

    def getName(self:
        return self.host + ":" + self.port + "/" + self.dev


