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
#console_handler.setLevel(logging.WARNING)
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

    def read_x_data(self):
        if not self.name.startswith('chany'):
            self.xvalue = np.arange(len(self.attr.value))
        else:
            self.xvalue = self.dev.devProxy.read_attribute(self.name.replace('y', 'x')).value
        return self.xvalue

    def get_prop_as_boolean(self, propName):
        propVal = None
        try:
            propString = self.get_prop(propName).lower()
            if propString == "true":
                propVal = True
            elif propString == "on":
                propVal = True
            elif propString == "1":
                propVal = True
            elif propString == "y":
                propVal = True
            elif propString == "yes":
                propVal = True
            else:
                propVal = False
            return propVal
        except:
            return propVal

    def get_prop_as_int(self, propName):
        try:
            return int(self.get_prop(propName))
        except:
            return None

    def get_prop_as_float(self, propName):
        try:
            return float(self.get_prop(propName))
        except:
            return None

    def get_prop(self, propName):
        try:
            if self.prop is None:
                self.read_properties()
            ps = self.prop[propName][0]
            return ps
        except:
            return None

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

    def restore_settings(self):
        # no command line parameters
        if len(sys.argv) <= 1:
            logger.log(logging.DEBUG, "No command line config")
            self.read_config()
            return
        # first command line parameter - config file
        if sys.argv[1].endswith(".json"):
            self.configFileName = sys.argv[1]
            self.read_config()
            return
        # host port device averaging in command line parameters
        logger.log(logging.DEBUG, "Reading config from command line")
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
        logger.log(logging.DEBUG, "ADC %s was added to config" % d.get_name())

    def read_config(self, folder=''):
        fullName = os.path.join(str(folder), self.configFileName)
        logger.log(logging.DEBUG, "Reading config from %s" % fullName)
        try :
            # read config from file
            with open(fullName, 'r') as configfile:
                s = configfile.read()
            self.conf = json.loads(s)

            # restore log level
            v = logging.DEBUG
            if 'Loglevel' in self.conf:
                v = self.conf['Loglevel']
            logger.setLevel(v)
            logger.log(logging.DEBUG, "Log level set to %d" % v)

            # read output directory
            if 'outDir' in self.conf:
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
                try:
                    d.host = self.conf[section]["host"]
                    d.port = self.conf[section]["port"]
                    d.dev = self.conf[section]["device"]
                    d.folder = self.conf[section]["folder"]
                    d.avg = self.conf[section]["avg"]
                except:
                    pass
                self.devList.append(d)
                logger.log(logging.DEBUG, "ADC %s was added to config" % d.get_name())

            # print OK message and exit
            logger.info('Configuration restored from %s' % fullName)
            return True
        except :
            # print error info
            self.print_exception_info()
            logger.info('Configuration restore error from %s' % fullName)
            return False

    def print_exception_info(self):
        logger.error("Exception ", exc_info=True)

    def process(self) :
        self.logFile = None
        self.zipFile = None

        if len(self.devList) <= 0 :
            logger.log(logging.CRITICAL, "No ADC found")
            return

        # activate AdlinkADC in deviceList
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

        nshot = 0

        while True :
            try :
                for d in self.devList:
                    try :
                        if not d.active:
                            if d.timeout > time.time():
                                continue
                            d.init()
                            logger.log(logging.DEBUG, "ADC %s was activated", d.fullName())
                        nshot = d.read_shot()
                        if nshot <= d.shot:
                            if self.locked:
                                continue
                            else:
                                break

                        d.shot = nshot
                        print("\n%s New Shot %d\n" % (self.time_stamp(), nshot))
                        if not self.locked:
                            self.make_folder()
                            self.lock_dir(self.outFolder)
                            self.logFile = self.open_log_file(self.outFolder)
                            # Write date and time
                            self.logFile.write(self.date_time_stamp())
                            # Write shot number
                            self.logFile.write('; Shot=%5d' % nshot)
                            # Open zip file
                            self.zipFile = self.open_zip_file(self.outFolder)

                        print("Saving from ADC " + d.get_name())
                        self.save_data_and_log(d, self.zipFile, self.logFile)
                    except :
                        d.active = False
                        d.timeout = time.time() + 10000
                        logger.log(logging.INFO, "ADC %s is inactive, 10 seconds timeout", d.get_name())

                if self.locked:
                    self.zipFile.close()
                    # Write zip file name
                    zfn = os.path.basename(self.zipFile.filename)
                    self.logFile.write('; File=%s' % zfn)
                    self.logFile.write('\n')
                    self.logFile.close()
                    self.unlock_dir()
                    print("\n%s Waiting for next shot ..." % self.time_stamp())
            except:
                logger.log(logging.CRITICAL, "Unexpected exception")
                self.print_exception_info()
                return
            time.sleep(1)

    def make_folder(self):
        self.outFolder = os.path.join(self.outRootDir, self.get_log_folder_name())
        try:
            if not os.path.exists(self.outFolder):
                os.makedirs(self.outFolder)
                logger.log(logging.DEBUG, "Folder %s has been created", self.outFolder)
                return True
        except:
            self.outFolder = None
            logger.log(logging.CRITICAL, "Can not create output folder %s", self.outFolder)
            return False

    def get_log_folder_name(self):
        ydf = datetime.datetime.today().strftime('%Y')
        mdf = datetime.datetime.today().strftime('%Y-%m')
        ddf = datetime.datetime.today().strftime('%Y-%m-%d')
        folder = os.path.join(ydf, mdf, ddf)
        return folder

    def lock_dir(self, folder):
        self.lockFile = open(os.path.join(folder, "lock.lock"), 'w+')
        self.locked = True
        logger.log(logging.DEBUG, "Directory %s locked", folder)

    def open_log_file(self, folder=''):
        self.logFileName = os.path.join(folder, self.get_log_file_name())
        logf = open(self.logFileName, 'a')
        return logf

    def get_log_file_name(self):
        logfn = datetime.datetime.today().strftime('%Y-%m-%d.log')
        return logfn

    def date_time_stamp(self):
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    def open_zip_file(self, folder):
        fn = datetime.datetime.today().strftime('%Y-%m-%d_%H%M%S.zip')
        zip_file_name = os.path.join(folder, fn)
        zip_file = zipfile.ZipFile(zip_file_name, 'a', compression=zipfile.ZIP_DEFLATED)
        return zip_file

    def unlock_dir(self):
        self.lockFile.close()
        os.remove(self.lockFile.name)
        self.locked = False
        logger.log(logging.DEBUG, "Directory unlocked")

    def time_stamp(self):
        return datetime.datetime.today().strftime('%H:%M:%S')

    def save_data_and_log(self, adc, zip_file, log_file):
        atts = adc.devProxy.get_attribute_list()
        #retry_count = 0
        for a in atts:
            if a.startswith("chany"):
                retry_count = 3
                while retry_count > 0:
                    try :
                        chan = Channel(adc, a)
                        # read save_data and save_log flags
                        sdf = chan.get_prop_as_boolean(Constants.SAVE_DATA)
                        slf = chan.get_prop_as_boolean(Constants.SAVE_LOG)
                        # save signal properties
                        if sdf or slf:
                            self.save_prop(zip_file, chan)
                            if sdf:
                                chan.read_data()
                                self.save_data(zip_file, chan)
                            if slf:
                                self.save_log(log_file, chan)
                        retry_count = -1
                    except:
                        self.print_exception_info()
                        self.logFile.flush()
                        #self.zipFile.close()
                        retry_count -= 1
                    if retry_count > 0:
                        print("Retry reading channel %s" % a.name)
                    if retry_count == 0:
                        print("Error reading channel %s" % a.name)

    def convert_to_buf(self, x, y, avgc):
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

    def save_data(self, zip_file, chan):
        entry = chan.dev.folder + "/" + chan.name + Constants.EXTENSION
        avg = chan.get_prop_as_int(Constants.SAVE_AVG)
        if avg < 1:
            avg = 1
        #// print("saveAvg: %d\r\n", saveAvg)
        buf = self.convert_to_buf(chan.read_x_data(), chan.attr.value, avg)
        zip_file.writestr(entry, buf)

    def save_prop(self, zip_file, chan):
        entry = chan.dev.folder + "/" + Constants.PARAM + chan.name + Constants.EXTENSION
        buf = "Signal_Name=%s/%s\r\n" % (chan.dev.get_name(), chan.name)
        buf += "Shot=%d\r\n" % chan.dev.shot
        prop_list = ['%s=%s'%(k, chan.prop[k][0]) for k in chan.prop]
        for prop in prop_list:
            buf += "%s\r\n" % prop
        zip_file.writestr(entry, buf)

    def save_log(self, log_file, chan):
        # Signal label = default mark name
        label = chan.get_prop('label')
        if label is None or '' == label:
            label = chan.get_prop('name')
        if label is None or '' == label:
            label = chan.name
        # Units
        unit = chan.get_prop('unit')
        # Calibration coefficient for conversion to units
        coeff = chan.get_prop_as_float(Constants.DISPLAY_UNIT)
        if coeff is None or coeff == 0.0:
            coeff = 1.0

        marks = chan.get_marks()

        # Find zero value
        zero = 0.0
        if Constants.ZERO_NAME in marks:
            zero = marks[Constants.ZERO_NAME]

        # Convert all marks to mark_value = (mark - zero)*coeff
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
        lgd.restore_settings()
        lgd.process()
    except:
        lgd.logger.log(logging.CRITICAL, "Exception in LoggerDumper")
        lgd.print_exception_info()