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

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                       datefmt='%H:%M:%S')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

progName = "PyTango Shot Dumper"
progNameShort = "ShotDumperPy"
progVersion = "2.0"
configFileName = progNameShort + ".json"

config = {}
item_list = []

def print_exception_info(level=logging.DEBUG):
    logger.log(level, "Exception ", exc_info=True)


class TestDevice:
    n = 0
    def __init__(self, delta_t=-1.0):
        self.n = TestDevice.n
        self.time = time.time()
        self.shot = 0
        self.active = False
        self.delta_t = delta_t
        TestDevice.n += 1

    def get_name(self):
        return "TestDevice_%d" % self.n

    def __str__(self):
        return self.get_name()

    def activate(self):
        self.active = True
        self.time = time.time()
        logger.log(logging.DEBUG, "TestDev %n activated" % self.n)
        return True

    def new_shot(self):
        if self.delta_t >= 0.0 and (time.time() - self.time) > self.delta_t:
            self.shot += 1
            self.time = time.time()
            logger.log(logging.DEBUG, "TestDev %n - New shot %d" % (self.n, self.shot))
            return True
        logger.log(logging.DEBUG, "TestDev %n - No new shot" % self.n)
        return False

    def save(self, log_file, zip_file):
        logger.log(logging.DEBUG, "TestDev %n - Save" % self.n)
        log_file.write('TestDev_%d=%f', (self.n, self.time))


class AdlinkADC:
    class Channel:
        def __init__(self, adc, name):
            self.dev = adc
            if type(name) is int:
                self.name = 'chany' + str(name)
            else:
                self.name = name
            self.prop = None
            self.attr = None
            self.xvalue = None

        def read_properties(self):
            # Read signal properties
            ap = self.dev.db.get_device_attribute_property(self.dev.name, self.name)
            self.prop = ap[self.name]

        def read_data(self):
            self.attr = self.dev.devProxy.read_attribute(self.name)
            return self.attr.value

        def read_x_data(self):
            if not self.name.startswith('chany'):
                self.read_data()
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
            if self.prop is None:
                self.read_properties()
            if self.attr is None:
                self.read_data()
            ml = {}
            for pk in self.prop:
                if pk.endswith("_start"):
                    pn = pk.replace("_start", "")
                    try:
                        pv = int(self.prop[pk][0])
                        pln = pn + "_length"
                        if pln in self.prop:
                            pl = int(self.prop[pln][0])
                        else:
                            pl = 1
                        ml[pn] = self.attr.value[pv:pv + pl].mean()
                    except:
                        ml[pn] = 0.0
            return ml

    def __init__(self, host='192.168.1.41', port=10000, dev='binp/nbi/adc0', avg=100, folder="ADC_0"):
        self.host = host
        self.port = port
        self.name = dev
        self.folder = folder
        self.avg = avg
        self.active = False
        self.shot = -1
        self.timeout = time.time()
        self.devProxy = None
        self.db = None
        self.x_data = None

    def get_name(self):
        return "%s:%d/%s" % (self.host, self.port, self.name)

    def __str__(self):
        return self.get_name()

    def activate(self):
        try:
            self.db = tango.Database()
            self.devProxy = tango.DeviceProxy(self.get_name())
            self.active = True
            logger.log(logging.DEBUG, "ADC %s activated" % self.get_name())
        except:
            self.active = False
            self.timeout = time.time() + 10000
            logger.log(logging.ERROR, "ADC %s activation error" % self.get_name())
        return self.active

    def read_shot(self):
        try:
            da = self.devProxy.read_attribute("Shot_id")
            shot = da.value
            return shot
        except:
            return -1

    def new_shot(self):
        ns = self.read_shot()
        if self.shot < ns:
            self.shot = ns
            self.x_data = None
            return True
        return False

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
        entry = chan.dev.folder + "/" + chan.name + ".txt"
        avg = chan.get_prop_as_int("save_avg")
        if avg < 1:
            avg = 1
        if self.x_data is None:
            self.x_data = chan.read_x_data()
        buf = self.convert_to_buf(self.x_data, chan.attr.value, avg)
        zip_file.writestr(entry, buf)

    def save_prop(self, zip_file, chan):
        entry = chan.dev.folder + "/" + "param" + chan.name + ".txt"
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
        coeff = chan.get_prop_as_float("display_unit")
        if coeff is None or coeff == 0.0:
            coeff = 1.0

        marks = chan.get_marks()

        # Find zero value
        zero = 0.0
        if "zero" in marks:
            zero = marks["zero"]
        # Convert all marks to mark_value = (mark - zero)*coeff
        for mark in marks:
            first_line = True
            # If it is not zero mark
            if not "zero" == mark:
                mark_value = (marks[mark] - zero) * coeff
                mark_name = mark
                # Default mark renamed to label
                if mark_name == "mark":
                    mark_name = label
                # Print mark name = value
                if first_line:
                    print("%10s " % chan.name, end='')
                    first_line = False
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

                fmt = "; %s = %7.3f %s"
                log_file.write(fmt % (mark_name, mark_value, unit))

    def save(self, log_file, zip_file):
        atts = self.devProxy.get_attribute_list()
        self.x_data = None
        # Retry_count = 0
        for a in atts:
            if a.startswith("chany"):
                retry_count = 3
                while retry_count > 0:
                    try :
                        chan = AdlinkADC.Channel(self, a)
                        # Read save_data and save_log flags
                        sdf = chan.get_prop_as_boolean("save_data")
                        slf = chan.get_prop_as_boolean("save_log")
                        # Save signal properties
                        if sdf or slf:
                            self.save_prop(zip_file, chan)
                            if sdf:
                                chan.read_data()
                                self.save_data(zip_file, chan)
                            if slf:
                                self.save_log(log_file, chan)
                        break
                    except:
                        logger.log(logging.WARNING, "Adlink %s data save exception" % self.get_name())
                        print_exception_info()
                        retry_count -= 1
                    if retry_count > 0:
                        logger.log(logging.DEBUG, "Retry reading channel %s" % self.get_name())
                    if retry_count == 0:
                        logger.log(logging.WARNING, "Error reading channel %s" % self.get_name())


class ShotDumper:
    def __init__(self):
        self.outRootDir = ".\\data\\"
        self.outFolder = ".\\data\\"
        self.devList = []
        self.lockFile = None
        self.locked = False
        self.shot = 0

        self.device_list = []

    def read_config(self, file_name=configFileName):
        global config
        global item_list
        try :
            # Read config from file
            with open(file_name, 'r') as configfile:
                s = configfile.read()
            config = json.loads(s)
            # Restore log level
            try:
                logger.setLevel(config['Loglevel'])
            except:
                logger.setLevel(logging.DEBUG)
            logger.log(logging.DEBUG, "Log level set to %d" % logger.level)
            # Read output directory
            if 'outDir' in config:
                self.outRootDir = config["outDir"]
            if 'shot' in config:
                self.shot = config['shot']
            # Restore devices
            if 'devices' not in config:
                logger.log(logging.WARNING, "No elements declared")
                item_list = []
                return
            item_list = config["devices"]
            if len(item_list) <= 0:
                logger.log(logging.WARNING, "No elements declared")
                return
            for d in item_list:
                try:
                    if 'import' in d:
                        exec(d["import"])
                    item = eval(d["init"])
                    item_list.append(item)
                    logger.log(logging.DEBUG, "Element %s added to list" % str(d))
                except:
                    logger.log(logging.WARNING, "Error in element %s processing" % str(d))
                    print_exception_info()
            logger.info('Configuration restored from %s' % file_name)
            return True
        except :
            logger.info('Configuration restore error from %s' % file_name)
            print_exception_info()
            return False

    def write_config(self, file_name=configFileName):
        global config
        try :
            config['shot'] = self.shot
            with open(file_name, 'w') as configfile:
                configfile.write(json.dumps(config, indent=4))
            logger.info('Configuration saved to %s' % file_name)
        except :
            logger.info('Configuration save error to %s' % file_name)
            print_exception_info()
            return False

    def process(self) :
        self.logFile = None
        self.zipFile = None

        # Activate items in item_list
        # Active item count
        count = 0
        n = 0
        for item in item_list :
            try :
                if item.activate():
                    count += 1
            except:
                item_list.remove(item)
                logger.log(logging.ERROR, "Element %d removed from list due to activation error" % n)
                print_exception_info()
            n += 1
        if count <= 0 :
            logger.log(logging.CRITICAL, "No active elements")
            return
        # Main loop
        while True :
            try :
                new_shot = False
                n = 0
                for item in item_list:
                    try :
                        # Reactivate all items
                        item.activate()
                        if item.check_new_shot():
                            new_shot = True
                            break
                    except:
                        item_list.remove(item)
                        logger.log(logging.ERROR, "Element %d removed from list due to activation error" % n)
                        print_exception_info()
                    n += 1
                if new_shot:
                    self.shot += 1
                    print("\n%s New Shot %d" % (self.time_stamp(), self.shot))
                    if not self.locked:
                        self.make_folder()
                        self.lock_dir(self.outFolder)
                        self.logFile = self.open_log_file(self.outFolder)
                    else:
                        logger.log(logging.WARNING, "Unexpected lock")
                        self.zipFile.close()
                        self.logFile.close()
                        self.unlock_dir()

                    # Write date and time
                    self.logFile.write(self.date_time_stamp())
                    # Write shot number
                    self.logFile.write('; Shot=%6d' % self.shot)
                    # Open zip file
                    self.zipFile = self.open_zip_file(self.outFolder)
                    for item in item_list:
                        print("Saving from %s", item.get_name())
                        item.save(self.logFile, self.zipFile)
                        ##self.logFile.flush()
                        ##self.zipFile.flush()
                    self.zipFile.close()
                    # Write zip file name
                    zfn = os.path.basename(self.zipFile.filename)
                    self.logFile.write('; File=%s' % zfn)
                    self.logFile.write('\n')
                    self.logFile.close()
                    self.unlock_dir()
                    print("%s Waiting for next shot ..." % self.time_stamp())
            except:
                logger.log(logging.CRITICAL, "Unexpected exception")
                print_exception_info()
                return
            self.write_config()
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

    def time_stamp(self):
        return datetime.datetime.today().strftime('%H:%M:%S')

    def open_zip_file(self, folder):
        fn = datetime.datetime.today().strftime('%Y-%m-%d_%H%M%S.zip')
        zip_file_name = os.path.join(folder, fn)
        zip_file = zipfile.ZipFile(zip_file_name, 'a', compression=zipfile.ZIP_DEFLATED)
        return zip_file

    def unlock_dir(self):
        if self.lockFile is not None:
           self.lockFile.close()
           os.remove(self.lockFile.name)
        self.locked = False
        logger.log(logging.DEBUG, "Directory unlocked")


if __name__ == '__main__':
    sd = ShotDumper()
    try:
        sd.read_config()
        sd.process()
    except:
        logger.log(logging.CRITICAL, "Exception in %s", progNameShort)
        print_exception_info()
