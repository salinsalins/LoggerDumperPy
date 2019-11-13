import os
import os.path
import sys
import json
import logging
import datetime
import time
import zipfile

import numpy
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
progVersion = "3.0"
configFileName = progNameShort + ".json"

config = {}
devices_list = []


def print_exception_info(level=logging.DEBUG):
    logger.log(level, "Exception ", exc_info=True)


def convert_to_buf(x, y, avgc, fmt='%f; %f'):
        xs = 0.0
        ys = 0.0
        ns = 0.0
        fmt = fmt
        s = ''
        outbuf = ''

        if y is None or x is None :
            return outbuf
        if len(y) <= 0 or len(x) <= 0 :
            return outbuf
        n = len(y)
        if len(y) != len(x):
            if len(x) < n:
                n = len(x)
            logger.log(logging.WARNING, "X and Y arrays of different length, truncated to %d" % n)

        if avgc < 1:
            avgc = 1

        for i in range(n):
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


class DumperDevice:
    def __init__(self, device, name):
        self.dev = device
        self.name = name
        self.time = time.time()
        # tango related
        self.devProxy = None
        self.db = None
        self.attr = None
        self.prop = None
        # additional
        self.label = ''
        self.unit = ''
        self.coeff = 1.0
        self.fmt = '%6.2f'

    def get_property(self, prop):
        try:
            if self.prop is None:
                self.read_all_properties()
            ps = self.prop[prop][0]
            return ps
        except:
            return None

    def get_prop_as_boolean(self, prop):
        val = None
        try:
            prop_str = self.get_property(prop).lower()
            if prop_str == "true":
                val = True
            elif prop_str == "on":
                val = True
            elif prop_str == "1":
                val = True
            elif prop_str == "y":
                val = True
            elif prop_str == "yes":
                val = True
            else:
                val = False
            return val
        except:
            return None

    def get_prop_as_int(self, prop):
        try:
            return int(self.get_property(prop))
        except:
            return None

    def get_prop_as_float(self, prop):
        try:
            return float(self.get_property(prop))
        except:
            return None

    def read_all_properties(self):
        # read all properties
        ap = self.db.get_device_attribute_property(self.dev, self.name)
        self.prop = ap[self.name]
        return self.prop

    def read_attribute(self):
        self.attr = self.devProxy.read_attribute(self.name)
        self.time = time.time()

    def get_name(self):
        return "%s:%d/%s" % (self.host, self.port, self.name)

    def __str__(self):
        return self.get_name()

    def activate(self):
        if self.active:
            return True
        try:
            self.db = tango.Database()
            self.devProxy = tango.DeviceProxy(self.dev)
            self.time = time.time()
            self.active = True
            logger.log(logging.DEBUG, "Device %s activated" % self.dev)
        except:
            self.active = False
            self.time = time.time()
            logger.log(logging.ERROR, "Device %s activation error" % self.dev)
            print_exception_info()
        return self.active

    def new_shot(self):
        return False

    def convert_to_buf(self, avgc, y=None, x=None):
        outbuf = ''
        if avgc < 1:
            avgc = 1

        if x is None:
            # save only y values
            fmt = '%f'
            if y is None:
                y = self.attr.value
            n = len(y)
            ys = 0.0
            ns = 0.0
            for k in range(n):
                ys += y[k]
                ns += 1.0
                if ns >= avgc:
                    if k >= avgc:
                        outbuf += '\r\n'
                    s = fmt % (ys / ns)
                    outbuf += s.replace(",", ".")
                    ys = 0.0
                    ns = 0.0
            if ns > 0:
                outbuf += '\r\n'
                s = fmt % (ys / ns)
                outbuf += s.replace(",", ".")
        else:
            # save "x; y" pairs
            fmt = '%f; %f'
            if y is None:
                y = self.attr.value
            if y is None:
                return ''
            if len(y) <= 0 or len(x) <= 0 :
                return ''
            n = len(y)
            if len(x) < n:
                n = len(x)
                logger.log(logging.WARNING, "X and Y arrays of different length, truncated to %d" % n)
            xs = 0.0
            ys = 0.0
            ns = 0.0
            for k in range(n):
                xs += x[k]
                ys += y[k]
                ns += 1.0
                if ns >= avgc:
                    if k >= avgc:
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
        return outbuf

    def get_marks(self):
        if self.prop is None:
            self.read_all_properties()
        if self.attr is None:
            self.read_attribute()
        ml = {}
        for pk in self.prop:
            if pk.endswith("_start"):
                pn = pk.replace("_start", "")
                try:
                    pv = int(self.prop[pk][0])
                    pln = pk.replace("_start", "_length")
                    if pln in self.prop:
                        pl = int(self.prop[pln][0])
                    else:
                        pl = 1
                    ml[pn] = self.attr.value[pv:pv + pl].mean()
                except:
                    ml[pn] = 0.0
        return ml

    def save_log(self, log_file):
        try:
            if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
                v = self.attr.value
                if isinstance(v, (int, float, complex)) and not isinstance(v, bool):
                    v = self.fmt % (v * self.coeff)
                else:
                    v = str(v)
                outstr = ('; %s = ' + v + ' %s') % (self.label, self.unit)
                log_file.write(outstr)
            elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
                self.marks = self.get_marks()
                # find zero value
                zero = 0.0
                if "zero" in self.marks:
                    zero = self.marks["zero"]
                # convert all marks to mark_value = (mark - zero)*coeff
                for mark in self.marks:
                    first_line = True
                    # if it is not zero mark
                    if not "zero" == mark:
                        mark_value = (self.marks[mark] - zero) * self.coeff
                        mark_name = mark
                        # default mark renamed to label
                        if mark_name == "mark":
                            mark_name = self.label
                        # print mark name = value
                        if first_line:
                            print("%10s " % self.name, end='')
                            first_line = False
                        else:
                            print("%10s " % "  ", end='')
                        pmn = mark_name
                        if len(mark_name) > 14:
                            pmn = mark_name[:5] + '...' + mark_name[-6:]
                        if abs(mark_value) >= 1000.0:
                            print("%14s = %7.0f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        elif abs(mark_value) >= 100.0:
                            print("%14s = %7.1f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        elif abs(mark_value) >= 10.0:
                            print("%14s = %7.2f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        else:
                            print("%14s = %7.3f %s\r\n" % (pmn, mark_value, self.unit), end='')

                        outstr = ('; %s = ' + self.fmt + ' %s') % (mark_name, mark_value * self.coeff, self.unit)
                        log_file.write(outstr)
                if len(self.marks) <= 0:
                    v = (float(self.attr.value[0]) - zero) * self.coeff
                    outstr = ('; %s = ' + self.fmt + ' %s') % (self.name, v, self.unit)
                    log_file.write(outstr)
            else:
                return
        except:
            logger.log(logging.WARNING, "Log save error for %s" % self.get_name())

    def save_data(self, zip_file):
        entry = self.folder + "/" + self.name + ".txt"
        try:
            if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
                buf = str(self.attr.value)
            elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
                avg = self.get_prop_as_int("save_avg")
                if avg < 1:
                    avg = 1
                buf = self.convert_to_buf(avg)
            else:
                return

            zip_file.writestr(entry, buf)
        except:
            logger.log(logging.WARNING, "Data save error for %s" % self.get_name())

    def save_prop(self, zip_file):
        entry = self.folder + "/" + "param" + self.name + ".txt"
        buf = "attribute=%s\r\n" % self.get_name()
        for pr in self.prop:
            buf += '%s=%s\r\n' % (pr, self.prop[pr][0])
        zip_file.writestr(entry, buf)

    def save(self, log_file, zip_file):
        self.read_all_properties()
        # save_data and save_log flags
        self.sdf = self.get_prop_as_boolean("save_data")
        self.slf = self.get_prop_as_boolean("save_log")
        # force save if requested during attribute creation
        if self.force:
            self.sdf = True
            self.slf = True
        # do not save if both flags are False
        if not (self.sdf or self.slf):
            return
        # read attribute with retries
        rc = self.retry_count
        while rc > 0:
            try:
                self.read_attribute()
                break
            except:
                logger.log(logging.DEBUG, "Attribute %s read exception" % self.get_name())
                print_exception_info()
                rc -= 1
        if rc == 0:
            logger.log(logging.WARNING, "Retry count exceeded reading attribute %s" % self.get_name())
            self.active = False
            self.time = time.time()
            return

        if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
            logger.log(logging.DEBUG, "Scalar attribute %s" % self.name)
        elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
            logger.log(logging.DEBUG, "SPECRUM attribute %s" % self.name)
        else:
            logger.log(logging.WARNING, "Unsupported attribute format for %s" % self.name)
            raise ValueError

        # determine required attribute properties
        # attribute label
        self.label = self.get_property('label')
        if self.label is None or '' == self.label:
            self.label = self.get_property('name')
        if self.label is None or '' == self.label:
            self.label = self.name
        # units
        self.unit = self.get_property('unit')
        # calibration coefficient for conversion to units
        try:
            cf = self.get_property('display_unit')
            self.coeff = float(cf)
        except:
            self.coeff = 1.0
        if self.coeff is None:
            self.coeff = 1.0
        # format string
        self.fmt = self.get_property('format')
        if self.fmt is None or '' == self.fmt:
            self.fmt = '%6.2f'

        if self.sdf or self.slf:
            self.save_prop(zip_file)
        if self.slf:
            self.save_log(log_file)
        if self.sdf:
            self.save_data(zip_file)


class TestDevice:
    n = 0
    def __init__(self, delta_t=-1.0, points=0):
        self.n = TestDevice.n
        self.time = time.time()
        self.shot = 0
        self.active = False
        self.delta_t = delta_t
        self.points = points
        TestDevice.n += 1

    def get_name(self):
        return "TestDevice_%d" % self.n

    def __str__(self):
        return self.get_name()

    def activate(self):
        if self.active:
            return True
        self.active = True
        self.time = time.time()
        logger.log(logging.DEBUG, "TestDevice %d activated" % self.n)
        return True

    def new_shot(self):
        if self.delta_t >= 0.0 and (time.time() - self.time) > self.delta_t:
            self.shot += 1
            self.time = time.time()
            logger.log(logging.DEBUG, "TestDevice %d - New shot %d" % (self.n, self.shot))
            return True
        logger.log(logging.DEBUG, "TestDevice %d - No new shot" % self.n)
        return False

    def save(self, log_file, zip_file):
        logger.log(logging.DEBUG, "TestDevice %d - Save" % self.n)
        log_file.write('; TestDev_%d=%f'%(self.n, self.time))
        if self.points > 0:
            buf = ""
            for k in range(self.points):
                s = '%f; %f' % (float(k), numpy.sin(time.time()+float(self.n)+float(k)/100.0)+0.1*numpy.sin(time.time()+float(k)/5.0))
                buf += s.replace(",", ".")
                if k < self.points-1:
                    buf += '\r\n'
            entry = "TestDev/chanTestDev_%d.txt"%self.n
            zip_file.writestr(entry, buf)
            entry = "TestDev/paramchanTestDev_%d.txt"%self.n
            zip_file.writestr(entry, "name=TestDev_%d\r\nxlabel=Point number"%self.n)


class AdlinkADC:
    class Channel:
        def __init__(self, adc, name, x=None):
            self.dev = adc
            if type(name) is int:
                self.name = 'chany' + str(name)
            else:
                self.name = str(name)
            self.prop = None
            self.attr = None
            self.x_data = x

        def read_properties(self):
            # Read signal properties
            ap = self.dev.db.get_device_attribute_property(self.dev.name, self.name)
            self.prop = ap[self.name]
            return self.prop

        def read_data(self):
            self.attr = self.dev.devProxy.read_attribute(self.name)
            return self.attr.value

        def read_x_data(self):
            if not self.name.startswith('chany'):
                if self.attr is None:
                    self.read_data()
                # Generate 1 increment array as x
                self.x_data = numpy.arange(len(self.attr.value))
            else:
                self.x_data = self.dev.devProxy.read_attribute(self.name.replace('y', 'x')).value
            return self.x_data

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
            if self.x_data is None:
                self.read_x_data()
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
                        dx = self.x_data[1] - self.x_data[0]
                        n1 = int((pv - self.x_data[0]) / dx)
                        n2 = int((pv +  pl - self.x_data[0]) / dx)
                        ml[pn] = self.attr.value[n1:n2].mean()
                    except:
                        ml[pn] = 0.0
            return ml

    def __init__(self, host='192.168.1.41', port=10000, dev='binp/nbi/adc0', avg=100, folder="ADC_0", first=False):
        self.host = host
        self.port = port
        self.name = dev
        self.folder = folder
        self.avg = avg
        self.first = first
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
        if not self.active:
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
        if (not self.first) and (self.shot < 0):
            self.shot = ns
            return False
        if self.shot < ns:
            self.shot = ns
            self.x_data = None
            self.first = False
            return True
        return False

    def save_data(self, zip_file, chan):
        entry = chan.dev.folder + "/" + chan.name + ".txt"
        avg = chan.get_prop_as_int("save_avg")
        if avg < 1:
            avg = 1
        if chan.x_data is None or len(chan.x_data) != len(chan.attr.value):
            chan.x_data = chan.read_x_data()
        buf = convert_to_buf(chan.x_data, chan.attr.value, avg)
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

                format = chan.get_prop('format')
                if format is None or '' == format:
                    format = '%6.2f'
                outstr = "; %s = "%mark_name + format%mark_value + " %s"%unit
                log_file.write(outstr)

    def save(self, log_file, zip_file):
        atts = self.devProxy.get_attribute_list()
        self.x_data = None
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


class TangoAttribute:
    def __init__(self, device, attribute_name, folder=None, force=True):
        self.dev = device
        self.name = attribute_name
        self.folder = folder
        if folder is None:
            self.folder = "%s/%s" % (self.dev, self.name)
        self.force = force
        self.retry_count = 3
        self.active = False
        self.time = time.time()
        # tango related
        self.devProxy = None
        self.db = None
        self.attr = None
        self.prop = None
        # additional
        self.label = ''
        self.unit = ''
        self.coeff = 1.0
        self.fmt = '%6.2f'
        self.slf = False
        self.sdf = False

    def get_property(self, prop):
        try:
            if self.prop is None:
                self.read_all_properties()
            ps = self.prop[prop][0]
            return ps
        except:
            return None

    def get_prop_as_boolean(self, prop):
        val = None
        try:
            prop_str = self.get_property(prop).lower()
            if prop_str == "true":
                val = True
            elif prop_str == "on":
                val = True
            elif prop_str == "1":
                val = True
            elif prop_str == "y":
                val = True
            elif prop_str == "yes":
                val = True
            else:
                val = False
            return val
        except:
            return None

    def get_prop_as_int(self, prop):
        try:
            return int(self.get_property(prop))
        except:
            return None

    def get_prop_as_float(self, prop):
        try:
            return float(self.get_property(prop))
        except:
            return None

    def read_all_properties(self):
        # read all properties
        ap = self.db.get_device_attribute_property(self.dev, self.name)
        self.prop = ap[self.name]
        return self.prop

    def read_attribute(self):
        self.attr = self.devProxy.read_attribute(self.name)
        self.time = time.time()

    def get_name(self):
        return "%s/%s" % (self.dev, self.name)

    def __str__(self):
        return self.get_name()

    def activate(self):
        if self.active:
            return True
        try:
            self.db = tango.Database()
            self.devProxy = tango.DeviceProxy(self.dev)
            self.time = time.time()
            self.active = True
            logger.log(logging.DEBUG, "Device %s activated" % self.dev)
        except:
            self.active = False
            self.time = time.time()
            logger.log(logging.ERROR, "Device %s activation error" % self.dev)
            print_exception_info()
        return self.active

    def new_shot(self):
        return False

    def convert_to_buf(self, avgc, y=None, x=None):
        outbuf = ''
        if avgc < 1:
            avgc = 1

        if x is None:
            # save only y values
            fmt = '%f'
            if y is None:
                y = self.attr.value
            n = len(y)
            ys = 0.0
            ns = 0.0
            for k in range(n):
                ys += y[k]
                ns += 1.0
                if ns >= avgc:
                    if k >= avgc:
                        outbuf += '\r\n'
                    s = fmt % (ys / ns)
                    outbuf += s.replace(",", ".")
                    ys = 0.0
                    ns = 0.0
            if ns > 0:
                outbuf += '\r\n'
                s = fmt % (ys / ns)
                outbuf += s.replace(",", ".")
        else:
            # save "x; y" pairs
            fmt = '%f; %f'
            if y is None:
                y = self.attr.value
            if y is None:
                return ''
            if len(y) <= 0 or len(x) <= 0 :
                return ''
            n = len(y)
            if len(x) < n:
                n = len(x)
                logger.log(logging.WARNING, "X and Y arrays of different length, truncated to %d" % n)
            xs = 0.0
            ys = 0.0
            ns = 0.0
            for k in range(n):
                xs += x[k]
                ys += y[k]
                ns += 1.0
                if ns >= avgc:
                    if k >= avgc:
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
        return outbuf

    def get_marks(self):
        if self.prop is None:
            self.read_all_properties()
        if self.attr is None:
            self.read_attribute()
        ml = {}
        for pk in self.prop:
            if pk.endswith("_start"):
                pn = pk.replace("_start", "")
                try:
                    pv = int(self.prop[pk][0])
                    pln = pk.replace("_start", "_length")
                    if pln in self.prop:
                        pl = int(self.prop[pln][0])
                    else:
                        pl = 1
                    ml[pn] = self.attr.value[pv:pv + pl].mean()
                except:
                    ml[pn] = 0.0
        return ml

    def save_log(self, log_file):
        try:
            if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
                v = self.attr.value
                if isinstance(v, (int, float, complex)) and not isinstance(v, bool):
                    v = self.fmt % (v * self.coeff)
                else:
                    v = str(v)
                outstr = ('; %s = ' + v + ' %s') % (self.label, self.unit)
                log_file.write(outstr)
                print(outstr[1:])
            elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
                self.marks = self.get_marks()
                # find zero value
                zero = 0.0
                if "zero" in self.marks:
                    zero = self.marks["zero"]
                # convert all marks to mark_value = (mark - zero)*coeff
                for mark in self.marks:
                    first_line = True
                    # if it is not zero mark
                    if not "zero" == mark:
                        mark_value = (self.marks[mark] - zero) * self.coeff
                        mark_name = mark
                        # default mark renamed to label
                        if mark_name == "mark":
                            mark_name = self.label
                        # print mark name = value
                        if first_line:
                            print("%10s " % self.name, end='')
                            first_line = False
                        else:
                            print("%10s " % "  ", end='')
                        pmn = mark_name
                        if len(mark_name) > 14:
                            pmn = mark_name[:5] + '...' + mark_name[-6:]
                        if abs(mark_value) >= 1000.0:
                            print("%14s = %7.0f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        elif abs(mark_value) >= 100.0:
                            print("%14s = %7.1f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        elif abs(mark_value) >= 10.0:
                            print("%14s = %7.2f %s\r\n" % (pmn, mark_value, self.unit), end='')
                        else:
                            print("%14s = %7.3f %s\r\n" % (pmn, mark_value, self.unit), end='')

                        outstr = ('; %s = ' + self.fmt + ' %s') % (mark_name, mark_value * self.coeff, self.unit)
                        log_file.write(outstr)
                if len(self.marks) <= 0:
                    v = (float(self.attr.value[0]) - zero) * self.coeff
                    outstr = ('; %s = ' + self.fmt + ' %s') % (self.name, v, self.unit)
                    log_file.write(outstr)
            else:
                return
        except:
            logger.log(logging.WARNING, "Log save error for %s" % self.get_name())

    def save_data(self, zip_file:zipfile.ZipFile):
        entry = self.folder + "/" + self.name + ".txt"
        try:
            if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
                buf = str(self.attr.value)
            elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
                avg = self.get_prop_as_int("save_avg")
                if avg < 1:
                    avg = 1
                buf = self.convert_to_buf(avg)
            else:
                logger.log(logging.WARNING, "Unsupported attribute format for %s" % self.get_name())
                return
            try:
                info = zip_file.getinfo(entry)
                self.folder += ("_" + self.dev + '_' + str(time.time()))
                self.folder.replace('/', '_')
                self.folder.replace('.', '_')
                logger.log(logging.WARNING, "Duplicate entry %s in zip file. Folder is changed to %s." % (entry, self.folder))
                entry = self.folder + "/" + self.name + ".txt"
            except:
                pass
            zip_file.writestr(entry, buf)
        except:
            logger.log(logging.WARNING, "Attribute data save error for %s" % self.get_name())

    def save_prop(self, zip_file):
        entry = self.folder + "/" + "param" + self.name + ".txt"
        buf = "attribute=%s\r\n" % self.get_name()
        for pr in self.prop:
            buf += '%s=%s\r\n' % (pr, self.prop[pr][0])
        try:
            info = zip_file.getinfo(entry)
            self.folder += ("_" + self.dev + '_' + str(time.time()))
            self.folder.replace('/', '_')
            self.folder.replace('.', '_')
            logger.log(logging.WARNING,
                       "Duplicate entry %s in zip file. Folder is changed to %s." % (entry, self.folder))
            entry = self.folder + "/" + self.name + ".txt"
        except:
            pass
        zip_file.writestr(entry, buf)

    def save(self, log_file, zip_file):
        self.read_all_properties()
        # save_data and save_log flags
        self.sdf = self.get_prop_as_boolean("save_data")
        self.slf = self.get_prop_as_boolean("save_log")
        # force save if requested during attribute creation
        if self.force:
            self.sdf = True
            self.slf = True
        # do not save if both flags are False
        if not (self.sdf or self.slf):
            return
        # read attribute with retries
        rc = self.retry_count
        while rc > 0:
            try:
                self.read_attribute()
                self.time = time.time()
                break
            except:
                logger.log(logging.DEBUG, "Attribute %s read exception" % self.get_name())
                print_exception_info()
                rc -= 1
        if rc == 0:
            logger.log(logging.WARNING, "Retry count exceeded reading attribute %s" % self.get_name())
            self.active = False
            self.time = time.time()
            return

        if self.attr.data_format == tango._tango.AttrDataFormat.SCALAR:
            logger.log(logging.DEBUG, "Scalar attribute %s" % self.name)
        elif self.attr.data_format == tango._tango.AttrDataFormat.SPECTRUM:
            logger.log(logging.DEBUG, "SPECRUM attribute %s" % self.name)
        else:
            logger.log(logging.WARNING, "Unsupported attribute format for %s" % self.name)
            raise ValueError

        # determine required attribute properties
        # attribute label
        self.label = self.get_property('label')
        if self.label is None or '' == self.label:
            self.label = self.get_property('name')
        if self.label is None or '' == self.label:
            self.label = self.name
        # units
        self.unit = self.get_property('unit')
        # calibration coefficient for conversion to units
        try:
            cf = self.get_property('display_unit')
            self.coeff = float(cf)
        except:
            self.coeff = 1.0
        # format string
        self.fmt = self.get_property('format')
        if self.fmt is None or '' == self.fmt:
            self.fmt = '%6.2f'

        if self.sdf or self.slf:
            self.save_prop(zip_file)
        if self.slf:
            self.save_log(log_file)
        if self.sdf:
            self.save_data(zip_file)


class ShotDumper:
    def __init__(self):
        self.outFolder = ".\\data\\"
        self.lockFile = None
        self.locked = False
        self.shot = 0
        self.logFile = None
        self.zipFile = None

    def read_config(self, file_name=configFileName):
        global config
        global devices_list
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
            if 'sleep' not in config:
                config["sleep"] = 1.0
            # Read output directory
            if 'outDir' in config:
                self.outRootDir = config["outDir"]
            if 'shot' in config:
                self.shot = config['shot']
            # Restore devices
            if 'devices' not in config:
                logger.log(logging.WARNING, "No devices declared")
                devices_list = []
                return
            items = config["devices"]
            if len(items) <= 0:
                logger.log(logging.WARNING, "No devices declared")
                return
            for unit in items:
                try:
                    if 'exec' in unit:
                        exec(unit["exec"])
                    if 'eval' in unit:
                        item = eval(unit["eval"])
                        devices_list.append(item)
                        logger.log(logging.DEBUG, "Device %s added" % str(unit["eval"]))
                    else:
                        logger.log(logging.DEBUG, "No 'eval' option for device %s" % unit)

                except:
                    logger.log(logging.WARNING, "Error in device processing %s" % str(unit))
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
        global devices_list

        self.logFile = None
        self.zipFile = None

        # Activate items in item_list
        count = 0   # Active item count
        n = 0
        for item in devices_list :
            try:
                if item.activate():
                    count += 1
            except:
                devices_list.remove(item)
                logger.log(logging.ERROR, "Device %d removed from list due to activation error" % n)
                print_exception_info()
            n += 1
        if count <= 0 :
            logger.log(logging.CRITICAL, "No active devices")
            return
        # main loop
        print("%s Waiting for next shot ..." % self.time_stamp())
        while True :
            try :
                new_shot = False
                for item in devices_list:
                    try :
                        # reactivate all items
                        item.activate()
                        # check for new shot
                        if item.new_shot():
                            new_shot = True
                            #break
                    except:
                        devices_list.remove(item)
                        logger.log(logging.ERROR, "Device %d removed from list due to activation error" % n)
                        print_exception_info()

                if new_shot:
                    dts = self.date_time_stamp()
                    self.shot += 1
                    config['shot'] = self.shot
                    config['shot_time'] = dts
                    print("\n%s New Shot %d" % (dts, self.shot))
                    self.make_log_folder()
                    if self.locked:
                        logger.log(logging.WARNING, "Unexpected lock")
                        self.zipFile.close()
                        self.logFile.close()
                        self.unlock_dir()
                    self.lock_dir(self.outFolder)
                    self.logFile = self.open_log_file(self.outFolder)
                    # Write date and time
                    self.logFile.write(dts)
                    # Write shot number
                    self.logFile.write('; Shot=%d' % self.shot)
                    # Open zip file
                    self.zipFile = self.open_zip_file(self.outFolder)
                    for item in devices_list:
                        print("Saving from %s"%item.get_name())
                        try:
                            item.save(self.logFile, self.zipFile)
                        except:
                            logger.log(logging.WARNING, "Exception saving data from %s"%str(item))
                            print_exception_info()
                    self.zipFile.close()
                    zfn = os.path.basename(self.zipFile.filename)
                    self.logFile.write('; File=%s' % zfn)
                    self.logFile.write('\n')
                    self.logFile.close()
                    self.unlock_dir()
                    self.write_config()
                    print("%s Waiting for next shot ..." % self.time_stamp())
            except:
                logger.log(logging.CRITICAL, "Unexpected exception")
                print_exception_info()
                return
            time.sleep(config['sleep'])

    def make_log_folder(self):
        of = os.path.join(self.outRootDir, self.get_log_folder())
        try:
            if not os.path.exists(of):
                os.makedirs(of)
                logger.log(logging.DEBUG, "Output folder %s has been created", self.outFolder)
            self.outFolder = of
            return True
        except:
            logger.log(logging.CRITICAL, "Can not create output folder %s", self.outFolder)
            self.outFolder = None
            return False

    def get_log_folder(self):
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
