import sys
import tango

class LoggerDumper:
    def __init__(self):
        # configure logging
        self.progName = "Adlink DAQ-2204 PyTango Logger"
        self.progNameShort = "LoggerDumperPy"
        self.progVersion = "0.1"
        self.iniFileName = self.progNameShort + ".ini"
        self.outRootDir = ".\\data\\"
        self.outFolder = ".\\data\\"
        self.devList = []
        self.lockFile = "lock.lock"
        self.locked = False

    def readConfig(self):
        self.devList = []
        if (len(sys.argv) <= 1):
            self.readConfigFromIni()
            return
        if (sys.argv[1].endswith(".ini")):
            self.iniFileName = sys.argv[1]
            self.readConfigFromIni()
            return

        d = Device()
        try:
            d.folder = "ADC_0"
            d.host = sys.argv[1]
            d.port = sys.argv[2]
            d.dev = sys.argv[3]
            d.avg = int(sys.argv[4])
            outRootDir = sys.argv[5]
        except:
            pass

        self.devList.append(d)
        LOGGER.log(Level.FINE, "Added {0}", d.getName());
}



if __name__ == '__main__':
    lgd = LoggerDumper()
    try:
        lgd.readConfig()
        lgd.process()
    except:
        LOGGER.log(Level.SEVERE, "Exception in LoggerDumper")
        LOGGER.log(Level.INFO, "Exception info", ex)




    def makeFolder(self):
        if (not outRootDir.endsWith("\\")):
            outRootDir = outRootDir + "\\";
        outFolder = outRootDir + getLogFolderName();
        file = File(outFolder);
        if (file.mkdirs()):
            LOGGER.log(Level.FINE, "Folder {0} created", outFolder);
            return True;
        else:
            LOGGER.log(Level.SEVERE, "Output folder {0} not created", outFolder);
            return False;

    def getLogFolderName(self):
        ydf = new SimpleDateFormat("yyyy");
        mdf = new SimpleDateFormat("yyyy-MM");
        ddf = new SimpleDateFormat("yyyy-MM-dd");
        now = new Date();
        folder = ydf.format(now) + "\\" + mdf.format(now) + "\\" + ddf.format(now);
        return folder

    def getLogFileName(self):
        now = new Date()
        dayFmt = new SimpleDateFormat("yyyy-MM-dd");
        logFileName = dayFmt.format(now) + ".log";
        return logFileName

    def openLogFile(self, String folder): #throws IOException {
        logFileName = folder + "\\" + getLogFileName()
        fw = FileWriter(logFileName, true)
        logFile = Formatter(fw);
        return logFile

    def dateTimeStamp(self):
        return dateTimeStamp(new Date())

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
        #//System.out.printf("Created file %s\r\n", zipFileName);
        return zipFile

    def saveToZip(ZipFormatter zipFile, double[] x, double[] y, int avgc): # throws IOException {
        xs = 0.0
        ys = 0.0
        ns = 0.0
        fmt = Constants.XY_FORMAT
        s = ''

        zipFile.flush()

        if (y == null or x == null):
            return
        if (y.length <= 0 or x.length <= 0):
            return
        if (y.length > x.length):
            return

        if (avgc < 1):
            avgc = 1

        #// System.out.printf("y: %d x: %d avgc: %d\r\n", y.length, x.length, avgc);
        for (int i = 0; i < y.length; i++):
            xs += x[i]
            ys += y[i]
            ns += 1
            if (ns >= avgc):
                if (i >= avgc):
                    zipFile.format(Constants.CRLF)
                s = String.format(fmt, xs / ns, ys / ns);
                zipFile.format(s.replace(",", "."));
                xs = 0.0;
                ys = 0.0;
                ns = 0.0;
        if (ns > 0):
            s = String.format(Constants.CRLF + fmt, xs / ns, ys / ns);
            zipFile.format(s.replace(",", "."));
            xs = 0.0;
            ys = 0.0;
            ns = 0.0;
        zipFile.flush();

    def saveSignalData(ZipFormatter zipFile, Signal sig, String folder): # throws IOException {
        entryName = folder + sig.name + Constants.EXTENSION
        zipFile.putNextEntry(entryName);
        saveAvg = sig.getPropInteger(Constants.SAVE_AVG);
        if (saveAvg < 10):
            saveAvg = 10
        #// System.out.printf("saveAvg: %d\r\n", saveAvg);
        saveToZip(zipFile, sig.x.data, sig.y.data, saveAvg);
        zipFile.flush();
        zipFile.closeEntry();

    def saveSignalProp(ZipFormatter zipFile, Signal sig, String folder): # throws IOException, DevFailed {
        zipFile.flush()
        entryName = folder + Constants.PARAM + sig.name + Constants.EXTENSION;
        zipFile.putNextEntry(entryName);
        zipFile.format("Name%s%s\r\n", Constants.PROP_VAL_DELIMETER, sig.fullName());
        zipFile.format("Shot%s%d\r\n", Constants.PROP_VAL_DELIMETER, sig.shot());
        propList = sig.getPropValList();
        if (propList.length > 0):
            for (String prop : propList):
                // System.out.printf("%s\r\n", prop);
                zipFile.format("%s\r\n", prop);
        zipFile.flush();
        zipFile.closeEntry();

    def saveSignalLog(Formatter logFile, Signal sig): # throws IOException, DevFailed {
        #// Get signal label = default mark name
        label = sig.getPropString(Constants.LABEL);
        #//System.out.printf("label = %s\n", label);

        #// Get unit name
        unit = sig.getPropString(Constants.UNIT);
        #//System.out.printf("unit = %s\n", unit);

        #// Get calibration coefficient for conversion to unit
        coeff = sig.getPropDouble(Constants.DISPLAY_UNIT);
        if (coeff == 0.0):
            coeff = 1.0
        #//System.out.printf("coeff = %g\n", coeff);

        List<Mark> marks = sig.getMarkList();

        // Find zero value
        double zero = 0.0;
        for (Mark mark : marks) {
            if (Constants.ZERO_NAME.equals(mark.name)) {
                zero = mark.yValue;
                //System.out.printf("zero = %g\n", zero);
                break;
            }
        }
        // Find all marks and log (mark - zero)*coeff
        for (Mark mark : marks) {
            boolean firstLine = true;
            if (!Constants.ZERO_NAME.equals(mark.name)) {
                double logMarkValue = (mark.yValue - zero) * coeff;

                String logMarkName = mark.name;
                if (logMarkName.equals(Constants.MARK_NAME)) {
                    logMarkName = label;
                }
                //System.out.printf(Constants.LOG_CONSOLE_FORMAT, logMarkName, logMarkValue, unit);
                if (firstLine) {
                    System.out.printf("%7s ", sig.name());
                } else {
                    System.out.printf("%7s ", "  ");
                }

                if (Math.abs(logMarkValue) >= 1000.0) {
                    System.out.printf("%10s = %7.0f %s\n", logMarkName, logMarkValue, unit);
                } else if (Math.abs(logMarkValue) >= 100.0) {
                    System.out.printf("%10s = %7.1f %s\n", logMarkName, logMarkValue, unit);
                } else if (Math.abs(logMarkValue) >= 10.0) {
                    System.out.printf("%10s = %7.2f %s\n", logMarkName, logMarkValue, unit);
                } else {
                    System.out.printf("%10s = %7.3f %s\n", logMarkName, logMarkValue, unit);
                }
                firstLine = false;

                String fmt = Constants.LOG_DELIMETER + Constants.LOG_FORMAT;
                logFile.format(fmt, logMarkName, logMarkValue, unit);
            }
        }

    public void dumpADCDataAndLog(AdlinkADC adc, ZipFormatter zipFile, Formatter logFile, String folder)
            throws IOException, DevFailed {
    	AttributeInfo[] atts = adc.devProxy.get_attribute_info();
        int retryCount = 0;
        for (int i = 0; i < atts.length; i++) {
            if (atts[i].name.startsWith("chany")) {
                try {
                    Channel chan = new Channel(adc, atts[i].name);
                    Boolean saveDataFlag = chan.getPropertyAsBoolean(Constants.SAVE_DATA);
                    Boolean saveLogFlag = chan.getPropertyAsBoolean(Constants.SAVE_LOG);
                    if (saveDataFlag || saveLogFlag) {
                        Signal sig = new Signal(chan);
                        saveSignalProp(zipFile, sig, folder);
                        if (saveDataFlag) {
                            sig.readData();
                            saveSignalData(zipFile, sig, folder);
                        }
                        if (saveLogFlag) {
                            saveSignalLog(logFile, sig);
                        }
                    } // if save_auto || save_log is on
                    retryCount = 0;
                } // try
                catch (Exception ex) {
                    //System.out.println("Channel saving exception : " + ex );
                    //e.printStackTrace();
                    logFile.flush();
                    zipFile.flush();
                    zipFile.closeEntry();
                    retryCount++;
                } // catch
                if (retryCount > 0 && retryCount < 3) {
                    System.out.println("Retry reading channel " + atts[i].name);
                    i--;
                }
                if (retryCount >= 3) {
                    System.out.println("Error reading channel " + atts[i].name);
                }
            } // if
        } // for

    public static void delay(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
        }

    private void lockDir(String folder) throws FileNotFoundException {
        lockFile = new File(folder + "\\lock.lock");
        lockFileOS = new FileOutputStream(lockFile);
        locked = true;
        LOGGER.log(Level.FINE, "Directory locked");
    }

    private void unlockDir() throws IOException {
        lockFileOS.close();
        lockFile.delete();
        locked = false;
        LOGGER.log(Level.FINE, "Directory unlocked");
    }


    private void readConfigFromIni() {
        try {
            String s;
            int i;
            Wini ini = new Wini(new File(iniFileName));

            // Restore log level
            s = ini.get("Log", "level");
            if (s != null) {
                LOGGER.setLevel(Level.parse(s));
                LOGGER.log(Level.FINE, "Log level {0} set", s);
            }

            // Number of ADCs
            int n = 0;
            n = ini.get("Common", "ADCCount", int.class);
            if (n <= 0) {
                LOGGER.log(Level.WARNING, "No ADC declared in ini file");
                return;
            }
            // Read ADCs
            for (int j = 0; j < n; j++) {
                Device d = new Device();
                String section = "ADC_" + j;
                s = ini.get(section, "host");
                if (s != null)
                    d.host = s;
                s = ini.get(section, "port");
                if (s != null)
                    d.port = s;
                s = ini.get(section, "device");
                if (s != null)
                    d.dev = s;
                s = ini.get(section, "folder");
                if (s != null)
                    d.folder = s;
                else
                    d.folder = section;
                d.avg = 100;
                i = ini.get(section, "avg", int.class);
                if (i > 0)
                    d.avg = i;
                devList.add(d);
                LOGGER.log(Level.FINE, "Added {0}", d.getName());
            }
            // Read output directory
            s = ini.get("Common", "outDir");
            if (s != null)
                outRootDir = s;
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Ini file read error");
            LOGGER.log(Level.INFO, "Exception info", ex);
        }
        LOGGER.log(Level.FINE, "Configuration restored from {0}", iniFileName);
    }

    public void process() throws IOException {
        Formatter logFile = null;
        ZipFormatter zipFile = null;

        if (devList.isEmpty())  {
            LOGGER.log(Level.SEVERE, "No ADC found.");
            return;
        }

        // Fill AdlinkADC in deviceList
        int count = 0;
        for (Device d:devList) {
            try {
                d.init();
                d.timeout = System.currentTimeMillis();
                d.active = true;
                count++;
            }
            catch (Exception ex) {
                LOGGER.log(Level.INFO, "ADC {0} initialization error", d.getName());
                d.active = false;
                d.timeout = System.currentTimeMillis() + 10000;
            }
        }
        if (count == 0) {
            LOGGER.log(Level.WARNING, "No active ADC found");
            return;
        }

        long shotNew = 0;

        while (true) {
            try {
                for (Device d:devList) {
                    try {
                        if (!d.active) {
                            if (d.timeout > System.currentTimeMillis())
                                continue;
                            d.init();
                            d.timeout = System.currentTimeMillis();
                            d.active = true;
                            LOGGER.log(Level.FINE, "ADC {0} activated", d.fullName());
                        }
                        shotNew = d.readShot();
                        if (shotNew <= d.shot)
                            if(!locked)
                                break;
                            else
                                continue;
                        d.shot = shotNew;
                        System.out.printf("\n%s New Shot %d\n", timeStamp(), shotNew);

                        if(!locked) {
                            makeFolder();
                            lockDir(outFolder);
                            logFile = openLogFile(outFolder);
                            // write date and time
                            logFile.format("%s", dateTimeStamp());
                            // wrie shot number
                            String fmt = Constants.LOG_DELIMETER + "Shot" + Constants.PROP_VAL_DELIMETER + "%5d";
                            logFile.format(fmt, shotNew);
                            // open zip file
                            zipFile = openZipFile(outFolder);
                        }
                        System.out.println("Saving from " + d.fullName());
                        dumpADCDataAndLog(d, zipFile, logFile, d.folder);
                        zipFile.flush();
                    }
                    catch (DevFailed df) {
                        d.active = false;
                        d.timeout = System.currentTimeMillis() + 10000;
                        LOGGER.log(Level.INFO, "ADC {0} inactive, timeout for 10 seconds", d.fullName());
                    }
                }
                if(locked) {
                    zipFile.flush();
                    zipFile.close();
                    // write zip file name
                    String fmt = Constants.LOG_DELIMETER + "File" + Constants.PROP_VAL_DELIMETER + "%s";
                    String zipFileName = zipFile.getName();
                    logFile.format(fmt, zipFileName);
                    logFile.format(Constants.CRLF);
                    logFile.flush();
                    logFile.close();
                    unlockDir();
                    System.out.printf("\n%s Waiting for next shot ...", timeStamp());
                }
            }
            catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Unexpected exception");
                return;
            }
            delay(1000);
        }// while
    }

    class Device extends AdlinkADC {
        String folder = "ADC_0";
        int avg = 100;
        boolean active = false;
        long timeout = 0;
        long shot = -8888L;

        public Device() {
            this.folder = "ADC_0";
            this.avg = 100;
            this.active = false;
            this.shot = -8888L;
            this.timeout = System.currentTimeMillis();
        }

        public Device(String h, String p, String d, String f, int a) {
            super(h, p, d);
            this.folder = f;
            this.avg = a;
            this.active = false;
            this.shot = -8888L;
            this.timeout = System.currentTimeMillis();
        }

        public String getName() {
            return host + ":" + port + "/" + dev;
        }
    }


