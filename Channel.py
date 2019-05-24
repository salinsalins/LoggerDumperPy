
class Channel:
    adc = None
    devAttr = None
    dbAttr = None
    data = []
    shot = -2
    time = -1

    def __init__(self, adc, channelName):
        self.adc = adc
        self.devAttr = adc.devProxy.read_attribute(channelName)
        self.dbAttr = adc.devProxy.get_attribute_property(channelName)


    def fullName(self):
        return self.adc.devProxy.fullName() + "/" + self.devAttr.getName()

    def name(self):
        return self.devAttr.getName()


    def label(self):
        ai = self.adc.devProxy.get_attribute_info(name())
        return ai.label


    def shot(self):
        return self.shot


    def readShot(self):
        return self.adc.readShot()


    def readData(self):
        self.data = self.devAttr.extractDoubleArray()
        self.shot = self.readShot()
        self.time = self.devAttr.getTime()
        return self.data


    def getProperty(self, propertyName):
        return self.dbAttr.get_string_value(propertyName)


    def getPropertyAsDouble(self, propName):
        propVal = None
        try:
            propVal = self.Double.parseDouble(self.getProperty(propName))
        except:
            pass

        return propVal


    def getPropertyAsBoolean(self, propName):
        propVal = False
        propString = self.getProperty(propName)
        if propString.equalsIgnoreCase("True"):
            propVal = True
        elif propString.equalsIgnoreCase("on"):
            propVal = True
        elif propString.equals("1"):
            propVal = True
        return propVal


    def getPropertyAsInteger(self, propName):
        propVal = Integer.MIN_VALUE
        try:
            propVal = new Integer(getProperty(propName))
        except:
            pass
        return propVal


    def getPropValList(self):
        propNames = self.dbAttr.get_property_list()
        if propNames != None and len(propNames) > 0:
            for i in  range(len(propNames)):
                propVal = self.dbAttr.get_string_value(propNames[i])
                if propVal == None
                    propVal = ""
                propNames[i] = propNames[i] + Constants.PROP_VAL_DELIMETER + propVal
        return propNames


    def getTickList(self):
        tickList = []
        propNames = self.dbAttr.get_property_list()
        for propName in propNames:
            if propName.endswith(Constants.START_SUFFIX)):
                tick = Tick(self, propName.replace(Constants.START_SUFFIX, ""))
                if tick.length > 0.0 and tick.name != '':
                    tickList.append(tick)
        return tickList