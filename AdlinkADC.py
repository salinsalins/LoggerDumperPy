import tango

DEFAULT_HOST = "192.168.1.49"
DEFAULT_PORT = "10000"
DEFAULT_DEV = "binp/nbi/adc0"
SHOT = "Shot_id"


class AdlinkADC :


    def __init__(self):
        self.host= DEFAULT_HOST
        self.port= DEFAULT_PORT
        self.dev= DEFAULT_DEV
        self.devProxy= tango.DeviceProxy(self.dev, self.host, self.port)
    

    def fullName(self) :
        return self.devProxy.fullName(self)
    

    def name(self) :
        return self.devProxy.name(self)
    

    def readShot(self) :
        newShot = -1
        try :
            da = self.devProxy.read_attribute(SHOT)
            newShot = da.extractLong(self)
            return newShot
        except :
            return -2


    def getPropertyList(self) :
        return self.devProxy.get_property_list("*")
    

    def getPropertyList(self, wildcards) :
        return self.devProxy.get_property_list(wildcards)
    

    def getChannels(self) :
        attrInfo = self.devProxy.get_attribute_info(self)
        return attrInfo
    

    def getChannelNames(self) :
        attrInfo = self.devProxy.get_attribute_info(self)
        channelNames = []
        for ai in attrInfo :
            if ai.name.startswith(Constants.CHAN) :
                channelNames.append(ai.name)
        return channelNames
    

    def getSignalNames(self) :
        attrInfo = self.devProxy.get_attribute_info(self)
        yChannel = Constants.CHAN + "y"
        xChannel = Constants.CHAN + "x"
        # find all chanyNN for wich exists corresponding chanxNN
        signalNames = []
        for ai in attrInfo :
            if ai.name.startswith(yChannel) :
                xName = ai.name.replace(yChannel, xChannel)
                for aj in attrInfo :
                    if aj.name.equals(xName) :
                        signalNames.append(ai.name)
                        break
        return signalNames
    

    def getSignals(self) :
        attrInfo = self.devProxy.get_attribute_info(self)
        yChannel = Constants.CHAN + "y"
        xChannel = Constants.CHAN + "x"
        n = 0
        for (i = 0 i < attrInfo.length i += 1) :
            if (attrInfo[i].name.startsWith(yChannel)) :
                xName = attrInfo[i].name.replace(yChannel, xChannel)
                for (j = 0 j < attrInfo.length j += 1) :
                    if (attrInfo[j].name.equals(xName)) :
                        n += 1
                        break
        signals = AttributeInfo[n]
        n = 0
        for (i = 0 i < attrInfo.length i += 1) :
            if (attrInfo[i].name.startsWith(yChannel)) :
                xName = attrInfo[i].name.replace(yChannel, xChannel)
                for (j = 0 j < attrInfo.length j += 1) :
                    if (attrInfo[j].name.equals(xName)) :
                        signals[n] = attrInfo[i]
                        n += 1
                        break
        return signals
