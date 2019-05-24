class Tick:

    def __init__(self, name='', start=0.0, length=0.0):
        self.name = name
        self.start = start
        self.length = length


    Tick(Channel chan, tickName):
        this()
        if (!(tickName == None || "".equals(tickName))):
            start = chan.getPropertyAsDouble(tickName + Constants.START_SUFFIX)
            length = chan.getPropertyAsDouble(tickName + Constants.LENGTH_SUFFIX)
            name = chan.getProperty(tickName + Constants.NAME_SUFFIX)
            if (name == None || "".equals(name)) name = tickName
        }
    }

    Tick(Signal sig, tickName):
        this(sig.y, tickName)
    }

    def equals(self, tick):
        if not tick instanceof Tick):
            return False
        return ((Tick)tick).name.equals(name)


    def toString(self):
        return "Tick: %s : %g : %g" % (self.name, self.start, self.length)
