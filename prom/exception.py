
class InterfaceError(Exception):
    def __init__(self, e, exc_info=None):
        self.e = e
        self.exc_info = exc_info
        super(InterfaceError, self).__init__(str(e))

