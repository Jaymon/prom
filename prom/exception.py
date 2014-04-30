
class Error(Exception):
    """all prom errors will inherit from this base class"""
    def __init__(self, e, exc_info=None):
        self.e = e
        self.exc_info = exc_info
        super(Error, self).__init__(str(e))


class InterfaceError(Error):
    """specifically for wrapping SQLite and Postgres errors"""
    pass
