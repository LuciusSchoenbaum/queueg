



import datetime

from os import \
    popen as os_popen
from os.path import \
    exists as os_path_exists
from .cmd import cmd_stat_fmt

# the number of seconds since the Epoch,
# 1970-01-01 00:00:00 +0000  (UTC)
# according to strftime
fmt = "%s"



class Age:
    """
    Age of an artifact (a file),
    in terms of its time of update.
    Often referred to as the "last modified" timestamp.
    (By timestamp we mean the datetime.)
    If the file does not exist, the age is 0.

    Parameters:
        path (string):
            full path or relative path to a file.
            If None, a time of "now" (current datetime) is created. (default: None)
        age (string or integer):
            An age as timestamp, in seconds since Epoch.
    """

    #     todo somewhat deprecated, after learning about strftime,
    #      but continuing to use for the time being.

    def __init__(
            self,
            path = None,
            age = None,
    ):
        if path is not None:
            # S string output
            # m date modified
            # t format, passed to strftime
            if os_path_exists(path):
                cmd = cmd_stat_fmt(path)
                stat_ = os_popen(cmd).read().strip()
                self.dtint = int(stat_)
            else:
                self.dtint = 0
        elif age is not None:
            self.dtint = int(age) if age != '' else 0
        else:
            self.dtint = int(datetime.datetime.now().strftime(fmt))

        # some nonsense
        """
        __le__: 
        
        Return a <= b,
        can be called using the builtin <= operator in standalone syntax,
        a <= b             Same As         a.__le__(b)
        These and the other standard operations are 
        implicitly defined if __lt__ and __eq__ are defined. 

        :param b: 2nd argument
        :return: boolean
        """

    def __lt__(self, b):
        return self.dtint < b.dtint
    def __eq__(self, b):
        return self.dtint == b.dtint

    def __str__(self):
        return str(datetime.datetime.fromtimestamp(self.dtint))

