


# commands


from sys import platform




def cmd_stat_fmt(path, uname = None):
    """
    Get the last modified date+time in
    seconds since the Epoch.

    :param path: the target artifact
    :param uname: optional output from `uname`
    :return: command (string)

    :meta private:
    """
    if path == "":
        raise ValueError
    if uname is None:
        if platform.startswith('linux'):
            return f"test -f {path} && stat -c %X {path}"
        elif platform.startswith('darwin'):
            return f"test -f {path} && stat -f %m {path}"
        else:
            raise NotImplementedError
    else:
        if uname == 'Linux':
            return f"test -f {path} && stat -c %X {path}"
        elif uname == 'Darwin':
            return f"test -f {path} && stat -f %m {path}"
        else:
            raise NotImplementedError



def cmd_ls_fmt(path, uname = None):
    """
    Get the recursive directory tree information
    at the requested path, with last modified date+time
    in seconds since the Epoch included
    in the printed information.

    :param path: the target path
    :param uname: optional output from `uname`
    :return: command (string)

    :meta private:
    """
    if uname is None:
        if platform.startswith('linux'):
            return f"test -d {path} && ls -lR --time-style=+%s {path}"
        elif platform.startswith('darwin'):
            return f"test -d {path} && ls -lR -D %s {path}"
        else:
            raise NotImplementedError
    else:
        if uname == 'Linux':
            return f"test -d {path} && ls -lR --time-style=+%s {path}"
        elif uname == 'Darwin':
            return f"test -d {path} && ls -lR -D %s {path}"
        else:
            raise NotImplementedError







