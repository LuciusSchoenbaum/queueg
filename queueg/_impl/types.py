



class DateStore:

    def __init__(self):
        self.date = None

    def unset(self):
        return self.date is None

datestore = DateStore()


def parse_time(s):
    """
    Accepted time syntax, where <n> is a positive base ten integer,
    in Backus-Nauer notation, with short glosses in the column on the right:
    <::>       =   <n>  |                              s
                        <n>:<n> |                      m:s
                        <n>:<n>:<n>              h:m:s
    <input> =   <::>   |                         h:m:s
                       <n>-<::>                     d-h:m:s
    If an integer is passed, it is interpreted as seconds.

    :param s: string or integer
    :return: i: integer
    """
    if isinstance(s, int):
        return s
    if '-' in s:
        ss = s.split('-')
        if len(ss) != 2:
            raise ValueError
        else:
            i1 = 24*3600*int(ss[0])
        s2 = ss[1]
    else:
        i1 = 0
        s2 = s
    ss = s2.split(':')
    while len(ss) < 3:
        ss = ['0'] + ss
    i2 = sum([x * int(t) for x, t in zip([3600, 60, 1], ss)])
    i = i1 + i2
    return i



