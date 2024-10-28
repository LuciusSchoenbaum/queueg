


from .types import datestore
import datetime


class today:
    """
    Type of ``dateYMD`` argument of :any:`Path` to suggest a date
    relative to today, defined as the date at the instant that QueueG
    queries the date for the first time, which will be immediately
    after the process launches.

    The :any:`today` type has subtypes ``thismonth`` and ``thisyear``
    that can be substituted where :any:`today` appears.
    These are identical to ``today`` but :any:`Path`
    constructs only the YM part of YMD, or the Y part, respectively.

    Examples:

    .. code-block::

        # today
        path = queueg.Path(
            ...
            dateYMD = queueg.today(),
            ...
        )

        # yesterday
        path = queueg.Path(
            ...
            dateYMD = queueg.today(-1),
            ...
        )

        # Y/M
        path = queueg.Path(
            ...
            dateYMD = queueg.thismonth(),
            ...
        )

        # custom index
        path = queueg.Path(
            ...
            dateYMD = "1964/2/9",
            ...
        )


    Arguments:

        day_offset (integer): Number of days, from today's date, to offset.
            Typ. one of:

                - ``0``: today (default)
                - ``-1``: yesterday
                - ``1``: tomorrow

            For example, on February 9, 1964,
            the value of ``queueg.today(-1)`` is February 8, 1964, or ``1964/2/8`` in YMD.

    """

    def __init__(self, day_offset = 0):
        if datestore.unset():
            datestore.date = datetime.date.today()
            datestore.date += datetime.timedelta(days=day_offset)
        self.date = datestore.date



class thismonth(today):
    """
    Identical to :any:`today`,
    but Path constructs only the YM part of YMD.

    :meta private:
    """


class thisyear(thismonth):
    """
    Identical to :any:`today`,
    but Path constructs only the Y part of YMD.

    :meta private:
    """


