




class FigureofMerit:
    """
    A figure of merit: a measurement
    or score or grade, accompanied
    by an interpretation for convenience.
    Used by :any:`Post`.

    Parameters:

        value (scalar):
            figure of merit
        judgment (optional string):
            judgment, assessment, or interpretation
        label (optional string):
            label, if the figure is associated with a label

    """

    def __init__(self, value, judgment = None, label = None):
        self.value = value
        self.judgment = judgment
        self.label = label





