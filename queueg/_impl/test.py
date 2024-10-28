






from os import system
from .conf import Conf


class Test:
    """
    QueueG unit testing helper class,
    based on `unittest <https://docs.python.org/3/library/unittest.html>`_.
    This code assumes particularities about how the tests
    are structured, it is not for general use.

    The ``mode`` input selects for unit test, integration test,
    or dryrun test.
    - unit test (``N``): Run without output, checking all assertions (pass/fail).
    - integration test (``I``): Run like unit test ITCINOOD.
    - dryrun (``T``): Run verbosely (expose stdout). Allows the user
        to manually inspect behavior during a test.

    .. note::

        Overall this testing arrangement is convenient,
        although I have found that unittest can take a very
        long time to initialize. I would be glad if there were
        a way to shorten this.

    Parameters:

        mode (char, 'N', 'I', or 'T'):
            Testing mode.
        tests (dict string --> list of string):
            Tests to run. If format A.B, B singles out a single
            test method, otherwise everything unittest finds in A is run.
        verbose (boolean):
            Whether to run verbosely. (Default: False)

    """

    def __init__(
            self,
            mode = 'N',
            tests = None,
            verbose = False,
    ):
        self.mode = mode
        self.testdict = tests if tests is not None else {}
        self.v = verbose
        self.conf = Conf()
        self.env = self.conf['env']
        if self.conf['venv_manager'] != 'conda':
            raise NotImplementedError
        if self.mode == 'N':
            # unit test: pass/fail
            self.vm = ''
        elif self.mode == 'I':
            # integration test
            self.vm = ''
        elif self.mode == 'T':
            # dryrun: show me the output
            self.vm = '-v'
        else:
            raise ValueError(f"Unrecognized mode")


    def start(
            self,
    ):
        for submodule in self.testdict:
            sm = submodule.split('.')
            sm = '.'.join([self.mode + x for x in sm])
            tests = self.testdict[submodule]
            for tgt in tests:
                stgt = tgt.split('.')
                if len(stgt) > 1:
                    cmd = f"conda run -n {self.env} python -m unittest {self.vm} {sm}.{self.mode}{stgt[0]}.{self.mode}.test_{stgt[1]}"
                else:
                    cmd = f"conda run -n {self.env} python -m unittest {self.vm} {sm}.{self.mode}{tgt}.{self.mode}"
                if self.v:
                    print(cmd)
                system(cmd)


    def help(self):
        system(f"conda run -n {self.env} python -m unittest -h")

