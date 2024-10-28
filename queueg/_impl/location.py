

from os import (
    getcwd as os_getcwd,
    listdir as os_listdir,
    remove as os_remove,
)
from os.path import (
    join as os_path_join,
    exists as os_path_exists,
    split as os_path_split,
    isdir as os_path_isdir,
    isfile as os_path_isfile,
)

from mv1fw import (
    create_dir,
)

from .today import (
    today,
    thismonth,
    thisyear,
)
from .conf import Conf




class Location:
    """
    Location allows management of convenient structured paths.

    .. note::
        _Who is a structured path convenient for?_
        A structured path is convenient for a researcher
        or user who is performing repeated experiments,
        such as statistical experiments, numerical simulations,
        numerical experiments, coding experiments, or
        programming benchmarks.

    A structured path, or location,
    is created by combining the following components:

    .. code-block::

        <locale> <project> <datestamp> <handle>

    These may be described and specified as follows::

        - The ``<locale>`` or root location, must be defined
            in the user's Conf file. At least two locales must be
            defined, the **inventory**, and the **index**,
            for inputs and outputs, respectively.
            However, the user can define other locales by adding
            them to the Conf file. See the example below.
        - The ``<project>`` enables the locale to be subdivided.
            It may be composed of multiple stems.
            Normally, it is one stem, which is an alphanumeric string.
            If empty, or None, then it defaults to an empty value.
        - The ``<datestamp>`` or ``dateYMD`` is may be composed
            of multiple stems. It is typically a date of some kind,
            but in fact, the only condition is that
            each stem that it specifies must be purely numerical
            (consisting of digits 0-9) and cannot be empty.
            It need not be a date or even be a date structured in any
            particular way, although Y/M/D is an unofficial standard.
            There are helpers provided by QueueG that can be used
            to set typical (time/date dependent) datestamps.
        - The ``<handle>`` is often implicitly defined,
            but it can be defined explicitly, if needed,
            If empty, or None, then it defaults to an implicit handle,
            which is defined either via the working directory (typical)
            or (untypical) a directory specified by the user.

    Note that these rules are STIUYKB. As long as you
    use QueueG in an idiomatic way, they won't give you any trouble.

    An example to illustrate, which uses an explicit handle, is:

    .. code-block::

        # Information in the Conf file, in TOML:
        index_dir = '/home/ringostarr/index/'
        inventory_dir = '/home/ringostarr/inventory/'
        ...
        # Information in the arglist:
        <locale> = index
        <project> = 'popular'
        <datestamp> = '1964/2/9'
        <handle> = 'edsullivanshow/drums'
        # or explicitly:
        location = Location(
            locale = 'index',
            project = 'popular',
            dateYMD = '1964/2/9',
            handle = 'edsullivanshow/drums',
        )

    In this example, the resulting explicit path is
    ``/home/ringostarr/index/popular/1964/2/9/edsullivanshow/drums``.
    The datestamp and handle can be constructed implicitly,
    see the argument list below for details.

    It is more common to define the handle and the date implicitly.
    This is an example where a file's location implicitly establishes
    the handle, and a datestamp is chosen automatically,
    at the time of execution.

    .. code-block::

        # Conf file is same as before
        index_dir = '/home/ringostarr/index/'
        inventory_dir = '/home/ringostarr/inventory/'
        ...
        # Location of file:
        /home/ringostarr/inventory/career/musician/1964/concerts/america/edsullivan
        # Information in the arglist:
        <locale> = index
        <project> = 'photos'
        <datestamp> = (today)
        <handle> = (implicit)
        # or explicitly:
        location = Location(
            locale = 'index',
            project = 'photos',
            dateYMD = today(),
        )

    In this example, if the script where the location was defined
    was executed on May 18, 2021, the resulting explicit path is
    ``/home/ringostarr/index/photos/2021/5/18/concerts/america/edsullivan``.

    Arguments:

        locale (string):
            Default: 'index', can also be 'inventory' or a user-defined location.
            This root path must be defined in the Conf file.
        project (optional string):
            A keyphrase used to access a location in the root location.
            Determines the root of path that filename_prefix will be attached to.
            These locations are used to organize the inventory, and the index.

            The following rules apply:
                - A project stem cannot be purely numeric (consisting only of digits 0-9).

        dateYMD (optional string or :any:`today`):
            A datestamp, that can be specified by these types:

                - ``None``: no datestamp (default)
                - ``string``: explicit datestamp (*)
                - :any:`today` : a datestamp, in days, relative to today (**).

                    - today(0) or today(): today
                    - today(-1): yesterday
                    - today(1): tomorrow
                    - Etc.

            Instances of ``thismonth`` and ``thisyear`` are also :any:`today` instances,
            and work identically, but date-stamp with the suggested
            granularity, either Y/M or Y.

            (*) An explicit datestamp can be specified either by Y/M/D or Y-M-D
            format, if it was originally created with :any:`today`,
            but actually any string separated by / (slash) or by - (hyphen).
            If the output is date-stamped, the date can be passed here
            in either 2010/1/2 or 2010-1-2 format (year, month, date).
            Explicit year+month datestamps will work also: 2018-2 will work,
            and so will 2018/2, for February 2018. The explicit year by itself,
            2022, 2023, etc., can also be used, in order to bin artifacts annually.

            (**) Should the run occur over multiple days, "today" is defined as
            the day on which dateYMD is checked the first time, see :any:`today`.

            The following rules apply:
                - A datestamp must be a chain of purely numerical strings.
                    Other than / (slash) and - (hyphen) it may only contain digits 0-9.
                - A datestamp cannot be empty. If None, or empty, a
                    datestamp of '0000' is used.

        handle (string or list of string or :any:`handle`):
            A list of stems (you may think of directory names)
            to organize the desired path.
            It is often convenient to instead use :any:`handle` to
            implicitly define the handle. Doing this automatically
            correlates the locations of the inputs (scripts), to the outputs (data, logs, etc).

            The following rules apply:
                - A handle stem cannot be purely numeric (consisting only of digits 0-9).

        runtag (optional string):
            An optional stem for building a straightforward
            loop of runs. Will be appended to the handle
            as the final stem. Cannot be complex, i.e., it must be a single stem.
        jump (optional string):
            A jump to a higher input directory. Syntax same as ``project``.
            Describes a higher directory from the directory
            where a script is stored (or more generally,
            to a higher directory from the directory where the
            input directory is defined by the run script).
            Use a jump in order to "jump" up to higher directory
            before deploying the run. The typical use case is that there
            exists several runs developed as independent, free-standing
            descriptions, and the user wishes to run them all using a
            managing process in a base directory, without moving/copying
            the files, i.e. while still maintaining each as a standalone entity.
            The ``jump`` value can be compared to the ``runtag``:
            the runtag affects the output directory only, while jump
            affects the input directory, and if a handle is used via :any:`handle`,
            the jump will also affect the output directory (where the
            output is binned for storage).
            The `jump` parameter can be regarded a tool to perform
            more loosely structured multiple runs than :any:`Case`.
            The suitable use case for relying on `jump` is a small
            handful of runs, which is not uncommon.
            (Default: None)

    """

    # todo the argument docstrings for 'jump' and 'runtag'
    #  are slightly out of place in the abstract setting of
    #  Location as it refers to output/input/run. I'm not certain
    #  how best to modify documentation---my main concern is
    #  I'd like to have all the "Location documentation" in one place.

    # todo document the possibility of locale == 'local' which
    #  should (1) always work incl. with jump and/or runtag and (2) always set the path to
    #  the 'local' directory where the script is located (the cwd at excecution)
    #  ...this is not a priority but should be done eventually


    def __init__(
            self,
            locale = 'index',
            project = None,
            dateYMD = None,
            handle = None,
            jump = None,
            runtag = None,
    ):
        self.locale = locale
        # > project
        stemlist = project.split('/') if project is not None else []
        # > dateYMD
        if dateYMD is None:
            # No dateYMD given
            # This branch is for an edge case,
            # it is not the idiomatic or expected use case.
            # Invariant: there is at least one numeric
            # stem dividing the project and the handle.
            stemlist += ['0000']
        else:
            if isinstance(dateYMD, today):
                date = dateYMD.date
                stemlist += [str(date.year)]
                if not isinstance(dateYMD, thisyear):
                    stemlist += [str(date.month)]
                if not isinstance(dateYMD, thismonth):
                    stemlist += [str(date.day)]
            else:
                if '/' in dateYMD:
                    # 2023/12/9 or 2023/12
                    dt = dateYMD.split('/')
                elif '-' in dateYMD:
                    # 2023-12-9 or 2023-12
                    dt = dateYMD.split('-')
                else:
                    # 2023 (just a year) or else the user broke/bent the rules
                    # > keep things simple:
                    dt = [dateYMD]
                stemlist += dt
        # > handle
        if handle is None:
            # implicit handle
            hstemlist = []
            # todo this may need to be generalized (to non-cwd cases)
            #  but I wait until there is a clear need.
            tmp = os_getcwd()
            safety = 0
            while True:
                tmp, x = os_path_split(tmp)
                # todo windows
                if x.isdigit() or tmp == "/":
                    break
                hstemlist.append(x)
                safety += 1
                # safety break
                if safety == 100:
                    raise ValueError
            hstemlist.reverse()
        else:
            # handle is not None:
            # user specifies an explicit handle.
            # This branch is here for edge cases,
            # it is not an idiomatic or expected use case.
            if isinstance(handle, list):
                hstemlist = handle
            else:
                if handle == "":
                    raise ValueError
                else:
                    hstemlist = handle.split('/')
        self.handle_length = len(hstemlist)
        stemlist += hstemlist
        # > jump
        jumplist = jump.split('/') if jump is not None else None
        if jumplist is not None:
            stemlist += jumplist
            self.handle_length += len(jumplist)
            self.jumplist = jumplist
        else:
            self.jumplist = None
        # > runtag
        self.runtag = runtag
        self.stemlist = stemlist


    def get_root_path(
            self,
            create = False,
            explicit_conf = None,
    ):
        """
        Create a valid path for the operating system
        from the location, for the root path
        (the path, not including the runtag).

        Arguments:

            explicit_conf (optional string or None):
                STIUKB. If you know the Conf file already, you may pass it to
                the Path constructor using this argument,
                otherwise it will read it from storage. (default: None)
            create (boolean):
                Whether to create the directory. Default: False

        Returns:

                string, a path

        """
        if self.locale == 'local':
            # > the local directory is requested
            base = os_getcwd()
            stemlist = []
        else:
            conf = explicit_conf if explicit_conf is not None else Conf()
            base = conf[f'{self.locale}_dir']
            stemlist = self.stemlist
        if create:
            path = create_dir(base, stemlist)
        else:
            path = os_path_join(base, *stemlist)
        return path


    def filename(
            self,
            simple_filename,
    ):
        """
        Create a filename (absolute path to file,
        whether existing or not)
        from a simple filename with no directory
        information.

        Arguments:

            simple_filename (string)

        """
        path = self.get_path(create=False)
        filename = os_path_join(path, simple_filename)
        return filename


    def clean(
            self,
            skip = None,
    ):
        """
        Clean the location of files.

        Arguments:

            skip (optional list of string):
                Files to skip (do not delete)
                can be optionally specified.
                Directories are always skipped,
                for the sake of safety.

        """
        skip_ = [] if skip is None else skip
        path = self.get_path(create=False)
        if os_path_exists(path) and os_path_isdir(path):
            for filename in os_listdir(path):
                filename = os_path_join(path, filename)
                if os_path_isfile(filename) and not filename in skip_:
                    os_remove(filename)



    def get_path(
            self,
            create = False,
            explicit_conf = None,
    ):
        """
        Generate a valid path for the operating system
        from the location.

        Arguments:

            explicit_conf (optional string or None):
                STIUKB. If you know the Conf file already, you may pass it to
                the Path constructor using this argument,
                otherwise it will read it from storage. (default: None)
            create (boolean):
                Whether to create the directory. Default: False

        Returns:

                string, a path

        """
        root_path = self.get_root_path(create=create,explicit_conf=explicit_conf)
        if self.runtag is not None:
            stemlist = [self.runtag]
            if create:
                path = create_dir(root_path, stemlist)
            else:
                path = os_path_join(root_path, *stemlist)
        else:
            path = root_path
        return path


    def create_path(self):
        """
        Create the target path
        """
        _ = self.get_path(create=True)


    def update_runtag(
            self,
            runtag,
    ):
        """
        Change the runtag

        Arguments:

            runtag (string):

        """
        self.runtag = runtag



    def get_handle_list(self):
        """
        Obtain the handle list, including the runtag.

        Returns:

                list of string

        """
        L = len(self.stemlist)
        hL = self.handle_length
        out = self.stemlist[L-hL:]
        if self.runtag:
            out += [self.runtag]
        return out
