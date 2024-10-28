__all__ = [
    "experiment",
    #
    "Case",
    "Conf",
    "Location",
    "Sync",
    "Test",
    "today",
    "thismonth",
    "thisyear",
    "version",
    "Mode",
    "direct",
    "indirect",
    #
    "Post",
    #
    "CRun",
    "MPIRun",
    "BasiliskRun",
    "BasiliskMPIRun",
    #
    "CppRun",
    #
    "EngineRun",
    "PythonRun",
    #
    "PyPinnchRun",
    #
    "ttyRead",
    "prompt_user",
    "now",
    "copy",
    "zip_something_up",
    "default_conf_stemlist",
    "conf_file",
    "default_index_stem",
    "current_jobs",
    "Age",
    "cmd_stat_fmt",
    "cmd_ls_fmt",
    #
    "Figure",
    "Animation",
    "Graph",
    "create_dir",
    "sortdown",
]

# exposing the visutil sublibrary in mv1fw via queueg
# because it is often useful in ad-hoc post-processing tasks
from mv1fw.visutil import (
    Figure,
    Animation,
    Graph,
)
from mv1fw import (
    create_dir,
    sortdown,
)


from ._impl import (
    Case,
    Conf,
    Location,
    Sync,
    Test,
    today,
    thismonth,
    thisyear,
    Mode,
    direct,
    indirect,
    Post,
)


from . import experiment




from .C import (
    CRun,
    MPIRun,
    BasiliskRun,
    BasiliskMPIRun,
)

from .Cpp import (
    CppRun,
)

from .Python import (
    EngineRun,
    PythonRun,
)

from .PyPinnch import (
    PyPinnchRun,
)


# todo I wanted X.ossys but gave up
from ._impl.ossys.ossys import (
    ttyRead,
    prompt_user,
    copy,
    now,
    zip_something_up,
    default_conf_stemlist,
    conf_file,
    default_index_stem,
    current_jobs,
)
from ._impl.ossys.age import Age
from ._impl.ossys.cmd import (
    cmd_stat_fmt,
    cmd_ls_fmt,
)


def version():
    return "0-42"


