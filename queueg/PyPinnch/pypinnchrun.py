






from ..Python.enginerun import EngineRun



class PyPinnchRun(EngineRun):
    """
    Run PyPinnch and store data for post-analysis.

    To use the class, provide either ``main_module``, ``config_module``
    and ``config_list``, or else (the messier but more flexible way)
    ``explicit_import``.
    For example::

        main_module="main",
        config_module="main",
        config_list="config1, config2"

    is equivalent to::

        explicit_import = 'from main import engine\\nfrom main import config1, config2\\nconfig = [config1, config2]\\n'

    Parameters:

        main_module (string):
            see :any:`PyPinnchRunBase`
        config_module (optional string):
            see :any:`PyPinnchRunBase`
        config_list (optional string):
            see :any:`PyPinnchRunBase`
        explicit_import (string):
            see :any:`PyPinnchRunBase`
        location (:any:`Location`):
            see :any:`PyPinnchRunBase`
        reference_location (optional :any:`Location`):
            see :any:`PyPinnchRunBase`
        cluster_tasks (optional pair of integers):
            see :any:`PyPinnchRunBase`
        job_time_limit (optional string):
            see :any:`PyPinnchRunBase`
        partition (optional string):
            see :any:`PyPinnchRunBase`
        SLURM_args (optional list of string):
            see :any:`PyPinnchRunBase`
        verbose (boolean):
            see :any:`PyPinnchRunBase`

    """

    def __init__(
            self,
            # API #1
            main_module = "main",
            config_module = "main",
            config_list = None,
            config_args = None,
            # API #2
            explicit_import = None,
            # Cases
            case = None,
            caselist = None,
            case_name = None,
            case_import_string = None,
            # location
            location = None,
            reference_location = None,
            # SLURM
            cluster_tasks = None,
            job_time_limit = "1-00:00:00",
            partition = None,
            SLURM_args = None,
            # other
            verbose = False,
    ):
        super().__init__(
            main_module = main_module,
            config_module = config_module,
            config_list = config_list,
            config_args = config_args,
            #
            explicit_import = explicit_import,
            #
            case=case,
            caselist=caselist,
            case_name = case_name,
            case_import_string = case_import_string,
            #
            location = location,
            reference_location = reference_location,
            #
            cluster_tasks=cluster_tasks,
            job_time_limit=job_time_limit,
            partition=partition,
            SLURM_args=SLURM_args,
            #
            verbose=verbose,
        )



