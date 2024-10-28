




from os import (
    getcwd as os_getcwd,
    chmod as os_chmod,
    remove as os_remove,
)
from os.path import join as os_path_join
import datetime
import subprocess

from mv1fw import (
    Logger,
)

from .._impl.conf import Conf


insertx = \
R"""
from sys import argv
!import_main!
!case_import_string!

if __name__ == '__main__':
    if isinstance(config, list):
        for c in config:
            c(engine)
    else:
        config(engine)
    !config_class!
    engine.set_verbosity("quiet")
    engine.start(
        output_absolute_directory=argv[1],
        reference_absolute_root_directory=argv[2],
        case=case,
        code=argv[3],
        file=!file!,
    )
"""

filenamex = "_tmp.py"



class EngineRun:
    """
    Base type of Run objects for an engine-pattern Python job.
    The Run instance "steers" the solver during a scripted multiple-run job.

    Supports applying "config methods"
    between runs. This is mainly in order to enable multiple-run sweeps,
    but it can be used for other purposes.
    A **config method** is a callable e --> None that acts on e,
    the engine. From the :any:`Engine`, you have access to the the
    :any:`Problem` and essentially all other
    parameters you might wish to adjust.

    To use the class, provide either ``main_module``, ``config_module``
    and ``config_list``, or else (the messier but more flexible way)
    ``explicit_import``.
    For example::

        main_module="main",
        config_list="config1, config2"

    is equivalent to::

        explicit_import = 'from main import engine\\nfrom main import config1, config2\\nconfig = [config1, config2]\\n'

    Parameters:

        main_module (string):
            a name of a Python module containing the main module,
            the access point for the run background and the run engine.
        config_module (optional string):
            a name of a Python module where the config methods are found.
            (Default: same as main_module)
        config_list (optional string):
            a plaintext list of config methods from the config module specified by config_module.
            Provide either a comma-separated list, or None to apply no config methods.
            (Default: None)
        config_args (optional string):
            a plaintext list of arguments to a Config class defined in the config module.
            If this argument is set, a Config class must be defined, otherwise it is not necessary to define it.
            This argument defines the arguments that are passed to the config class in order to initialize.
            A config_list can do essentially anything that can be done using config_args,
            but it is sometimes more convenient to use config_args instead of the simpler,
            but less flexible config_list approach.
        explicit_import (string):
            Explicit import string, STIUYKB.
            Overrides use of main_module, config_module, and config_list.
            It should include the line(s) of code containing the engine import,
            and the line(s) of code containing all config imports,
            and a definition of the reference ``config`` as
            either a config method or a list of config methods.
        case (:any:`Case`):
            Case instance.
        caselist: (list of string):
            A list of case codes to be executed. For example::

                caselist = case.all() # generates all possibilities
                caselist = ['a1', 'a2', 'a3', 'b1', 'b3', 'c2'] # a selection

            See :any:`Case`.
        case_name (optional string):
            A name of a case found in a module called cases.
        case_import_string (optional string):
            A string to explicitly import the case. E.g., from a file in the local directory.
            For custom use case. Overrides case_name.
        location (:any:`Location`):
            Location for outputs.
        reference_location (optional :any:`Location`):
            Optional location for reference outputs (solutions).
            If not given, the location is inferred from `location`.
        cluster_tasks (optional pair of integers):
            A pair (number of nodes, number of tasks per node)
            setting up a simple distributed node topology.
            If set, triggers a SLURM non-blocking run (via ``sbatch``).
        job_time_limit (optional string):
            a time limit (default: 1 day).
            Via `SLURM documentation <https://slurm.schedmd.com/sbatch.html#OPT_time>`_:
            A time limit of zero requests that no time limit be imposed.
            Acceptable time formats include "minutes", "minutes:seconds",
            "hours:minutes:seconds", "days-hours", "days-hours:minutes"
            and "days-hours:minutes:seconds".
        partition (optional string):
            name of partition on target system
        SLURM_args (optional list of string):
            Other options desired for a SLURM nonblocking execution.
            See `sbatch documentation <https://slurm.schedmd.com/sbatch.html>`_
            or elsewhere for help/assistance.
        verbose (boolean):
            Verbose output.

    """

    # todo case_name and case_import_string are very awkward.
    #  change to explicit_import_case, think further.

    # todo
    #  CRunBase+Sync ideas are mixed with older PyPinnch ideas.
    #  CRunBase+Sync ideas are generally better, review
    # todo
    #  Going in the other direction, perform case runs using CRunBase.

    # todo verbose output: improve?:
    #  step through code and consider adding more verbose messages.


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
        if location is None:
            raise ValueError("You must specify an output location.")
        self.implicit_runtag = (location.runtag is None or location.runtag == "")
        if self.implicit_runtag:
            # Invariant: A runtag is always applied to the handle.
            # Why: PyPinnch cleans its output directory during its init.
            # On the other hand, the project directory is free space
            # for anything the user possibly wants to do.
            # So in other words, the user defines the meaning
            # of "run" but not the meaning of "PyPinnch run".
            # At the next run (if the run is re-deployed),
            # everything below the runtag
            # in the project tree should be left alone (not cleaned).
            location.update_runtag('run')
        conf = Conf()
        self.path = location.get_path(
            create = True,
            explicit_conf = conf,
        )
        if reference_location is not None:
            # do not create, as reference data must already be present there
            self.reference_path = reference_location.get_path(explicit_conf = conf)
        else:
            self.reference_path = location.get_root_path()
        self.wdir = os_getcwd()
        if location.jumplist:
            self.wdir = os_path_join(self.wdir, *location.jumplist)
        # > for printing only
        self.handle_list = location.get_handle_list()
        # self.handle = '/'.join(handle_list) if handle_list else 'handle_not_specified'
        # > virtual environment
        self.venv_manager = conf['venv_manager']
        self.env = conf['env']
        # The specifications for this run mechanism are:
        # - a mechanism whose main process does not fail if an individual run should fail.
        # - a mechanism that supports case-by-case run (organized clusters or sets of runs).
        # We might be able to achieve these specifications in a more effective/convenient
        # implementation, using the multiprocessing library. But it will take more work to do that.
        if main_module is None and explicit_import is None:
            raise ValueError(f"Run engine not specified")
        else:
            if main_module is not None:
                self.import_main = f"from {main_module} import engine\n"
                cfgmdl = config_module if config_module is not None else main_module
                if config_list is not None and len(config_list) > 0:
                    self.import_main += f"from {cfgmdl} import {config_list}\nconfig = [{config_list}]\n"
                else:
                    self.import_main += "config = lambda e: None\n"
                if config_args is not None:
                    self.config_class = f"_ = Config(engine, {config_args})"
                    self.import_main += f"from {cfgmdl} import Config\n"
                else:
                    self.config_class = ""
            else:
                self.import_main = explicit_import

        if case is None:
            self.caselist = [""]
            self.case_import_string = "case = None"
        else:
            self.caselist = caselist if caselist is not None else case.list_all()
            if case_import_string is None and case_name is None:
                raise ValueError(f"Case not specified")
            else:
                self.case_import_string = case_import_string if case_import_string is not None else f"from cases import {case_name}"

        # > whether to execute via slurm queue, in nonblocking mode.
        # > In other words, to queue the job instead of watching it run.
        self.SLURM_nonblocking = (cluster_tasks is not None)
        self.cluster_tasks = (1,1) if cluster_tasks is None else cluster_tasks
        if not isinstance(self.cluster_tasks, tuple):
            # todo sanity check of the requested cluster tasks? (after polling system?)
            # todo support for the general form(s) that this argument can take (cf. SLURM)?
            raise ValueError(f"cluster_tasks must be a pair of integers (nodes, ntasks), "
                             f"where nodes is the number of nodes, ntasks is the number of tasks per node.")
        self.job_time_limit = job_time_limit
        self.partition = partition
        self.SLURM_args = SLURM_args if SLURM_args is not None else []
        self.v = verbose
        ######################
        self.dat_stem = "dat"
        self.info_stem = "info"
        self.script_name = "swh"

        # todo accord with other runbase types
        self.log = Logger()
        self.log_err = Logger()


    def start(self):
        self._init()
        self._emit_script()
        if self.SLURM_nonblocking:
            self._start_nonblocking()
        else:
            for code in self.caselist:
                self._start_subprocess(code)
        self._deinit()


    def multiple_runs(self):
        return len(self.caselist) > 1


    def _init(self):
        # > truncate message if implicit runtag
        handle = '/'.join(self.handle_list if not self.implicit_runtag else self.handle_list[:-1])
        init_msg = f"Starting {handle}.\n"
        self.log(init_msg)
        if self.multiple_runs():
            dt = datetime.datetime.now()
            dts = f"{dt.month}/{dt.day}/{dt.year}"
            ts= f"{dt.hour}:{dt.minute}"
            info = f"\n{handle}\n\n"
            info += f"date/time: {dts} {ts}\n\n"
            # info += {datetime.datetime.now().isoformat()}\n\n"
            with open(os_path_join(self.path, "information.txt"), "w") as f:
                f.write(info)


    def _emit_script(self):
        script = insertx
        script = script.replace("!import_main!", self.import_main, 1)
        script = script.replace("!case_import_string!", self.case_import_string, 1)
        script = script.replace("!config_class!", self.config_class, 1)
        description = "None"
        if self.multiple_runs():
            file = "case.file"
        else:
            file = "None"
        script = script.replace("!description!", f"\"\"\"{description}\"\"\"", 1)
        script = script.replace("!file!", file, 1)
        if self.v:
            self.log('runscript:\n\n' + script)
        with open(os_path_join(self.wdir, filenamex), "w") as f:
            f.write(script)


    def _store_logs(self):
        with open(os_path_join(self.path, "out.txt"), "w") as f:
            f.write(self.log.thelog)
        with open(os_path_join(self.path, "err.txt"), "w") as f:
            f.write(self.log_err.thelog)


    def _deinit(self):
        os_remove(os_path_join(self.wdir, filenamex))
        if self.log_err.thelog:
            print(f"\nError messages detected.")
            printout = ""
            lines = self.log_err.thelog.strip().split('\n')
            start = ""
            end = "\n"
            ln = len(lines)
            if ln <= 10:
                printout += '\n'.join([f"[stderr] > {line}" for line in lines])
            else:
                printout += '\n'.join([f"[stderr] > {line}" for line in lines[:5]])
                printout += "\n(snip)\n"
                printout += '\n'.join([f"[stderr] > {line}" for line in lines[-5:]])
            print(start+printout+end)
        self.log("Done.\n")
        print(f"Writing files to {self.path}")
        self._store_logs()


    def _start_subprocess(self, code):
        program_args = [self.path, self.reference_path, code]
        try:
            # This will wait for the subprocess to finish.
            # See https://docs.python.org/3/library/subprocess.html#subprocess.run
            cp = subprocess.run(
                ["conda", "run", f"-n{self.env}", "python", os_path_join(self.wdir, filenamex)] + program_args,
                capture_output=True,
                )
            mr = self.multiple_runs()
            if mr:
                self.log(f"Finished running case {code}.")
                self.log_err(f"case {code}:\n", save=True)
            self.log(cp.stdout.decode(), save=True, end="")
            self.log_err(cp.stderr.decode(), save=True, end="")
            if mr and code != self.caselist[-1]:
                self.log_err(f"\n\n\n\n===============", save=True)
            # write the partial logs, helpful for long-running jobs
            # todo empty buffer and append, instead of cascading rewrites
            self._store_logs()
        except subprocess.CalledProcessError as exc:
            self.log(f"subprocess return code {exc.returncode}\n{exc}")



    def _start_nonblocking(self):
        caselist = self.caselist
        if self.venv_manager == "conda":
            python_invoc = ["conda", "run", f"-n{self.env}", "python"]
        elif self.venv_manager == "venv":
            # todo impl
            # Every time I think of doing this I get stuck
            # trying to make sense of venv
            raise NotImplementedError
        elif self.venv_manager is None:
            # todo impl
            # If you are here you probably know that
            # you shouldn't be running Python this way.
            python_invoc = ["python"]
        else:
            raise NotImplementedError
        python_invoc += [] # todo venv_args
        # > non-blocking execution via SLURM.
        slurm_args = []
        # > how many nodes, how many tasks per node
        slurm_args += [f"--nodes={self.cluster_tasks[0]}", f"--ntasks={self.cluster_tasks[1]}"]
        # > time limit
        if self.job_time_limit:
            slurm_args += [f"--time={self.job_time_limit}"]
        # > partition
        if self.partition:
            slurm_args += [f"--partition={self.partition}"]
        # > user-requested args
        slurm_args += self.SLURM_args
        self._msg(f"Executing in nonblocking (batch) mode with arguments {' '.join(slurm_args)}.")
        script_list = ["srun"] + slurm_args + python_invoc + [os_path_join(self.wdir, filenamex)]
        script = "#!/bin/bash\n\n"
        for code in caselist:
            program_args = [self.path, code]
            script += ' '.join(script_list + program_args) + '\n'
        # > create batch script
        script_name = os_path_join(self.wdir, self.script_name)
        with open(script_name, "w") as f:
            f.write(script)
        # > chmod +x
        os_chmod(script_name, 0o755)
        # > build alist (talk to slurm)
        alist = ["sbatch", script_name]
        self._msg(f" < {' '.join(alist)}", function="run", always=True)
        try:
            cp = subprocess.run(
                alist,
                capture_output=True,
            )
            s = cp.stdout.decode("utf-8").strip()
            self._msg(f" > {s}", function="run", always=True)
            self.log(self.script_name, "out", s)
            s = cp.stderr.decode("utf-8").strip()
            if s:
                self._msg(f" 2> {s}", function="run", always=True)
            self.log_err(self.script_name, "err", s)
        except subprocess.CalledProcessError as exc:
            self._msg(f"return code {exc.returncode}\n{exc}", function="run", always=True)
        # > talk to the user
        self._msg(f"Submitted job to SLURM queue.")




    def _msg(self, body, function = None, always = False, as_is = False):
        loc_list = [self.__class__.__name__]
        if function is not None:
            loc_list += [function]
        if self.v and not always:
            loc_list += ["verbose"]
        message = body if as_is else f"[{':'.join(loc_list)}] {body}"
        if self.v or always:
            print(message)








