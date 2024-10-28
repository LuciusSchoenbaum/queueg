



from os import (
    chdir as os_chdir,
    getcwd as os_getcwd,
    chmod as os_chmod,
    remove as os_remove,
)
from os.path import join as os_path_join
import subprocess
import shutil


from mv1fw import (
    create_dir,
    CogManager,
    Logger,
)

from ..._impl.conf import Conf




class PythonRunBase:
    """
    Base class for routines run using a Python interpreter
    and a Python virtual environment, and also for
    queueing such jobs via SLURM.

    Parameters:

        location (:any:`Location`):
            Location for outputs.
        filename (string):
            A filename, e.g., `abcd.py`, in the local directory
            along with the script where Run instance is created.
        source_names (list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files.
        cluster_tasks (optional pair of integers):
            A pair (number of nodes, number of tasks per node)
            setting up a simple distributed node topology.
        job_time_limit: optional string
            a time limit (default: 1 day).
            Via `SLURM documentation <https://slurm.schedmd.com/sbatch.html#OPT_time>`_:
            A time limit of zero requests that no time limit be imposed.
            Acceptable time formats include "minutes", "minutes:seconds",
            "hours:minutes:seconds", "days-hours", "days-hours:minutes"
            and "days-hours:minutes:seconds".
        partition: optional string
            partition name
        SLURM_args:
            Other options desired for
            a SLURM nonblocking execution.
            See (possibly) https://slurm.schedmd.com/sbatch.html
            or elsewhere for help/assistance.
        verbose:
            Whether to run verbosely.

    """

    def __init__(
            self,
            location,
            filename,
            source_names = None,
            #####
            venv_manager = None,
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
        source_names_ = source_names if isinstance(source_names, list) else [] if source_names is None else [source_names]
        self.source_names = [filename] + source_names_
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
        self.wdir = os_getcwd()
        # The class is stateless (for repeated runs). We change the working directory
        # in order to run Python files that implicitly drop files in the working directory.
        # > store the working directory at instantiation
        self.wdir_stateless = self.wdir
        if location.jumplist:
            self.wdir = os_path_join(self.wdir, *location.jumplist)
        # > for printing only
        self.handle_list = location.get_handle_list()
        # self.handle = '/'.join(handle_list) if handle_list else 'handle_not_specified'
        # > virtual environment
        self.venv_manager = conf['venv_manager'] if venv_manager is None else venv_manager
        self.env = conf['env']
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
        self.cog = None
        self.dat_stem = "dat"
        self.info_stem = "info"
        self.script_name = "swh"
        self.info_dir = None
        self.data_dir = None

        # todo accord with other runbase types
        self.log = Logger()
        self.log_err = Logger()


    def start(
            self,
            program_args = None,
            venv_args = None,
    ):
        """
        Run via the specified Python interpreter.

        Arguments:

            program_args (optional list of string):
            venv_args (optional list of string):

        """
        self._init()
        venv_args_ = venv_args if venv_args is not None else []
        program_args_ = program_args if program_args is not None else []
        self._prerun()
        self._run(program_args_, venv_args_)
        self._postrun()

    def _init(self):
        # > create output tree, now deleting all old data, if any exists
        self.cog = CogManager(
            output_abs_path_root=self.path,
        )
        # > a somewhat ad-hoc initialization of the cog manager,
        #  because we don't have the convenience of actions.
        self.cog.clean_tree()
        self.info_dir = create_dir(self.cog.engine_dir, self.info_stem)
        self.data_dir = create_dir(self.cog.engine_dir, self.dat_stem)
        # > store a copy of the source files
        self.copy_files(self.source_names, directory='info')


    def _deinit(self):
        # > restore working directory, assuming even postrun might change it
        os_chdir(self.wdir_stateless)


    def copy_files(self, names, directory):
        """
        Copy files described using relative filenames
        (relative to the input working directory)
        to a directory defined by the class
        (e.g., data directory or info directory, 'info' or 'data' resp.)
        The method will ensure directories exist.

        Arguments:

            names (list of string):
                list of relative filenames
            directory (string):
                descriptor for a location in the output tree

        """
        if directory == 'info':
            tgt_dir = self.info_dir
        elif directory == 'data':
            tgt_dir = self.data_dir
        else:
            raise ValueError
        for file in names:
            # todo windows
            fsplit = file.split('/')
            create_dir(tgt_dir, fsplit[:-1])
            shutil.copy(
                os_path_join(
                    self.wdir,
                    file,
                ),
                os_path_join(
                    tgt_dir,
                    file,
                ),
            )

    def _prerun(self):
        pass

    def _postrun(self):
        pass

    def _run(
            self,
            program_args,
            venv_args,
    ):
        """

        Arguments:

            program_args (list of string):

        """
        target_name = self.source_names[0]
        # > quick and dirty solution todo fix
        self.copy_files(self.source_names, directory='data')
        os_chdir(self.data_dir)
        if self.venv_manager == "conda":
            python_invoc = ["conda", "run", f"-n{self.env}", "python"]
        elif self.venv_manager == "venv":
            # todo impl
            # Every time I think of doing this I get stuck
            # trying to make sense of venv
            raise NotImplementedError(f"[PythonRunBase] venv is not supported.")
        elif self.venv_manager is None:
            # If you are here you probably know that
            # you shouldn't be running Python this way.
            python_invoc = ["python"]
        else:
            raise NotImplementedError(f"[PythonRunBase] unrecognized environment setup.")
        python_invoc += venv_args
        if not self.SLURM_nonblocking:
            # > run and block until completion
            alist = python_invoc + [target_name] + program_args
            self._msg(f" < {' '.join(alist)}", function="run", always=True)
            try:
                cp = subprocess.run(
                    alist,
                    capture_output=True,
                )
                s = cp.stdout.decode("utf-8").strip()
                self._msg(f" > {s}", function="run", always=True)
                self._log(target_name, "out", s)
                s = cp.stderr.decode("utf-8").strip()
                if s:
                    self._msg(f" 2> {s}", function="run", always=True)
                self._log(target_name, "err", s)
            except subprocess.CalledProcessError as exc:
                self._msg(f"return code {exc.returncode}\n{exc}", function="run", always=True)
        else:
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
            script_list = ["srun"] + slurm_args + python_invoc + [target_name] + program_args
            script = f"#!/bin/bash\n\n{' '.join(script_list)}\n"
            # > create batch script
            script_name = os_path_join(self.script_name)
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
                self._log(self.script_name, "out", s)
                s = cp.stderr.decode("utf-8").strip()
                if s:
                    self._msg(f" 2> {s}", function="run", always=True)
                self._log(self.script_name, "err", s)
            except subprocess.CalledProcessError as exc:
                self._msg(f"return code {exc.returncode}\n{exc}", function="run", always=True)
            # > talk to the user
            self._msg(f"Submitted job to SLURM queue.")
        # > restore state
        # todo unmake subdirs in data dir from this copy - this is a placeholder to inspire something better later
        for srcn in self.source_names:
            os_remove(os_path_join(self.data_dir, srcn))
        os_chdir(self.wdir)


    def _msg(self, body, function = None, always = False, as_is = False):
        loc_list = [self.__class__.__name__]
        if function is not None:
            loc_list += [function]
        if self.v and not always:
            loc_list += ["verbose"]
        message = body if as_is else f"[{':'.join(loc_list)}] {body}"
        if self.v or always:
            print(message)


    def _log(self, command, tag, body):
        out = self.cog.filename(
            handle=f"{command}.{tag}",
            ending="txt",
            stem=self.info_stem,
        )
        with open(out, "w") as f:
            f.write(body)


