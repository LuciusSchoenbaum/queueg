



from os import (
    remove as os_remove,
    chdir as os_chdir,
    getcwd as os_getcwd,
    chmod as os_chmod,
)
from os.path import join as os_path_join
import subprocess
import shutil

from mv1fw import (
    create_dir,
    CogManager,
)

from ..._impl.conf import Conf





class CRunBase:
    """
    Base class for routines run using a GCC-style compilation toolchain.

    Parameters:

        location (:any:`Location`):
            Location for outputs.
        filename (string):
            A filename, e.g., `abcd.c`, in the local directory
            along with the script where Run instance is created.
        source_names (optional list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files or header files.
        wdir_filenames (optional string or list of strings):
            Pass a filename or list of filenames that should be copied
            to the working directory of the program's execution
            from the repository from where the code is launched.
            todo more documentation/examples - clarify meaning of "filename"
        compiler_name (string):
            A compiler name. Default: ``gcc``.
        exec_name (optional string):
            Set if a compiled output must be run by a parent binary,
            the "executable" name. For example, MPI requires ``mpirun`` or ``mpiexec``.
            An exceptional routine will be launched if ``exec_name`` is
            set to "sbatch".
        include_path_handles (optional list of string):
            Names of include path handles. Paths themselves are set in
            the Conf file. The current working directory can also be
            added with the handle "wdir" or "cwd".
        library_path_handles:
            Names of library path handles. Typically, a directory
            called .../lib is where libraries for projects can be found.
            E.g., ["$PROJECT/lib"]
            The compiler adds $PROJECT/lib to its list of directories to try.
        library_handles:
            Names of libraries. E.g. ["foo"].
            In the cleanest, simplest case, the compiler goes on a search
            for a file "libfoo.a", but other things can happen.
        cluster_tasks (optional pair of integers):
            A pair (number of nodes, number of tasks per node)
            setting up a simple distributed node topology.
        job_time_limit (optional string):
            a time limit (default: 1 day).
            Via `SLURM documentation <https://slurm.schedmd.com/sbatch.html#OPT_time>`_:
            A time limit of zero requests that no time limit be imposed.
            Acceptable time formats include "minutes", "minutes:seconds",
            "hours:minutes:seconds", "days-hours", "days-hours:minutes"
            and "days-hours:minutes:seconds".
        partition (optional string):
            partition name, for a partitioned system
        SLURM_args (optional list of string):
            Other options desired for
            a SLURM nonblocking execution.
            See e.g. `SLURM sbatch documentation <https://slurm.schedmd.com/sbatch.html>`_ .
        verbose (boolean):
            Whether to run verbosely. Default: False
    """

    # todo documentation for adding lines to config file for
    #  includes (incl. the source include)
    #  libraries
    #  library paths
    #
    # todo windows?
    #
    # todo on some systems, "module load mpi" is necessary

    def __init__(
            self,
            location,
            filename,
            source_names = None,
            #######
            wdir_filenames = None,
            compiler_name = None,
            always_compiler_args = None,
            exec_name = None,
            include_path_handles = None,
            library_path_handles = None,
            library_handles = None,
            #####
            cluster_tasks = None,
            job_time_limit = "1-00:00:00",
            partition = None,
            SLURM_args = None,
            verbose = False,
    ):
        if location is None:
            raise ValueError("You must specify an output location.")
        self.source_names = [filename]
        self.source_names += source_names if isinstance(source_names, list) else [] if source_names is None else [source_names]
        self.wdir_filenames = wdir_filenames if isinstance(wdir_filenames, list) else [] if wdir_filenames is None else [wdir_filenames]
        # todo add self.source_names_fullpath and self.wdir_filenames_fullpath
        self.compiler_name = compiler_name if compiler_name is not None else "gcc"
        self.always_compiler_args = always_compiler_args if always_compiler_args is not None else ["std=c99", "-Wall", "-O3"]
        self.exec_name = [] if exec_name is None else [exec_name] if exec_name is isinstance(exec_name, str) else exec_name
        self.include_path_handles = include_path_handles if include_path_handles is not None else []
        self.library_path_handles = library_path_handles if library_path_handles is not None else []
        self.library_handles = library_handles if library_handles is not None else []
        self.conf = Conf()
        self.path = location.get_path(
            create = True,
            explicit_conf = self.conf,
        )
        self.wdir = os_getcwd()
        # > the class needs to be stateless, and we change
        #   the working directory
        # todo fix - we shouldn't do this ?
        self.wdir_stateless = self.wdir
        if location.jumplist:
            self.wdir = os_path_join(self.wdir, *location.jumplist)
        # > whether to execute via slurm queue, in nonblocking mode.
        # > In other words, to queue the job instead of watching it run.
        self.SLURM_nonblocking = True if exec_name == "sbatch" else False
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
        self.module_invocation = ""
        ######################
        self.cog = None
        self.include_paths = []
        self.library_paths = []
        self.dat_stem = "dat"
        self.info_stem = "etc"
        self.analysis_stem = "ans"
        self.lib_stem = "lib"
        self.target_name = "_a"
        self.script_name = "swh"
        self.lm = True
        self.info_dir = None
        self.data_dir = None
        self.lib_dir = None
        self.add_include_path_handles(self.include_path_handles)
        self.add_library_path_handles(self.library_path_handles)
        # set to stop flow of processing steps early (utility for subclasses)
        self.stop_flow = False



    def init(self):
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
        # Note: the executable will be run inside the data directory.
        # todo - reminder, here, we are moving files into place
        #  that the code wants to have so it can compile - because
        #  the code is using 'quote includes'.
        self.copy_files(self.wdir_filenames, directory='data')
        # todo .
        #  Originally, I implemented by moving everything
        #  to a place in the output tree and then compile+run.
        #  This, however, broke when I added `jump` to the location.
        #  It is patched up now, but,
        #  it reveals some hidden issues with the previous implementation.
        #  What I would like to do is this: revise the CRunBase implementation so that:
        #  - we create an image of the input source in the info_dir during __init__.
        #  - we compile in the info_dir and tell compiler to route the executable
        #    to the info_dir, where it can stay (just leave it, it's probably not a large file)
        #  - this is appropriate for small sources (dense scripts that control+direct libraries,
        #     with a few auxiliary files perhaps).
        #  - the user has the option not to do any of this, and instead compile
        #    directly in the input directory tree, and produce an executable that is
        #    deleted after it is run. In this case, nothing is copied to the info_dir.
        #    This is for larger source trees. <---that's a case that should be walled
        #    off and developed separately.

        # This was an idea but I backed out of it - delete later?
        # > make the rest of the code path independent
        # self.source_names = [os_path_join(self.wdir, name) for name in self.source_names]
        # self.wdir_filenames = [os_path_join(self.wdir, name) for name in self.wdir_filenames]


    def deinit(self):
        # > restore working directory
        # todo re-implement with no call to os_chdir.
        #  Using os_chdir creates a ticking time bomb
        #  when you are offering methods to subclasses and users
        #  (like postrun, precompilation, postcompilation)
        #  and it's opaque what the wdir is at any given moment/stage
        #  of process flow.
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
        # todo
        if directory == 'info':
            tgt_dir = self.info_dir
        elif directory == 'data':
            tgt_dir = self.data_dir
        else:
            raise ValueError
        for file in names:
            # # todo windows
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


    def add_include_path_handles(self, include_path_handles):
        handle_list = include_path_handles if isinstance(include_path_handles, list) else [include_path_handles]
        for handle in handle_list:
            if handle in self.conf:
                paths = self.conf[handle]
                if isinstance(paths, list):
                    self.include_paths += paths
                else:
                    self.include_paths.append(paths)
            elif handle == "wdir" or handle == "cwd":
                self.include_paths += [self.wdir]


    def add_library_path_handles(self, library_path_handles):
        handle_list = library_path_handles if isinstance(library_path_handles, list) else [library_path_handles]
        for handle in handle_list:
            if handle in self.conf:
                paths = self.conf[handle]
                if isinstance(paths, list):
                    self.library_paths += paths
                else:
                    self.library_paths.append(paths)


    def add_library_handles(self, library_handles):
        handle_list = library_handles if isinstance(library_handles, list) else [library_handles]
        self.library_handles += handle_list


    def archive(
            self,
            handle,
            source_names,
            compiler_args=None,
            include_paths=None,
            library_paths=None,
            library_handles=None,
            include_path_handles = None,
            library_path_handles = None,
    ):
        """
        Generate a static library, also known as an archive,
        and prepare for it to be linked to the main target
        compiled during the start() method.

        Usually, a static library is a file "libfoo.a" with .a ending.
        Compilers make (and then break) a lot of assumptions
        about these filenames.

        Arguments:

            handle:
                Handle for the target static library.
                Idiomatically, handle "foo" generates a file "libfoo.a".
            source_names (string or list of strings):
            include_paths:
            library_paths:
            library_handles:
            include_path_handles:
            library_path_handles:
            compiler_args:
        """
        # > if lib dir was not created, create it
        self.lib_dir = create_dir(self.cog.engine_dir, self.lib_stem)
        # > "push" former state to "stack"
        restore_ips = self.include_paths
        restore_lps = self.library_paths + [self.lib_dir]
        restore_ls = self.library_handles + [handle]
        # > marshall compiler arguments
        compiler_args_ = []
        compiler_args_ += self.always_compiler_args + ['-c']
        compiler_args_ += compiler_args if compiler_args is not None else []
        self.include_paths = include_paths if isinstance(include_paths, list) else [] if include_paths is None else include_paths
        self.library_paths = library_paths if isinstance(library_paths, list) else [] if library_paths is None else library_paths
        self.library_handles = library_handles if library_handles is not None else []
        if include_path_handles is not None:
            self.add_include_path_handles(include_path_handles)
        if library_path_handles is not None:
            self.add_library_path_handles(library_path_handles)
        # > call compiler
        self._compilation(
            compiler_name=self.compiler_name,
            target_name=os_path_join(self.lib_dir, handle + ".o"),
            source_names=source_names,
            compiler_args=compiler_args_,
            include_paths=self.include_paths,
            library_paths=self.library_paths,
            library_handles=self.library_handles,
        )
        ofile = os_path_join(self.lib_dir, handle + ".o")
        # todo ensure ^--this subprocess waits for v--that subprocess? 99% sure ok.
        # ar -rcs libfoo.a foo.o
        self._archive_generation(
            target_name=os_path_join(self.lib_dir, "lib" + handle + ".a"),
            source_names=[ofile]
        )
        # > restore state, but with the newly created library associated with the run target
        self.include_paths = restore_ips
        self.library_paths = restore_lps
        self.library_handles = restore_ls
        os_remove(ofile)


    def start(
            self,
            program_args = None,
            compiler_args = None,
    ):
        """
        Compile and run.

        Arguments:

            program_args (optional string or list of string):
                arguments to script (default: None will pass no arguments).
                Should appear as a list of strings.
            compiler_args (optional list of string):
                arguments ("flags") to compiler (default: None will use -Wall -O2).
                Note: compiler options should appear inside of a list,
                something like: ``['-v', '-Wall', '-g']`` for example,
                including the dash symbol.
        """
        self.init()
        compiler_args_ = self.always_compiler_args
        compiler_args_ += compiler_args if compiler_args is not None else []
        program_args_ = program_args if program_args is not None else []
        if not self.stop_flow:
            self._precompilation()
        if not self.stop_flow:
            self._compilation(
                compiler_name=self.compiler_name,
                target_name=os_path_join(self.data_dir, self.target_name),
                source_names=self.source_names,
                compiler_args=compiler_args_,
                include_paths=self.include_paths,
                library_paths=self.library_paths,
                library_handles=self.library_handles,
            )
        if not self.stop_flow:
            self._postcompilation()
        if not self.stop_flow:
            self._run(program_args_)
        if not self.stop_flow:
            self._postrun()
        self.deinit()


    def _precompilation(self):
        pass


    def _postcompilation(self):
        pass


    def _postrun(self):
        pass


    def _compilation(
            self,
            compiler_name,
            target_name,
            source_names,
            compiler_args,
            include_paths,
            library_paths,
            library_handles,
    ):
        """
        Run the compiler executable.
        Once we arrive here, all the arguments are lists of strings,
        except for the target name.

        Arguments:

            compiler_name (string):
            target_name (optional string):
                Target of compilation, if `None` the compiler's default is used
            source_names (list of string):
            compiler_args (list of string):
            include_paths (list of string):
            library_paths (list of string):
            library_handles (list of string):
        """
        os_chdir(self.wdir)
        cc = compiler_name
        # todo for some reason this isn't working with the subprocess idiom...
        #  I will try adding this module load to my bashrc on the machine of interest... :/
        # module_prefix = "module try-load mpi/mpich-x86_64" # for some reason a list won't work?????
        # cclist = [module_prefix, "&&", cc]
        cclist = [cc]
        cclist += compiler_args
        # > compile to the data directory, and all emitted files land there.
        cclist += [f"-I{x}" for x in include_paths]
        cclist += source_names
        cclist += [f"-L{x}" for x in library_paths]
        cclist += [f"-l{x}" for x in library_handles]
        if target_name is not None:
            cclist += [f"-o{target_name}"]
        if len(source_names) > 1:
            self._msg(f"source:", function=cc, always=True)
            self._msg('\n'.join(source_names), as_is=True, always=True)
        else:
            self._msg(f"source: {source_names[0]}", function=cc, always=True)
        if len(include_paths) > 0:
            self._msg(f"include paths:", function=cc, always=True)
            self._msg('\n'.join(include_paths), as_is=True, always=True)
        if len(library_paths) > 0:
            self._msg(f"library paths:", function=cc, always=True)
            self._msg('\n'.join(library_paths), as_is=True, always=True)
        if len(library_handles) > 4:
            self._msg(f"libraries:", function=cc, always=True)
            self._msg('\n'.join(library_handles), as_is=True, always=True)
        else:
            self._msg(f"libraries: {' '.join(library_handles)}", function=cc, always=True)
        self._msg(f" < {' '.join(cclist)}", function=cc, always=True)
        try:
            cp = subprocess.run(
                args=cclist,
                capture_output=True,
                # Needed if calling module
                # shell=True,
            )
            s = cp.stdout.decode("utf-8").strip()
            self._msg(f" > {s}", function=cc, always=True)
            self._log(cc, "out", s)
            s = cp.stderr.decode("utf-8").strip()
            if len(s) > 0:
                self._msg(f" 2> {s}", function=cc, always=True)
            self._log(cc, "err", s)
        except subprocess.CalledProcessError as exc:
            self._msg(f" return code {exc.returncode}\n{exc}", function=cc, always=True)
        # todo check if target_name exists, if not, stop.


    def _archive_generation(
            self,
            target_name,
            source_names,
    ):
        """
        Create an archive.
        Consult the man pages for `ar` for more information.

        Arguments:

            target_name (string):
                This should be "libfoo.a" or the like, not "foo" (not the library handle).
            source_names (list of string):
                Object file(s) (their names in cwd, or full paths).

        """
        # todo sources? can there be more than one?

        cc = "ar" # todo for now
        compiler_args = ["-rcs"] # todo for now
        cclist = [cc]
        cclist += compiler_args
        cclist += [target_name]
        cclist += source_names
        self._msg(f"source: {' '.join(source_names)}", function=cc, always=True)
        self._msg(f" < {' '.join(cclist)}", function=cc, always=True)
        try:
            cp = subprocess.run(
                args=cclist,
                capture_output=True,
            )
            s = cp.stdout.decode("utf-8").strip()
            self._msg(f" > {s}", function=cc, always=True)
            self._log(cc, "out", s)
            s = cp.stderr.decode("utf-8").strip()
            if len(s) > 0:
                self._msg(f" 2> {s}", function=cc, always=True)
            self._log(cc, "err", s)
        except subprocess.CalledProcessError as exc:
            self._msg(f" return code {exc.returncode}\n{exc}", function=cc, always=True)




    def _run(self, program_args):
        """
        Run the executable that is generated during the start() routine,
        as part of that routine.

        Arguments:

            program_args (list of string):

        """
        # Note: the executable was placed in the data directory.
        # > chdir to data directory
        os_chdir(self.data_dir)
        if not self.SLURM_nonblocking:
            # > run and block until completion
            alist = self.exec_name + [f"./{self.target_name}"] + program_args
            self._msg(f" < {' '.join(alist)}", function="run", always=True)
            try:
                cp = subprocess.run(
                    args=alist,
                    capture_output=True,
                    )
                s = cp.stdout.decode("utf-8").strip()
                self._msg(f" > {s}", function="run", always=True)
                self._log(self.target_name, "out", s)
                s = cp.stderr.decode("utf-8").strip()
                if s:
                    self._msg(f" 2> {s}", function="run", always=True)
                self._log(self.target_name, "err", s)
            except subprocess.CalledProcessError as exc:
                self._msg(f"return code {exc.returncode}\n{exc}", function="run", always=True)
            os_remove(f"./{self.target_name}")
        else:
            # > non-blocking execution via SLURM.
            slurm_args = []
            # > how many nodes, how many tasks per node
            slurm_args += [f"--nodes={self.cluster_tasks[0]}", f"--ntasks={self.cluster_tasks[1]}"]
            # > time limit
            slurm_args += [f"--time={self.job_time_limit}"]
            # > partition
            slurm_args += [f"--partition={self.partition}"]
            # > user-requested args
            slurm_args += self.SLURM_args
            self._msg(f"nonblocking (batch) mode with arguments {' '.join(slurm_args)}.", function="run", always=True)
            script_list = ["srun"] + slurm_args + [str(os_path_join(self.data_dir, self.target_name))] + program_args
            script = f"#!/bin/bash\n\n{self.module_invocation}\n{' '.join(script_list)}\n"
            # > create batch script
            script_name = os_path_join(self.data_dir, self.script_name)
            with open(script_name, "w") as f:
                f.write(script)
            # > chmod +x
            os_chmod(script_name, 0o755)
            # > build alist (talk to slurm)
            alist = ["sbatch", script_name]
            self._msg(f" < {' '.join(alist)}", function="run", always=True)
            try:
                cp = subprocess.run(
                    args=alist,
                    capture_output=True,
                    # shell=True,
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
            self._msg(f"Submitted job to SLURM queue.", function="run", always=True)
        self._msg(f"output path:\n{self.path}", function="run", always=True)
        # > restore state
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



