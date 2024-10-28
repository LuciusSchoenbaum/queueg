


from .C_impl.crunbase import CRunBase

from os import \
    remove as os_remove



class BasiliskRun(CRunBase):
    """
    Running routine for the `Basilisk <https://basilisk.fr>`_ PDE solver
    that places files in the index directory tree.
    Basilisk provides makefiles for running,
    but the output conventions differ from those
    of PyPinnch, so may be convenient to run
    Basilisk via a Python script.

    Usage: create the BasiliskRun instance, then call start().

    .. note::
        To help localize a problem, try qcc-compilation
        with the -event flag (`qcc -Wall -O2 -event ....`)

    .. warning:: (about multiple file compilation):
        qcc does not accept more than one file, the "script". (AFAIK.)
        As a result, all Basilisk C must go there.
        Other files can be included in the compilation,
        but they must be C99 - typically these might be functions
        called at points during the main script.

    Parameters:
        filename (string):
            A filename, e.g., `abcd.c`, in the local directory
            along with the script where BasiliskRunner instance is created.
        location (:any:`Location`):
            Output path.
        source_names (optional list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files or header files.
        qcc_args (optional list of string):
            Arguments to qcc compiler.
        emit_c_source_only (boolean):
            Emit the C source file compiled by the Basilisk
            qcc compiler, and exit. The file will appear as ``_{source_name}``.
        wdir_filenames (optional string or list of strings):
            Pass a filename or list of filenames that should be copied
            to the working directory of the program's execution
            from the repository from where the code is launched.
    """

    def __init__(
        self,
        filename,
        location,
        source_names = None,
        qcc_args = None,
        emit_c_source_only = False,
        wdir_filenames = None,
    ):
        super().__init__(
            filename=filename,
            location=location,
            source_names=source_names,
            wdir_filenames=wdir_filenames,
            compiler_name = "gcc",
            always_compiler_args = ["-std=c99", "-D_XOPEN_SOURCE=700", "-Wall", "-O3"],
            exec_name = None,
            include_path_handles = ["basiliskrun_include"],
            library_path_handles = ["basiliskrun_lib"],
            library_handles = ["m"],
        )
        self.basilisk_script = filename
        # We always proceed by generating the source
        # and generating source code using a C compiler in a separate step.
        # This is purely for the sake of code re-use.
        self.qcc_args = ["-source", "-Wall", "-O3"]
        self.qcc_args += qcc_args if isinstance(qcc_args, list) else []
        self.emit_c_source_only = emit_c_source_only


    def _precompilation(self):
        self._compilation(
            compiler_name="qcc",
            target_name=None,
            source_names=[self.basilisk_script],
            compiler_args = self.qcc_args,
            include_paths=self.include_paths,
            library_paths=[],
            library_handles=[],
        )
        self.source_names[0] = f"_{self.basilisk_script}"
        if self.emit_c_source_only:
            self.stop_flow = True

    def _postcompilation(self):
        pass


    def _postrun(self):
        # > copy qcc source(s) to info directory (for record-keeping)
        self.copy_files([f"_{self.basilisk_script}"], directory='info')
        # > remove qcc sources from working directory
        os_remove(f"_{self.basilisk_script}")
        # This almost certainly isn't necessary, but:
        # > restore the state to the expected one in the superclass
        self.source_names[0] = self.basilisk_script


