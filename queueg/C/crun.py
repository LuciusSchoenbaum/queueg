






from .C_impl.crunbase import CRunBase

class CRun(CRunBase):
    """
    Running routine for multi-file or single-file code written in C.

    Parameters:
        filename (string):
            A filename, e.g., `abcd.c`, in the local directory
            along with the script where Run instance is created.
        source_names (optional list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files or header files.
        location (:any:`Location`):
            Output path.
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
            wdir_filenames = None,
            include_path_handles = None,
            library_path_handles = None,
            library_handles = None,
    ):
        super().__init__(
            filename = filename,
            location = location,
            source_names = source_names,
            wdir_filenames = wdir_filenames,
            compiler_name = "gcc",
            always_compiler_args = ["-std=c99", "-Wall", "-O3"],
            exec_name = None,
            include_path_handles = ["crun_include"] if include_path_handles is None else ["crun_include"]+include_path_handles,
            library_path_handles = ["crun_lib"] if library_path_handles is None else ["crun_lib"]+library_path_handles,
            library_handles = ["m"] if library_handles is None else ["m"]+library_handles,
        )



