


from ..C.C_impl.crunbase import CRunBase



class CppRun(CRunBase):
    """
    Running routine for single-file or multi-file codes written in C++.

    .. note::
        This is a stub in version |version|.

    Parameters:
        filename (string):
            A filename, e.g., `abcd.c`, in the local directory
            along with the script where Run instance is created.
        source_names (optional list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files or header files.
        location (:any:`Location`):
            Output path.
        cpp_version (string): integer specifying
            C++ version, e.g. '17', '20'.

    """

    def __init__(
            self,
            filename,
            location,
            source_names = None,
            cpp_version = '17',
    ):
        super().__init__(
            filename = filename,
            location = location,
            source_names = source_names,
            compiler_name = "gcc",
            always_compiler_args = [f"-std=cxx{cpp_version}", "-Wall", "-O3"],
            exec_name = None,
            include_path_handles = ["cpprun_include"],
            library_path_handles = ["cpprun_lib"],
            library_handles = ["m"],
        )



