






from .C_impl.crunbase import CRunBase


class MPIRun(CRunBase):
    """
    Running routine for codes written in C99 using MPI.

    .. note::
        `Petsc <https://petsc.org/>`_, if installed and configured via QueueG
        can be included via the helper ``petsc()``.

    Arguments:
        filename (string):
            A filename, e.g., `abcd.c`, in the local directory
            along with the script where Run instance is created.
        location (:any:`Location`):
            Output path.
        source_names (optional list of string):
            Filenames (relative paths or absolute paths)
            of supplementary source code files or header files.
        wdir_filenames (optional string or list of strings):
            Pass a filename or list of filenames that should be copied
            to the working directory of the program's execution
            from the repository from where the code is launched.
        n (optional integer):
            number of processes.
        cluster_tasks (optional pair of integers):
            A pair (number of nodes, number of tasks per node)
            setting up a simple distributed node topology.
            If set, overrides the value of n and triggers and triggers a SLURM
            non-blocking run (via sbatch).
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
        MPI_module_name (string):
            Name of MPI module to be invoked via
            "module try-load {mpi-name}".
            ICIH: Sometimes there are flavors of MPI available
            and using a different flavor can resolve an issue.
            Try "module avail" to query modules on your system.
    """

    # todo improve documentation on the helper petsc()

    def __init__(
            self,
            filename,
            location,
            source_names = None,
            wdir_filenames = None,
            # todo a good default value of n
            n = 4,
            #########
            cluster_tasks = None,
            job_time_limit = None,
            partition = None,
            SLURM_args = None,
            MPI_module_name = "mpi",
    ):
        super().__init__(
            filename=filename,
            location=location,
            source_names=source_names,
            wdir_filenames=wdir_filenames,
            compiler_name = "mpicc",
            always_compiler_args = ["-std=c99", "-Wall", "-O3"],
            exec_name = ["mpiexec", "-n", str(n)] if cluster_tasks is None else "sbatch",
            include_path_handles = ["cmpirun_include"],
            library_path_handles = ["cmpirun_lib"],
            library_handles = ["mpi", "m"],
            cluster_tasks=cluster_tasks if cluster_tasks is not None else (1, n),
            job_time_limit= job_time_limit,
            partition = partition,
            SLURM_args = SLURM_args,
        )
        self.module_invocation += f"if command -v module &> /dev/null\nthen\n\tmodule try-load {MPI_module_name}\nfi\n"


    def load_module(self, module="mpi"):
        # todo call "load module mpi", currently this is obviated (??) by if ... try-load, cf. self.module_invocation
        raise NotImplementedError


    def petsc(self):
        self.add_include_path_handles("petsc_include")
        self.add_library_path_handles("petsc_lib")
        self.add_library_handles("petsc")


