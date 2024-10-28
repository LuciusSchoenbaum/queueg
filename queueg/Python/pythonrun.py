




from .Python_impl.pythonrunbase import PythonRunBase




class PythonRun(PythonRunBase):
    """
    Run using a Python interpreter
    and a Python virtual environment, and optionally
    queue such jobs via SLURM.

    Parameters:

        filename (string):
            A filename, e.g., `abcd.py`, in the local directory
            along with the script where Run instance is created.
        location (:any:`Location`):
            Location for outputs.
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
            #####
            cluster_tasks = None,
            job_time_limit = "1-00:00:00",
            partition = None,
            SLURM_args = None,
            #####
            verbose = False,
    ):
        super().__init__(
            location=location,
            filename=filename,
            source_names=source_names,
            ####
            venv_manager=venv_manager,
            ####
            cluster_tasks=cluster_tasks,
            job_time_limit=job_time_limit,
            partition=partition,
            SLURM_args=SLURM_args,
            ####
            verbose=verbose,
        )




