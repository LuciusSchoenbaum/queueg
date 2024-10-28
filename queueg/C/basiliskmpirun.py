


from .mpirun import MPIRun

from os import \
    remove as os_remove

from sys import platform



class BasiliskMPIRun(MPIRun):
    """
    Run instance for Basilisk compiled
    and run via MPI.

    For documentation of parameters
    see :any:`MPIRun` and :any:`BasiliskRun`.

    """

    def __init__(
            self,
            filename,
            location,
            source_names = None,
            qcc_args = None,
            wdir_filenames = None,
            n = 4,
            #########
            cluster_tasks = None,
            job_time_limit = None,
            partition = None,
            SLURM_args = None,
    ):
        super().__init__(
            filename=filename,
            location=location,
            source_names=source_names,
            wdir_filenames=wdir_filenames,
            n = n,
            #########
            cluster_tasks = cluster_tasks,
            job_time_limit = job_time_limit,
            partition = partition,
            SLURM_args = SLURM_args,
        )
        self.basilisk_script = filename
        self.always_compiler_args += ["-D_MPI=1",]
        if platform == 'linux':
            self.always_compiler_args += ["-D_GNU_SOURCE=1"]
        elif platform == 'darwin':
            self.always_compiler_args += ["-D_POSIX_C_SOURCE=1"]
        else:
            # todo
            raise NotImplementedError
        self.qcc_args = ["-source", "-Wall", "-D_XOPEN_SOURCE=700", "-O3", "-D_MPI=1"]
        self.qcc_args += qcc_args if isinstance(qcc_args, list) else []
        self.add_include_path_handles("basiliskmpirun_include")
        self.add_library_path_handles("basiliskmpirun_lib")




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


