



class Mode:
    """

    For a remote system with SLURM,
    it is possible to execute the target in non-blocking mode,
    or in the normal way on a single-processor system.
    These are referred to as running in "indirect" mode
    and in "direct" mode, respectively.
    To use :any:`Mode`, invoke one of the two possible modes,
    namely ``direct`` or ``indirect``, for example:

    .. code-block::

        mode = queueg.direct(),
        ...

    The ``direct`` mode is the simplest mode
    (intuitively and for implementation):
    run the target and block until completion
    on both the host and the local node.
    For a ``direct`` mode run, arguments are never used.

    The ``indirect`` mode is only supported on
    a remote system with SLURM, ITCINOOD.
    In this case, after the target process has launched, it is assumed
    that the job is now being managed by SLURM.
    All that is left to do, then, is "watch" the queue
    before closing the run_remote procedure.
    This closing procedure has a few possible ("expected") outcomes:

        - the job launched and is running now.
        - the job launched and completed successfully while you watched, and you pulled the output.
        - the job failed to launch and you caught it, and pulled the output for diagnostics.
        - the job failed to launch and you didn't watch long enough, and you aren't aware.

    All except for the final outcome are acceptable. Therefore, steps
    are taken to prevent this outcome, namely, an "issue check".

    .. note::

        Changing the mode from direct to indirect does nothing except to
        change the behavior once the target has completed.
        The target of the run is responsible for invoking SLURM.

    Arguments:

        check_until (integer or TIME):
            If not None, deploys a run (possibly remotely) that will execute
            nonblocking via SLURM. Immediately after a nonblocking SLURM run,
            the queue is monitored for `check_until` quantity of time,
            This is typ. for the purpose of an issue check, ensuring
            a successful launch of the critical section of the code.
            For TIME see :any:`Sync`. (Default: ``20``)
        check_every (integer or TIME):
            Frequency (in seconds) of SLURM queue queries during issue check.
            Does not correspond to wall time; the thread will sleep for this long.
            For TIME see :any:`Sync`. (Default: ``5``)

    """
    def __init__(
            self,
            check_until = 20,
            check_every = 5,
    ):
        self.check_until = check_until
        self.check_every = check_every






class direct(Mode):
    """

    :meta private:

    """



class indirect(Mode):
    """

    :meta private:

    """






