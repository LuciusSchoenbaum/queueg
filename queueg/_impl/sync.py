




import paramiko
import paramiko as pm

from os import (
    environ as os_environ,
    popen as os_popen,
    remove as os_remove,
    makedirs as os_makedirs,
)
from shutil import (
    copy as shutil_copy,
)
from os.path import (
    join as os_path_join,
    exists as os_path_exists,
    expanduser as os_path_expanduser,
)
import atexit
import time
from re import (
    split as re_split,
    compile as re_compile,
)

from mv1fw import (
    create_dir,
)

from .._impl.ossys import (
    prompt_user,
    default_conf_stemlist,
    current_jobs,
    conf_file,
    Age,
    cmd_stat_fmt,
    cmd_ls_fmt,
)
from .._impl.ossys.ossys import (
    job_status_pp,
    ttyRead,
)
# from .._impl.path import Path
from .._impl.conf import Conf
from .._impl.types import parse_time
from .._impl.mode import (
    Mode,
    direct,
    indirect,
)






class Sync:
    """
    A utility for synchronizing data organized by QueueG
    on a local and a remote host.
    Sync enables synchonization of index locations
    quickly and can also be used to target specific
    files or groups of files.
    It also provides low-level operations
    that could be used for routine work between
    server and host machines.

    .. note::

        Because Sync relies on `atexit <https://docs.python.org/3/library/atexit.html>`_,
        Sync internals will persist all created instances of Sync until
        Python is terminated. This includes the remote connection.

    .. warning::

        Use of Sync with publickey authentication is not guaranteed to
        be secure from sophisticated man-in-the-middle (MITM) attacks.
        **Use at your own risk.**
        If you have security expertise, please help by
        contributing improvements to QueueG!

    Parameters:

        login (string):
            A login of the form ``username@hostname``
        password (optional string):
            An explicit password or (if None) a password read from stdin.
            If a public key (``pkey``) is given, publickey authentication is
            used and the password is the "passphrase" used when you create
            the keys.
        twofa (boolean):
            If True, a twofa validation code will be read from stdin and
            used for authentication. (Default: False)
        pkey (optional string):
            file system path to private key, for publickey authentication.
        port (integer):
            A port for ssh (default 22)
        bare (boolean):
            Set True if you only wish to use as a basic or "bare" SSH/SFTP.
            In this case Sync will skip acquiring the remote conf file. (default: False)

    """

    # todo For publickey authentication, cf. Martin Prikryl's answer:
    #  https://stackoverflow.com/questions/10670217/paramiko-unknown-server#43093883

    def __init__(
            self,
            login,
            password = None,
            twofa = False,
            pkey = None,
            port = 22,
            bare = False,
            verbose = False,
    ):
        self.conf = Conf()
        # idea is that these are as-needed, just-in-time resources
        self._ssh = None
        self._sftp = None
        # needs to remain up while sftp is up
        self._transport0 = None
        self._transport = None
        # uname for target system
        self._uname = None
        # active SLURM jobs (list of job id numbers)
        self.jobs = []
        # script name for SLURM queue (no significance)
        self.script_name = "_swh"
        # working directory management -
        # a possible working directory on the remote target when a push occurs,
        # can be invoked with self.cd('last push')
        self.last_push_dir = None
        self.remote_wdir = None
        # verbose output
        self.v = verbose
        self.pkey_filename = pkey
        self.pkey = None
        if login != "loopback":
            username, host = login.split("@")
            self.username = username
            self.host = host
            # todo change target to target_cache_dir (?)
            self.target = self.target_name()
            if password is None or password == "":
                p = prompt_user("Password: ", quiet = True)
                self.password = p
            else:
                self.password = password
            if twofa:
                tfa = prompt_user("2FA Code: ", quiet = True)
            else:
                tfa = None
            self.twofa = tfa
            # todo: the process knows the (time-dependent) twofa, but now this must be used <---- task
            self.port = port
            if self.pkey_filename:
                # todo cases besides Ed25519
                self.pkey = paramiko.Ed25519Key.from_private_key_file(
                    filename=self.pkey_filename,
                    password=self.password,
                )
            # initialize
            self.init()
            # cleanup
            atexit.register(self.deinit)
            # initialize rconf (remote Conf file)
            if bare:
                self.rconf = None
            else:
                dlist = default_conf_stemlist + ["sync", self.target]
                HOME = os_environ["HOME"]
                if os_path_exists(os_path_join(HOME, *dlist)):
                    # cache directory found
                    self._msg("Conf cache directory found.")
                    cache_dir = os_path_join(HOME, *dlist)
                else:
                    # cache directory not found
                    self._msg("Conf cache directory not found - one will be created.")
                    # > create a cache directory
                    cache_dir = create_dir(HOME, dlist)
                cache_path = os_path_join(cache_dir, conf_file)
                self._cache_refresh(cache_path)
                self.rconf = Conf(explicit_path=cache_path)
        else:
            # loopback case
            self.username = None
            self.host = None
            self.target = None
            self.password = None
            self.twofa = False
            self.rconf = self.conf
        # Invariant: either host is defined or else Sync is in the loopback state.
        # Check:
        if self.host is None and self._sftp is not None:
            raise ValueError(f"Something wrong? Review Sync source code.")


    def target_name(self):
        return self.username + '--' + self.host


    def init(self):

        def twofa_keyboardinteractive_handler(title, instructions, prompt_list):
            print("Working with prompt list", prompt_list)
            plist = prompt_list
            if len(plist) == 0:
                return []
            if len(plist) > 1:
                raise pm.SSHException("Expecting one field only.")
            if "Password" in plist[0][0]:
                return [self.password]
            elif "Verification" in plist[0][0]:
                return [self.twofa]
            else:
                raise pm.SSHException("Unexpected prompt")

        class TwoFAKeyboardInteractiveHandler(pm.AuthSource):
            def __init__(self, username):
                self.username = username
            def authenticate(self, transport):
                transport.auth_interactive(
                    username=self.username, handler=twofa_keyboardinteractive_handler,
                )

        if self._ssh is None:
            self._ssh = pm.client.SSHClient()
            self._ssh.load_host_keys(os_path_expanduser('~/.ssh/known_hosts'))
            self._ssh.load_system_host_keys()
            # what to do if the host key is missing
            # self._ssh.set_missing_host_key_policy(pm.AutoAddPolicy())
            if self.twofa:
                # check for host key, password authentication,
                # and two-factor authentication, via keyboard-interactive
                self._ssh.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    auth_strategy=TwoFAKeyboardInteractiveHandler(username=self.username),
                )
            else:
                # check for host key, and password authentication
                self._ssh.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    key_filename=None,
                    pkey=self.pkey,
                )
            self._uname = self.ssh("uname", strip=True)
            self._msg(f"Connected to remote {self._uname} system.", always=True)
        if self._sftp is None:
            self._sftp = self._ssh.open_sftp()


    def deinit(self):
        if self._ssh is not None:
            self._ssh.close()
        if self._sftp is not None:
            self._sftp.close()
        if self._transport is not None:
            self._transport.close()


    def os_path_exists(self, path):
        if self.host is not None:
            output = self.ssh(f"test -e {path} && echo $?", strip=True)
            return output == '0'
        else:
            return os_path_exists(path)

    def mkdir_remote(self, path):
        if self.host is not None:
            self.ssh(f"mkdir -p {path}")
        else:
            try:
                os_makedirs(path, exist_ok=True)
            except OSError as error:
                print(f"Error creating directory {path}: {error}")


    def _sync1way_impl(
            self,
            candidates,
            pull = True,
    ):
        updated = []
        leftalone = []
        created = []
        transfer = self.get if pull else self.put
        for candidate in candidates:
            path1, age1, path2, age2 = candidate
            if self.os_path_exists(path2):
                if age2 < age1:
                    # target artifact has been updated.
                    # > overwrite
                    transfer((path1, path2))
                    updated.append(path2)
                else:
                    # > do nothing
                    leftalone.append(path2)
            else:
                transfer((path1, path2))
                created.append(path2)
        return updated, leftalone, created


    def pull(
            self,
            location = None,
            explicit_path = None,
            explicit_remote_path = None,
            pass_dirs = None,
    ):
        """
        Intuitively, pull something back from the remote system.
        More precisely, a sync operation that respects
        the last-modified information in the local system.

        In other words, it will pull files recursively to the local system,
        and perform checks on last-modified timestamps, if matching
        files are found in both locations. It will only pull back the
        updated files and the "new" ones.

        Not a full sync, because it leaves existing files in the target path,
        ITCINOOD.

        Arguments:

            location (:any:`Location`):
            explicit_path (optional string):
            explicit_remote_path (optional string):
            pass_dirs (optional list of string):

        """
        # > build target path
        tgt_path = location.get_path(
            create=True,
            explicit_conf=self.conf,
        ) if explicit_path is not None else explicit_path
        if self.host:
            # > build source path
            src_path = location.get_path(
                create=False,
                explicit_conf=self.rconf,
            ) if explicit_remote_path is None else explicit_remote_path
            self._msg(f"Underway.\n[remote]{src_path}\n\t|VVV|\n{tgt_path}", function=self.pull.__name__, always=True)
            # > read the remote system, get all the candidates
            candidates = self.get_candidates_remote(
                src_path=src_path,
                tgt_path=tgt_path,
                pass_dirs=[] if pass_dirs is None else pass_dirs,
            )
            # > perform operation
            updated, leftalone, created = self._sync1way_impl(candidates)
            # > messages
            self._transfer_messages(updated, leftalone, created, self.pull.__name__)
        else:
            raise NotImplementedError


    def push(
            self,
            location = None,
            explicit_path = None,
            empty_push = False,
            #
            explicit_local_path = None,
            pass_dirs = None,
            double_explicit_stem = None,
    ):
        """
        Intuitively, push out (delicately)
        to the remote system.

        More precisely, a sync operation that
        respects the last-modified information
        in the remote system.

        Intuitively, push something out to the remote system.
        More precisely, a sync operation that respects
        the last-modified information in the remote system.

        In other words, it will push files recursively to the remote system,
        and perform checks on last-modified timestamps, if matching
        files are found in both locations. It will only push the
        updated files and the "new" ones.

        Not a full sync, because it leaves all existing files in the target path,
        ITCINOOD.

        Arguments:

            location (:any:`Location`):
            explicit_path (string):
            empty_push (boolean):
                Set to execute a push without actually
                pushing files - can be useful if re-running a multi-step script,
                and the source files have already been pushed.
            explicit_local_path (string):
            pass_dirs (list of string):
            double_explicit_stem (string):

        """
        if double_explicit_stem is not None:
            if not (explicit_path is not None and explicit_local_path is not None):
                raise ValueError("Cannot use double_explicit_stem argument unless using two explicit paths.")
            src_epath = os_path_join(explicit_local_path, double_explicit_stem)
            tgt_epath = os_path_join(explicit_path, double_explicit_stem)
        else:
            src_epath = explicit_local_path
            tgt_epath = explicit_path
        # > build source path
        # print(f"[push] src_epath {src_epath} location {location}")
        src_path = location.get_path(
            create=False,
            explicit_conf=self.conf,
        ) if src_epath is None else src_epath
        tgt_path = location.get_path(
            create=False,
            explicit_conf=self.rconf,
        ) if tgt_epath is None else tgt_epath
        if not empty_push:
            self._msg(f"Underway.\n{src_path}\n\t|VVV|\n[remote]{tgt_path}", function=self.push.__name__, always=True)
            # > create the target path
            self.mkdir_remote(tgt_path)
            # self.ssh(f"mkdir -p {tgt_path}")
            # > read the local system, get all the candidates
            candidates = self.get_candidates_local(
                src_path=src_path,
                tgt_path=tgt_path,
                pass_dirs=pass_dirs if pass_dirs is not None else [],
            )
            # > perform operation
            updated, leftalone, created = self._sync1way_impl(candidates, pull=False)
            # > messages
            updated = [f"[remote]{x}" for x in updated]
            leftalone = [f"[remote]{x}" for x in leftalone]
            created = [f"[remote]{x}" for x in created]
            self._transfer_messages(updated, leftalone, created, self.push.__name__)
        self.last_push_dir = tgt_path if tgt_path else src_path



    def twoway(
            self,
            location = None,
            explicit_path = None,
            explicit_remote_path = None,
            pass_dirs_remote = None,
    ):
        """
        Intuitively, synchronize directory trees
        on two systems.

        More precisely, a sync operation that
        respects the last-modified information
        in the local system and the remote system,
        locking in any changes that were made on
        one system, but not the other.

        If the same file is modified on both systems,
        there is no comparison (no "diff").
        The youngest ("freshest", or most recently updated) file is kept.
        So, this can work if a directory tree was updated
        within separate "sectors" on the two systems,
        but should not be used if there is any chance that updates
        might overlap one another, in which case
        some updates will be destroyed.

        Arguments:

            location (:any:`Location`):
            explicit_path (string):
            explicit_remote_path (string):
            pass_dirs_remote (list of string):

        :meta private:
        """
        # todo Work in progress.

        # todo this could be fixed without too much trouble
        #  by synchronizing another
        #  pair of directory trees with a copy
        #  that files can be diff'ed against. Then
        #  this third (reference) system can be
        #  updated with the changes from both
        #  downstream systems.
        #  UPDATE: I have some new notes on this somewhere.

        # > build source path
        src_path = location.get_path(
            create = False,
            explicit_conf=self.rconf,
        ) if explicit_remote_path is None else explicit_remote_path
        if self.host:
            # > build target path
            tgt_path = location.get_path(
                create = True,
                explicit_conf = self.conf,
            ) if explicit_path is None else explicit_path
            self._msg(f"Underway.\n[remote]{src_path}\n\t|VVV||^^^|\n{tgt_path}", function=self.twoway.__name__, always=True)
            # > read the remote system, get all the candidates
            candidates = self.get_candidates_remote(
                src_path=src_path,
                tgt_path=tgt_path,
                pass_dirs=[] if pass_dirs_remote is None else pass_dirs_remote,
            )
            # > perform operation
            updated, leftalone, created = self._sync1way_impl(candidates)
            candidates = self.get_candidates_local(
                src_path=src_path,
                tgt_path=tgt_path,
                pass_dirs=[],
            )
            # todo review this comparison
            candidates = [candidate for candidate in candidates if candidate[0] not in updated + created]
            updated2, leftalone2, created2 = self._sync1way_impl(candidates, pull=False)
            updated2 = [f"[remote]{x}" for x in updated2]
            leftalone2 = [f"[remote]{x}" for x in leftalone2]
            created2 = [f"[remote]{x}" for x in created2]
            self._transfer_messages(updated+updated2, leftalone+leftalone2, created+created2, function=self.twoway.__name__)
        else:
            raise NotImplementedError


    def run(
            self,
            target_name,
            program_args = None,
            venv_manager = None,
            venv_args = None,
            #####
            mode = None,
    ):
        """
        Run a job on a remote system,
        with a mode selected, either ``direct`` (blocking)
        or ``indirect`` (non-blocking via SLURM).

        Arguments:

            target_name (string):
                The target script or job. Can be a relative path in the
                working directory of the caller.
            program_args (optional list of string):
                Arguments to pass on to ``target_name``.
            venv_manager (optional string):
                The virtual environment manager.
                If left default, will be obtained from Conf.
            venv_args (optional list of string):
                The virtual environment argument list, STIUYKB.
            mode (:any:`Mode`):
                The run_remote mode, direct or indirect via SLURM.
        """
        program_args_ = program_args if program_args is not None else []
        venv_manager_ = self.rconf['venv_manager'] if venv_manager is None else venv_manager
        env = self.rconf['env']
        if venv_manager_ == "conda":
            python_invoc = ["conda", "run", f"-n{env}", "python"]
        elif venv_manager_ == "venv":
            # todo impl?
            # Every time I think of doing this I get frustrated
            # trying to make sense of venv, and I wonder if
            # it is just not a well-designed feature of Python.
            raise NotImplementedError
        elif venv_manager_ is None:
            # Run Python in the shell environment.
            python_invoc = ["python"]
        else:
            raise NotImplementedError
        python_invoc += venv_args if venv_args is not None else []
        if not isinstance(mode, Mode):
            raise ValueError(f"The mode must be a QueueG Mode.")
        elif isinstance(mode, indirect):
            # > mode #2 (run and monitor for successful SLURM job deployment)
            self._msg(f"Non-blocking via SLURM.", function=self.run.__name__, always=True)
            # > (Step 1/3) Launch
            # > run the launch script
            clist = python_invoc + [target_name] + program_args_ # + ['>out.txt', '2>err.txt']
            cmd = ' '.join(clist)
            output = self.ssh(cmd, strip=True)
            self._msg(output, as_is=True, always=True)
            # submitted batch job xxxx
            rr = re_compile("Submitted batch job (.*)")
            job = rr.search(output).group(1)
            # > (Step 3/3) Issue Check
            # > give SLURM a moment to process # todo hold off on this for now
            # print("Quitting early. Check job!!")
            # return 'R'
            # time.sleep(1)
            # > commence issue check
            current_status = self._check(
                job=job,
                until=mode.check_until,
                every=mode.check_every,
                seen_on_queue=False,
            )
            # Done.
            return current_status
            #####end#####
        else:
            self._msg(f"Blocking. Please wait.", function=self.run.__name__, always=True)
            # > run blocking via SSH client
            clist = python_invoc + [target_name] + program_args_
            cmd = ' '.join(clist)
            output = self.ssh(cmd)
            self._msg(f"Remote run finished. stdout:\n{output}", function=self.run.__name__, always=True)
            #####end#####


    def _check(
            self,
            job,
            until,
            every,
            seen_on_queue = True,
    ):
        """
        Internal implementation of SLURM queue monitoring.

        The time value `until` and `every` indicate to check
        every _every_ seconds until _until_ seconds have elapsed.
        Does not correspond to wall time; the thread will sleep for this long
        until sleep totals the requested amount: this is so the
        user can reason more easily about the input values.
        These may be strings or integers according to the
        TIME SYNTAX (see below).
        The minimum values of until and every are 1.
        If both have their minimum values,
        the check will occur exactly once.


        **TIME SYNTAX**:
        Accepted time syntax, where <n> is a positive base ten integer,
        in Backus-Nauer notation, with short glosses in the column on the right::

            <::>       =   <n>  |                              s
                                <n>:<n> |                      m:s
                                <n>:<n>:<n>              h:m:s
            <input> =   <::>   |                         h:m:s
                               <n>-<::>                     d-h:m:s

        If an integer is passed, it is interpreted as seconds.

        Arguments:

            job (string): job id
            every (integer or TIME):
            until (integer or TIME):
            seen_on_queue (boolean):

        Returns:

            string: job status

        """
        every_s = parse_time(every)
        until_s = parse_time(until)
        cmd = f"squeue -j{job} -o %t -h"
        seen = seen_on_queue
        stop_check_now = False
        slept = 0
        while True:
            status = self.ssh(cmd, strip=True)
            # > output should be a SLURM code, or else
            # whatever SLURM decides to do.
            if status in job_status_pp:
                seen = True
                current_status = status
            else:
                # If the job has left the queue the status is possibly
                # something like: `slurm_load_jobs error: Invalid job id specified`
                # which is a messy/ambiguous way for SLURM to say "it is done!".
                # I suspect there is a better way, but for now I am just
                # hoping to get something that (at least) works.
                self._msg(f"raw status: {status}", function=self.run.__name__)
                if seen:
                    if not status:
                        current_status = 'OQ'
                        stop_check_now = True
                    else:
                        current_status = 'UK'
                        self._msg(f"Status not recognized: {status}", function=self.run.__name__)
                else:
                    if not status:
                        current_status = 'NS'
                    else:
                        current_status = 'UK'
                        self._msg(f"Status not recognized: {status}", function=self.run.__name__)
            # > ....the reporting job that all this code is for.....
            self._msg(f"Status {job}: {job_status_pp[current_status]}", function=self.run.__name__, always=True)
            if stop_check_now:
                break
            slept += every_s
            if slept == until_s:
                self._msg("Timed out of issue check.", function=self.run.__name__, always=True)
                break
            time.sleep(every_s)
        return current_status


    def check(self, job):
        """
        Check the queue for a job ``job`` that is presumed to have
        deployed to SLURM.

        Arguments:

            job (string): job id

        Returns:

            string: job status
        """
        current_status = self._check(
            job=job,
            until=1,
            every=1,
            seen_on_queue=True,
        )
        return current_status


    def decide_to_pull(self, current_status):
        """
        Convenience function for processing a status
        obtained from a SLURM queue check.

        Arguments:

            current_status (string): SLURM job status

        Returns:
            boolean: pull or do not pull.

        """
        if current_status == 'OQ':
            self._msg("Job is off queue.", function=self.decide_to_pull.__name__, always=True)
            pull = True
        elif current_status == 'R':
            self._msg("Job is running.", function=self.decide_to_pull.__name__, always=True)
            pull = False
        else:
            self._msg(f"Job status: {current_status}. Would you like to pull result? This will not harm the job if it is still in progress. (y/n)", always=True)
            # todo timeout and then pull
            pull = (ttyRead().lower() == 'y')
        return pull


    ##########################################
    # Low-Level Transfer Operations



    def ssh(self, commands, strip = False, loopback = False):
        """
        Run commands remotely.

        Arguments:

            commands (string or list of string):
            strip (boolean):
                strip command outputs (no newlines)
            loopback (boolean):
                run as local shell

        Returns:

            list of string: outputs
        """
        if not isinstance(commands, list):
            commands = [commands]
        if self.remote_wdir:
            commands = [f"cd {self.remote_wdir}"] + commands
        # NOTE: exec_command's invocation generates
        # a new environment in the remote host, i.o.w. a new "exec channel".
        # Ordinary usage dictates respecting possible dependency of the
        # input command list. To achieve this,
        # either chain the list using POSIX syntax, or use` invoke_shell`.
        # So far, I have found the former sufficient.
        # todo test on failed commands.
        command = ' && '.join(commands)
        self._msg(f"< {command}", function=self.ssh.__name__)
        if loopback or self.host is None:
            # todo what is the best way of doing this? popen seems to work.
            output = os_popen(command).read()
            if strip:
                output = output.strip()
        else:
            _stdin, _stdout, _stderr = self._ssh.exec_command(command)
            output = _stdout.read().decode()
            error = _stderr.read().decode()
            if strip:
                output = output.strip()
            if error:
                self._msg("[ERROR] There was an error while running a command remotely. See the following (< input, > stdout, 2> stderr).", function=self.ssh.__name__, always=True)
                self._msg(f"< {command}", function=self.ssh.__name__, always=True)
                self._msg(f"> {output}", function=self.ssh.__name__, always=True)
                self._msg(f"2> {error}", function=self.ssh.__name__, always=True)
                raise SystemError
        self._msg(f"> {output}", function=self.ssh.__name__)
        return output




    def put(self, commands):
        """
        You must specify a full path in both directions.

        Arguments:

            commands (list of pair of string or pair of string):
                List of pairs (target artifact, source artifact).
                Both are full paths.
        """

        # todo does it overwrite?

        if not isinstance(commands, list):
            commands = [commands]
        if self.host:
            for command in commands:
                local = command[0]
                remote = command[1]
                # respect user's intuition of what cd() does
                if self.remote_wdir and remote[0] != '/':
                    remote = os_path_join(self.remote_wdir, remote)
                if self.v:
                    body = f"\n{local}\n\t|v|\n[remote]{remote}"
                    self._msg(body, function=self.put.__name__)
                self._sftp.put(local, remote)
        else:
            for command in commands:
                local = command[0]
                remote = command[1]
                if self.v:
                    body = f"\n{local}\n\t|v|\n[remote]{remote}"
                    self._msg(body, function=self.put.__name__)
                try:
                    shutil_copy(local, remote)
                except Exception as e:
                    print(f"Error copying file: {e}")

    def get(self, commands):
        """
        You must specify a full path in both directions.

        Will overwrite an existing artifact AFAIK.

        Arguments:

            commands (list of pair of string or pair of string):
                List of pairs (target artifact, source artifact).
                Both are full paths.

        """
        if not isinstance(commands, list):
            commands = [commands]
        if self.host:
            for command in commands:
                remote = command[0]
                local = command[1]
                # respect user's intuition of what cd() does
                if self.remote_wdir and remote[0] != '/':
                    remote = os_path_join(self.remote_wdir, remote)
                if self.v:
                    body = f"\n[remote]{remote}\n\t|v|\n{local}"
                    self._msg(body, function=self.get.__name__, as_is = True)
                self._sftp.get(remote, local)
        else:
            for command in commands:
                remote = command[0]
                local = command[1]
                if self.v:
                    body = f"\n[remote]{remote}\n\t|v|\n{local}"
                    self._msg(body, function=self.get.__name__, as_is = True)
                os_popen(f"cp {command[0]} {command[1]}")



    def cd(self, path):
        """
        Change directory on remote machine.
        Does not seem very sophisticated to me, but it should work.

        Arguments:

            path (string):

        """
        if path == '~':
            self.remote_wdir = None
        if path == 'last push' or path == 'push':
            self.remote_wdir = self.last_push_dir
        else:
            self.remote_wdir = path



    #######################################################
    # Cache and System Operations

    def get_age(self, path):
        """
        Get a conveniently wrapped timstamp for an artifact
        on the requested path.

        Arguments:
                path (string):
        Returns:
                :any:`Age`:
        """
        # This ended up being messier than I had originally hoped.
        cmd = cmd_stat_fmt(path, uname=self._uname)
        age = self.ssh(cmd, strip=True)
        return Age(age=age)


    def get_candidates_remote(
            self,
            src_path,
            tgt_path,
            pass_dirs,
    ):
        """
        Get candidates for a syncing operation,
        by a recursive polling for information from
        the file system.

        This can be done using `ls`.
        Parsing of `ls` output makes assumptions
        about how ls behaves. Has
        been tested on Darwin (flavor of BSD)
        and Linux ITCINOOD.

        Builds target directory tree as side effect.

        Arguments:

            src_path (string):
                source path
            tgt_path (string):
                uses the tgt_path as root path and create
                the directory tree revealed while building the
                candidate list.
            pass_dirs (list of string):
                Directories to skip during scan of remote
                directory. These directories, if they exist, and their
                contents will be left unchecked, and untouched.

        Returns:

            list of pairs (path, age, tgt_path) where `name`
                is a full path to an artifact and `age` is its age.

        :meta private:
        """
        # todo get_candidates_remote's pass_dirs feature was
        #  added as a hasty afterthought - my apologies :/
        #  It's a needed feature, and when there is time
        #  we need to implement a stable and comprehensive test.

        # todo merge code for get_candidates_local and get_candidates_remote?
        candidates = []
        cmd = cmd_ls_fmt(src_path, uname=self._uname)
        output = self.ssh(cmd)
        blocks = output.strip().split("\n\n")
        # Assumption: When a full path is passed to ls,
        # the headings on the blocks in the recursive output
        # are all full paths. (ls -lR /full/path/to/X)
        prefix = src_path
        stemlist = []
        for block in blocks:
            pass_block = False
            lines = block.split("\n")
            for line in lines:
                if pass_block:
                    self._msg(f"Passing block:\n{block}", function=self.get_candidates_remote.__name__)
                    break # from line loop
                line = line.strip()
                if line == "":
                    continue
                if line.startswith("total"):
                    continue
                if line.endswith(":"):
                    prefix = line.split(":")[0]
                    stem = prefix.removeprefix(src_path)
                    # Does the OS's `ls` write /foo/bar/ or /foo/bar?
                    # I think I've seen both. :/
                    # I could use strip('/') but I
                    # want to be certain I have an empty
                    # string in the case I am expecting one.
                    if stem:
                        while stem[0] == '/':
                            stem = stem[1:]
                        if stem:
                            while stem[-1] == '/':
                                stem = stem[:-1]
                    stemlist = stem.split('/')
                    for pass_dir in pass_dirs:
                        if pass_dir in stemlist:
                            pass_block = True
                            break # from pass_dir loop
                    if not pass_block:
                        if stemlist:
                            create_dir(tgt_path, stem=stemlist)
                    continue
                cols = re_split(r" +", line)
                name = cols[6]
                if name[0] != '.' and cols[0][0] != 'd':
                    path1 = os_path_join(prefix, name)
                    age1 = int(cols[5])
                    path2 = os_path_join(tgt_path, *stemlist, name)
                    age2 = Age(path2).dtint
                    candidates.append((path1, age1, path2, age2))
        self._msg(candidates, function=self.get_candidates_remote.__name__)
        return candidates


    def get_candidates_local(
            self,
            src_path,
            tgt_path,
            pass_dirs,
    ):
        """
        Builds target directory tree as side effect.

        Arguments:

            src_path (string):
            tgt_path (string):
            pass_dirs (list of string):
                List of directory names to pass,
                on a recursive basis.

        Returns:

            candidate list

        :meta private:
        """
        candidates = []
        cmd = cmd_ls_fmt(src_path)
        output = self.ssh(cmd, loopback=True)
        # todo if there is a problem, STOP.
        blocks = output.strip().split("\n\n")
        # Assumption: When a full path is passed to ls,
        # the headings on the blocks in the recursive output
        # are all full paths. (ls -lR /full/path/to/X)
        root1 = src_path
        stemlist = []
        for block in blocks:
            pass_block = False
            lines = block.split("\n")
            for line in lines:
                if pass_block:
                    self._msg(f"Passing block:\n{block}", function=self.get_candidates_local.__name__)
                    break # from line loop
                else:
                    line = line.strip()
                    if line == "":
                        continue
                    if line.startswith("total"):
                        continue
                    if line.endswith(":"):
                        root1 = line.split(":")[0]
                        stem = root1.removeprefix(src_path)
                        # Does the OS's `ls` write /foo/bar/ or /foo/bar?
                        # I think I've seen both. :/
                        # I could use strip('/') but I
                        # want to be certain I have an empty
                        # string in the case I am expecting one.
                        if stem:
                            while stem[0] == '/':
                                stem = stem[1:]
                            if stem:
                                while stem[-1] == '/':
                                    stem = stem[:-1]
                        stemlist = stem.split('/')
                        # todo document this loop
                        for pass_dir in pass_dirs:
                            if pass_dir in stemlist:
                                pass_block = True
                                break # from pass_dir loop
                        if not pass_block:
                            if stem:
                                tgt_path_stem = os_path_join(tgt_path, stem)
                                if self.host is not None:
                                    # todo test unusual characters in the path
                                    self.ssh(f"mkdir -p {tgt_path_stem}")
                                else:
                                    os_makedirs(tgt_path_stem, exist_ok=True)
                        continue
                    cols = re_split(r" +", line)
                    name = cols[6]
                    if name[0] != '.' and cols[0][0] != 'd':
                        path1 = os_path_join(root1, name)
                        age = int(cols[5])
                        path2 = os_path_join(tgt_path, *stemlist, name)
                        age2 = Age(age=self.ssh(cmd_stat_fmt(path2, uname=self._uname), strip=True)).dtint
                        candidates.append((path1, age, path2, age2))
        self._msg(candidates, function=self.get_candidates_local.__name__)
        return candidates


    def _get_rconf_path(self):
        """
        Helper to get the remote Conf file's path.

        Returns:
            string: full path

        """
        queueg_conf_dir = self.ssh("echo $QUEUEG_CONF_DIR", strip=True)
        if queueg_conf_dir != "":
            path = os_path_join(queueg_conf_dir, conf_file)
        else:
            home = self.ssh("echo $HOME", strip=True)
            path = os_path_join(home, *default_conf_stemlist, conf_file)
        return path


    def _cache_refresh(self, cache_path):
        """

        Arguments:
             cache_path (string):
        """
        # > get remote lastmodified
        rconf_path = self._get_rconf_path()
        if os_path_exists(cache_path):
            # > compare with remote
            # > get local lastmodified
            cache_age = Age(path=cache_path)
            rconf_age = self.get_age(rconf_path)
            if cache_age < rconf_age:
                self._msg(f"Updating local cache for {self.target}", function=self._cache_refresh.__name__)
                # the cache is out of date
                # > update the cache
                self.get((rconf_path, cache_path))
        else:
            self._msg(f"Creating local cache for {self.target}", function=self._cache_refresh.__name__)
            # > get the cache and put it at cache_path
            self.get((rconf_path, cache_path))
        self.rconf = Conf(cache_path)



    def _transfer_messages(self, updated, leftalone, created, function):
        self._msg("Done.", function=function, always=True)
        something = False
        if updated:
            self._msg("These files were updated:", function=function, always=True)
            self._msg('\n'.join(updated), always=True, as_is=True)
            something |= True
        if leftalone:
            self._msg("These files were left alone:", function=function, always=True)
            self._msg('\n'.join(leftalone), always=True, as_is=True)
            something |= True
        if created:
            self._msg("These files were created:", function=function, always=True)
            self._msg('\n'.join(created), always=True, as_is=True)
            something |= True
        if not something:
            self._msg("There was nothing to do.", always=True)



    def _msg(self, body, function = None, always = False, as_is = False):
        loc_list = [self.__class__.__name__]
        if function is not None:
            loc_list += [function]
        if self.v and not always:
            loc_list += ["verbose"]
        message = body if as_is else f"[{':'.join(loc_list)}] {body}"
        if self.v or always:
            print(message)


