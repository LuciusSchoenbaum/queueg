



import os
import sys
import tty, termios
import re
from datetime import datetime
from zipfile import ZipFile



# conf constant strings
default_conf_stemlist = [".config", "QueueG"]
conf_file = "config.toml"
default_index_stem = "QueueGindex"
default_inventory_stem = "QueueGinventory"

# sync constant strings
current_jobs = "current_jobs.txt"
# pretty print of job status
# https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
# NOTE: NS, OQ, UK are not SLURM job state codes.
# NS means we don't have any information from SLURM yet,
# OQ means we don't see the job in SLURM any longer.
# UK means something is wrong, maybe time to debug.
job_status_pp = {
    # Most often seen
    'NS': "Not seen on queue (NS)", # (not a true job status)
    'PD': "Pending (PD)",
    'R': "Running (R)",
    'OQ': "Off the queue (OQ)",
    'UK': "Unknown status (UK)",
    # Other statuses
    'BF': "Boot fail (BF)",
    'CA': "Cancelled (CA)",
    'CD': "Completed (CD)",
    'CF': "Configuring (CF)",
    'CG': "Completing (CG)",
    'DL': "Deadline (DL)",
    'F': "Failed (F)",
    'S': "Suspended (S)",
    'ST': "Stopped with SIGSTOP signal (ST, STOPPED)",
    'TO': "Timed out (TO, TIMEOUT)",
    # Other statuses
    'PR': "Preempted (PR, PREEMPTED)",
    'RD': "Held after requested reservation was deleted (RD, RESV_DEL_HOLD)",
    'RF': "Being requeued by a federation (RF, REQUEUE_FED)",
    'RH': "Held to be requeued (RF, REQUEUE_HOLD)",
    'RQ': "Completing, being requeued (RQ, REQUEUED)",
    'RS': "Resizing (RS, RESIZING)",
    'RV': "Sibling removed from cluster due to other cluster starting the job (RV, REVOKED)",
    'SI': "Being signaled (SI, SIGNALING)",
    'SE': "Requeued in a special state (SE, SPECIAL_EXIT)",
    'SO': "Staging out files (SO, STAGE_OUT)",
}


def now(YMD = False, hm = False, hms = False):
    """
    Get either a human-readable timestamp
    representing the current time in the system location,
    or a canonical symbol representing the date
    or the date+time, up to the minute,
    or the date+time+seconds, up to the second.

    - date: Y/M/D (Gregorian calendar).
    - Hours: 0 to 23.
    - Minutes: 0 to 59.
    - Seconds: 0 to 59.

    Arguments:

        YMD (boolean):
            Return a datestamp in the form '<Y><M><D>'.
            Default: False
        hm (boolean):
            Default: False
        hms (boolean):
            Overrides the setting of hm.
            Default: False

    Returns:
        string: 'Y/M/D h:m:s' or 'YMD' or 'YMDhm' or 'YMDhms'.

    :meta private:
    """
    N = datetime.now()
    Y, M, D = N.year, N.month, N.day
    h, m, s = N.hour, N.minute, N.second
    if YMD:
        out = f'{Y}{M:02d}{D:02d}'
        if hms or hm:
            out += f'{h:02d}{m:02d}'
            if hms:
                out += f'{s:02d}'
    else:
        out = f'{Y}/{M}/{D} {h}:{m}:{s}'
    return out


def copy(filename, stem1 = '.', stem2 = '.'):
    """
    Copy (in the sense of not changing the name but
    changing the location of something).

    Arguments:

        filename (string): artifact
        stem1 (optional string): absolute or relative path, source.
        (Default: current working directory)
        stem2 (optional string): absolute or relative path, target.
        (Default: current working directory)

    :meta private:
    """
    os.system(f'cp {stem1}/{filename} {stem2}/{filename}')




def zip_something_up(
        sources,
        zip_root,
        zip_path = '.',
        skip_unix_hidden = True,
        dryrun = False,
):
    """
    Create archive using ``sources`` information,
    with the option to skip over unwanted subdirectories and hidden files.

    Arguments:

        sources (dict[string] of dict[string] of string):
            A map associating root directories with stem subdirectories,
            to which are associated further subdirectories to skip ("skip directories").
        zip_root (string):
            The name of the directory that appears
            when you unzip the zip file.
            This is also automatically the name of the target zip file,
            omitting the ".zip" ending.
        zip_path (string):
            An optional path redirection for the target file.
            (Default: no redirection)
        skip_unix_hidden (boolean):
            Skip hidden files. (Default: True)
        dryrun (boolean):
            Print target files detected and quit early.
            (Default: False)

    :meta private:
    """
    files_dict = {}
    for root_dir in sources:
        stems = sources[root_dir]
        root_dir = os.path.abspath(root_dir)
        files_dict[root_dir] = []
        for stem in stems:
            skip_dirs = stems[stem]
            tgt_dir = os.path.abspath(os.path.join(root_dir, stem))
            files = []
            for path, subdirs, fnames in os.walk(tgt_dir):
                for skip_dir in skip_dirs:
                    if skip_dir in subdirs:
                        subdirs.remove(skip_dir)
                for fname in fnames:
                    if skip_unix_hidden and fname[0] == '.':
                        pass
                    else:
                        file = os.path.abspath(os.path.join(path, fname))
                        files.append(file)
            files_dict[root_dir] += files
    if dryrun:
        print(f"[zip_something_up] Files:\n{files_dict}")
        print(f"[zip_something_up] dryrun {dryrun}, no zip file was created")
    else:
        with ZipFile(f"{zip_path}/{zip_root}.zip", 'w') as target:
            for root_dir in files_dict:
                files = files_dict[root_dir]
                for file in files:
                    # > prevent attempting to write aliases
                    if os.path.exists(file):
                        arcname = re.sub(root_dir, zip_root, file)
                        target.write(file, arcname=arcname)



# helper
# read and return a character from stdin, at terminal
def ttyRead():
    fd = sys.stdin.fileno()
    struct = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        readch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, struct)
    return readch



# helper
# read and return a line from stdin, at terminal.
# optionally, remove relay of stdin input to stdout during entry.
def prompt_user(prompt, quiet = False):
    print(prompt, end="", flush=True)
    out = ""
    fd = sys.stdin.fileno()
    struct = termios.tcgetattr(fd)
    restore = sys.stdout
    if quiet:
        sys.stdout = open(os.devnull, 'w')
    while True:
        try:
            tty.setcbreak(fd)
            readch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, struct)
        if readch == '\n':
            break
        out += readch
    if quiet:
        sys.stdout = restore
    # complete the prompt's line
    print("", flush=True)
    return out







