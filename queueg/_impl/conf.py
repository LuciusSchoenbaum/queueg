



from os import (
    environ as os_environ,
)
from os.path import (
    join as os_path_join,
    exists as os_path_exists,
)

from mv1fw import (
    create_dir,
)

from .ossys.ossys import (
    default_conf_stemlist,
    conf_file,
    default_index_stem,
    default_inventory_stem,
)


import tomli

# todo 'pyp39' - we have to assume use of env called pyp39.
#  do we want to hard-code the environment name?

# todo a version of queueg without anaconda dependency assumed.
#  conf option: use_anaconda, use_venv, ...???


new_user_config = """\
# This file was automatically generated by QueueG for a "new user" setup. 
# 1) You can edit this file to change the basic configuration of QueueG. 
#   Recommended to change it by modifying the existing key-value pairs. 
#   If you have questions about the TOML syntax, see https://toml.io/en/ 
# 2) You can move this file to another directory (of your choosing), 
#   by setting the environment variable QUEUEG_CONF_DIR. 
# 3) You can edit or remove this comment block. 

conf_by_user = false
env = 'pyp39'
venv_manager = 'conda'
index_dir = 'default'
inventory_dir = 'default'
"""




class Conf:
    """
    Housing for access to the config file, or ``Conf file".
    Includes parsing, exception-handling,
    and some user notifications.

    Parameters:
        explicit_path (string):
            read a conf file on a nonstandard path (cf. Sync)
        verbose (boolean):
            read verbosely (debug)

    :meta private:
    """

    def __init__(
        self,
        explicit_path = None,
        verbose=False,
    ):
        self.verbose = verbose
        # repository (in memory) for conf file
        self.confdict = {}
        self._load(explicit_path)


    def __getitem__(self, x):
        # todo error handling, or let python error handling take care of it?
        return self.confdict[x]


    def __contains__(self, x):
        return x in self.confdict


    def _load(
            self,
            explicit_path,
    ):
        if explicit_path is None:
            conf_dir_v = "QUEUEG_CONF_DIR"
            if conf_dir_v not in os_environ:
                self.msg(f"{conf_dir_v} not found")
                # new (perhaps) user who wants to use.
                conf_dir = create_dir(os_environ["HOME"], default_conf_stemlist)
                # Invariant: conf_dir is created/defined.
                conf_file_path = os_path_join(conf_dir, conf_file)
                self.msg(f"conf_file_path {conf_file_path}")
                read_ok, parse_ok, msg = self._read_parse_procedure(conf_file_path)
                if read_ok and parse_ok:
                    self.msg(f">>> Case of new (perhaps) user who has an existing config.")
                    self._welcome_msg()
                    self._proceed()
                elif not read_ok:
                    self.msg(f">>> Case of new (perhaps) user who does not have a config.")
                    # It may be otherwise, but we don't consider it further.
                    # The main objective is to onboard new users quickly.
                    with open(conf_file_path, "w") as f:
                        # todo - when the config is read in, the option conf_by_user is set to false here, SO a message will appear and remind user to review config and set conf_by_user to true in order to remove the message.
                        f.write(new_user_config)
                    self._parse_conf_file(new_user_config)
                    self._welcome_msg()
                    self._proceed()
                else: # not parse_ok:
                    # Case of new (perhaps) user with a broken config, for some reason.
                    raise EnvironmentError(f"[QueueG] Conf file {msg}. If unsure how to proceed, you can restore default config by removing this file from your system: {conf_file_path} and then re-run.")
            else:
                self.msg(">>> Case of non-new user")
                self.msg(f"{conf_dir_v} found")
                conf_dir = os_environ[conf_dir_v]
                if not os_path_exists(conf_dir):
                    self.msg(">>> ...who has a broken config, for some reason.")
                    eemsg = f"Found variable {conf_dir_v} but directory not found. {conf_dir_v}={conf_dir}"
                    raise EnvironmentError(eemsg)
                conf_file_path = os_path_join(conf_dir, conf_file)
                self.msg(f"conf_file_path {conf_file_path}")
                read_ok, parse_ok, msg = self._read_parse_procedure(conf_file_path)
                if read_ok & parse_ok:
                    self.msg(">>> ...whose config is working.")
                    self._proceed()
                else:
                    self.msg(">>> ...who has a broken config, for some reason.")
                    eemsg = f"Found variable {conf_dir_v} but conf file {msg}. {conf_dir_v}={os_environ[conf_dir_v]}"
                    raise EnvironmentError(eemsg)
        else:
            # This branch is added for sync to use
            _, _, _ = self._read_parse_procedure(explicit_path)
            # todo error handling



    def _read_parse_procedure(self, conf_file_path):
        read_ok, conf_file_raw = self._read_conf_file(conf_file_path)
        parse_ok = True
        msg = None
        if read_ok:
            parse_ok = self._parse_conf_file(conf_file_raw)
            if parse_ok:
                self.msg("parse ok")
            else:
                msg = "could not be parsed (parsing error)"
        else:
            msg = "not found"
        return read_ok, parse_ok, msg



    def _read_conf_file(self, conf_file_path):
        read_ok = False
        conf_file_raw = None
        if os_path_exists(conf_file_path):
            read_ok = True
            self.msg("read ok")
            with open(conf_file_path, "r") as f:
                conf_file_raw = f.read()
        return read_ok, conf_file_raw



    def _parse_conf_file(self, conf_file_raw):
        parse_ok = True
        try:
            # rely on tomli's error handling. todo improve
            self.confdict = tomli.loads(conf_file_raw)
        except:
            parse_ok = False
        return parse_ok



    def _proceed(self):
        self.msg(f"proceed\nconf = {self.confdict}")
        if self.confdict['index_dir'] == 'default':
            index_dir = create_dir(os_environ["HOME"], [default_index_stem])
            self.confdict['index_dir'] = str(index_dir)
        if self.confdict['inventory_dir'] == 'default':
            inventory_dir = create_dir(os_environ["HOME"], [default_inventory_stem])
            self.confdict['inventory_dir'] = str(inventory_dir)



    def msg(self, x):
        if self.verbose:
            print(x)


    def _welcome_msg(self):
        if not self.confdict['conf_by_user']:
            welcomemsg = f"""\
QueueG has generated a config file that you can modify. 
Please review the config file {conf_file} in the directory   
{os_path_join(os_environ["HOME"], *default_conf_stemlist)}. 
When you are satisfied, change the value of conf_by_user 
from "false" to "true" and this message will no longer appear.\n"""
            print(welcomemsg)


    def __str__(self):
        return str(self.confdict)


