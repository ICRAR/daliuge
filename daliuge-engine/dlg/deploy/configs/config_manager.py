#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import sys
import optparse # pylint: disable=deprecated-module
import shutil
import textwrap
import typing

from enum import Enum, auto
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

import dlg.deploy.configs as default_configs
from dlg.deploy.configs import ConfigFactory

USER_CONFIG_DIR = Path.home() / ".config"
DLG_CONFIG_DIR = USER_CONFIG_DIR / "dlg"

class ConfigType(Enum):
    ENV = "ini"
    SLURM = "slurm"


class ConfigDirState(Enum):
    NEW = auto()
    EXISTS = auto()
    NOT_CREATED = auto()


@dataclass
class ConfigManager:
    """
    Utility class to manager job submission configuration files
    """

    facilities: typing.List[str]
    prompt_setup = True

    def setup_user(self):
        """
        Called when passing the "--setup" command of the "dlg remote" option.
        """
        dir_state = self.create_dlg_config_directory()
        if dir_state == ConfigDirState.NEW:
            self.copy_defaults_to_user_config_directory()
            self.print_available_config()
        elif dir_state == ConfigDirState.EXISTS:
            print(f"{DLG_CONFIG_DIR} already exists.")
        else:
            print(f"{DLG_CONFIG_DIR} was not created.")

    def prompt_user(self):
        """
        Create prompt and validate it against expected output
        """

        while True:
            ui = input("Do you want to create a $HOME/.config/dlg directory"
                       " to store your custom configuration files and scripts (y/n)? ")
            if ui.lower() == "y" or ui.lower() == "n":
                return ui.lower() == "y"
            else:
                print("Please selected from the options (Y/N)")

    def create_dlg_config_directory(self):
        """
        Establish the user $HOME/.config/dlg/ directory
        """
        if DLG_CONFIG_DIR.exists():
            return ConfigDirState.EXISTS
        else:
            if self.prompt_user():
                try:
                    Path.mkdir(DLG_CONFIG_DIR)
                    return ConfigDirState.NEW
                except Exception as e:
                    raise (e)
            else:
                return ConfigDirState.NOT_CREATED

    def copy_defaults_to_user_config_directory(self):
        """
        Move configuration files from dlg/deploy/configs to DLG_CONFIG_DIR
        """
        cfg_path = files(default_configs)

        configs = [p for p in cfg_path.iterdir() if
                   (".ini" in p.name) or (".slurm" in p.name)]

        if DLG_CONFIG_DIR.exists():
            for cfg in configs:
                if cfg.exists():
                    shutil.copy(cfg, DLG_CONFIG_DIR)
                else:
                    print("Unable to copy %s, does not exist", cfg)
        else:
            print("Unable to copy to %s, does is not available", DLG_CONFIG_DIR)

    def get_user_configs(self) -> typing.Dict:
        """
        Returns dictionary of filetypes to split out?
        """
        configs = {}
        if not DLG_CONFIG_DIR.exists():
            return configs
        filetypes = ["ini", "slurm"]

        for ftype in filetypes:
            configs[ftype] = [cfg for cfg in DLG_CONFIG_DIR.iterdir() if
                ftype in cfg.name]
        return configs

    def print_available_config(self):
        """
        Present the options available for deployment
        """
        print("User Configs (~/.config/dlg)")
        print("----------------------------")
        user_configs = self.get_user_configs()
        if user_configs:
            print("Environments (--config_file):")
            for config in user_configs["ini"]:
                print(textwrap.indent(config.name, "\t"))
            print("Slurm scripts (--slurm_template):")
            for config in user_configs["slurm"]:
                print(textwrap.indent(config.name, "\t"))
        else:
            print(textwrap.indent("N/a: User-specific directory is not setup.", "\t"))

        print("\nDALiuGE Defaults (-f/--facility):")
        print("-----------------------------------")
        for f in self.facilities:
            print(textwrap.indent(f, "\t"))

    def load_user_config(self, config_type: ConfigType, config_choice: str) -> Path:
        """
        Resolve the config type and choice to a config file and return full path.

        Path resolution occurs in the following hierarchy:
        - If config choice is absolute path, return the absolute path
        - If config choice is not an absolute path, but the path exists (i.e. can be
        found) return the path
        - If the config choice is relative, and doesn't exist, check if it exists in
          DLG_CONFIG_DIR
            - If it does exist, construct the path
            - If it doesn't exist, return None
        """
        if not DLG_CONFIG_DIR.exists() and self.prompt_setup:
            print("NOTE: No user configs exists; consider running 'dlg config --setup'.")
            self.prompt_setup = False

        choice_path = Path(config_choice)
        if choice_path.is_absolute() and choice_path.exists():
            return choice_path
        elif choice_path.exists():
            return choice_path.absolute()
        else:
            user_configs = self.get_user_configs()

            options = user_configs.get(config_type.value, [])
            for o in options:
                if config_choice == o.name:
                    return o
            return None

def run(_, args):

    cfg_manager = ConfigManager(ConfigFactory.available())

    parser = optparse.OptionParser()
    parser.add_option(
        "--setup",
        dest="setup",
        action="store_true",
        help="Setup local '$HOME/.config/dlg' directory to store custom environment config and slurm scripts",
        default=False
    )
    parser.add_option(
        "-l", "--list",
        dest="list",
        action="store_true",
        help="List the available configuration for DALiuGE deployment."
    )
    (opts, _) = parser.parse_args(sys.argv)
    if opts.setup:
        cfg_manager.setup_user()
        sys.exit(0)
    elif opts.list:
        print("Available facilities:\n")
        cfg_manager.print_available_config()
        sys.exit(0)
    else:
        print(f"Incorrect arguments: {args}")
        parser.print_help()



