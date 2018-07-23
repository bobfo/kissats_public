"""A basic test runner"""


import argparse

import logging
import logging.handlers
import os
import tempfile
import time

import pathlib2 as pathlib

from kissats.common import load_data_file
from kissats.task_pack import TaskPack


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def logger_setup(log_loc=None, log_prefix=__name__):
    """setup some basic logging"""

    log_format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_formater = logging.Formatter(log_format_str)
    log_level = logging.DEBUG

    logging.basicConfig(format=log_format_str, level=log_level)

    root_logger = logging.getLogger()

    if log_loc is None:
        log_location = pathlib.Path(tempfile.gettempdir())
    else:
        log_location = pathlib.Path(log_loc)

    log_file_name = "{0}_{1}.log".format(log_prefix, time.strftime("%Y%m%d_%H%M%S"))
    log_file = pathlib.Path(log_location, log_file_name)

    file_hdlr = logging.FileHandler(str(log_file))
    file_hdlr.setFormatter(log_formater)
    root_logger.addHandler(file_hdlr)
    root_logger.setLevel(log_level)

    logger.info("Logging setup complete")


def build_parser():
    """setup the arg parser"""

    parser = argparse.ArgumentParser(description=__doc__,)
    arg_group = parser.add_mutually_exclusive_group(required=False)

    parser.add_argument('--params',
                        dest='param_input',
                        action='store',
                        help=('JSON or YAML formated parameter file input'),
                        required=True)
    parser.add_argument('--log_prefix',
                        dest='log_pre',
                        action='store',
                        help=('prefix for log file name, '
                              'default to {0}'.format(__name__)),
                        required=False, default=__name__)
    parser.add_argument('--log',
                        dest='log_loc',
                        action='store',
                        help='dir for log files, default cwd',
                        required=False, default=os.getcwd())
    arg_group.add_argument('--testtorun',
                           dest='test_to_run',
                           action='store',
                           help=('test to run'),
                           default=None)
    arg_group.add_argument('--testgroup',
                           dest='test_group',
                           action='store',
                           help=('test group to run'),
                           default=None)
    parser.add_argument('--singletest',
                        dest='single_mode',
                        action='store_true',
                        help=('enable single test mode, ignore prereqs, '
                              'skip gloabal setup and teardown'),
                        required=False)
    parser.add_argument('--skip_setup',
                        dest='skip_setup',
                        action='store_true',
                        help='skip gloabal setup',
                        required=False)
    parser.add_argument('--ignore_prereq',
                        dest='ignore_prereq',
                        action='store_true',
                        help='Ignore all prereqs',
                        required=False)
    parser.add_argument('--skip_teardown',
                        dest='skip_teardown',
                        action='store_true',
                        help='skip gloabal teardown',
                        required=False)
    parser.add_argument('--run_mode',
                        dest='run_mode',
                        action='store',
                        help=('Run mode, default is normal'),
                        default="normal",
                        required=False)
    parser.add_argument('--schema',
                        dest='schema_add',
                        action='store',
                        help=('Additional schema file'),
                        default=None,
                        required=False)

    return parser


def main(input_args):
    """the main function"""

    global_params = load_data_file(input_args.param_input)

    # TODO(BF): need to handle this differantly
    # if global_params.get('test_groups'):
    #    input_test_groups = global_params.pop('test_groups')

    big_pack = TaskPack(global_params, input_args.schema_add)

    if args.test_group is not None:
        big_pack.add_test_group(args.test_group)
    elif args.test_to_run is not None:
        big_pack.add_test_task(args.test_to_run)

    skip_setup = False
    skip_teardown = False

    if input_args.singletest:
        big_pack.ignore_prereq = True
        skip_setup = True
        skip_teardown = True
    else:
        skip_teardown = skip_teardown
        skip_setup = input_args.skip_setup
        big_pack.ignore_prereq = input_args.ignore_prereq

    if not skip_setup and not skip_teardown:
        print("running all tasks")
        big_pack.run_all_que()
    else:
        if not skip_setup:
            print ("running setup")
            big_pack.run_setup_que()
        print ("running tests")
        big_pack.run_test_que()
        if not skip_teardown:
            print ("running teardown")
            big_pack.run_teardown_que()

    print("run complete")


if __name__ == "__main__":

    main_parser = build_parser()
    args = main_parser.parse_args()

    logger_setup(args.log_loc, args.log_pre)

    main(args)
