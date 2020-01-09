"""
For documentation purpose, run the script with -h flag
"""

import os
import sys
import logging

from argparse import ArgumentParser
from datetime import datetime as dt


###############################################################################
# helper (manipulates handlers for arcpy messaging)
class ArcPyLogHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = record.msg.format(record.args)
        except:
            msg = record.msg

        if record.levelno == logging.ERROR:
            arcpy.AddError(msg)
        elif record.levelno == logging.WARNING:
            arcpy.AddWarning(msg)
        else:
            arcpy.AddMessage(msg)

        super(ArcPyLogHandler, self).emit(record)


###############################################################################
# helper (manipulates handlers for the logic of the application)
def run_app():
    """Run the logic of the application"""
    eco_file, eco_fld, eco_name, vri, vri_filter, tsa, tfl, private,\
        fwa, lake_ha, harvest, gdb, logger = get_input_parameters()
    riparian_analysis(eco_file, eco_fld, eco_name, vri, vri_filter,
                      tsa, tfl, private, fwa, lake_ha, harvest, gdb, logger)


###############################################################################
# handler (defines core functions of the application)

def get_input_parameters():
    """
        Parse arguments and set up logger
    """
    try:
        parser = ArgumentParser(
            description='Populate this with a description of the tool')
        parser.add_argument('eco_file', help='Path to Ecosections')
        parser.add_argument('eco_fld', nargs='?', help='Ecosection Name Field')
        parser.add_argument('eco_name', nargs='?', help='Ecosection Name Value')
        parser.add_argument('vri', help='Path to VRI')
        parser.add_argument('vri_filter', help='VRI BCLS_Level_5 values', default=['LA', 'RE'])
        parser.add_argument('tsa', help='Path to TSA')
        parser.add_argument('tfl', help='Path to TFL')
        parser.add_argument('private', help='Path to Private Land')
        parser.add_argument('fwa', help='Path to FWA Lakes')
        parser.add_argument('lake_ha', nargs='?', help='Minimum Lake Size')
        parser.add_argument('harvest', help='Harvest Data Constraint')
        parser.add_argument('gdb', help='Path to Working Geodatabase')
        parser.add_argument('--log_level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                            help='Log level')
        parser.add_argument('--log_dir', help='Path to Log Directory')
        args = parser.parse_args()

        log_name = 'main_logger'
        logger = logging.getLogger(log_name)
        logger.handlers = []

        log_fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_file_base_name = os.path.basename(sys.argv[0])
        log_file_extension = 'log'
        timestamp = dt.now().strftime('%Y-%m-%d_%H-%M-%S')
        log_file = '{}_{}.{}'.format(timestamp, log_file_base_name, log_file_extension)

        logger.setLevel(args.log_level)

        sh = logging.StreamHandler()
        sh.setLevel(args.log_level)
        sh.setFormatter(log_fmt)
        logger.addHandler(sh)

        if args.log_dir:
            try:
                os.makedirs(args.log_dir)
            except OSError:
                pass

            fh = logging.FileHandler(os.path.join(args.log_dir, log_file))
            fh.setLevel(args.log_level)
            fh.setFormatter(log_fmt)
            logger.addHandler(fh)

        if os.path.basename(sys.executable).lower() == 'python.exe':
            arc_env = False
        else:
            arc_env = True

        if arc_env:
            arc_handler = ArcPyLogHandler()
            arc_handler.setLevel(args.log_level)
            arc_handler.setFormatter(log_fmt)
            logger.addHandler(arc_handler)

        return args.eco_file, args.eco_fld, args.eco_name, args.vri, args.vri_filter, args.tsa, args.tfl, args.private,\
               args.fwa, args.lake_ha, args.harvest, args.gdb, logger

    except Exception as e:
        logging.error('Unexpected exception. Program terminating.')
        raise Exception('Errors exist')


def riparian_analysis(eco_file, eco_fld, eco_name, vri, vri_filter,
                      tsa, tfl, private, fwa, lake_ha, harvest, gdb, logger):
    """
        This script is a sample script to be filled in.

        :param str eco_file: Path to Ecosections file
        :param str eco_fld: Ecosection name field
        :param str eco_name: Ecosection name value
        :param str vri: Path to VRI
        :param str vri_filter: VRI BCLS_Level_5 values
        :param str tsa: Path to TSA
        :param str tfl: Path to TFL
        :param str private: Path to Private Land
        :param str fwa: Path to FWA Lakes
        :param float lake_ha: Minimum Lake Size
        :param str harvest: Harvest Data Constraint
        :param str gdb: Path to Working Geodatabase
        :param logger: logger object for console and log file reporting
    """
    return


###############################################################################
# register event (runs the helper with different configurations)
if __name__ == '__main__':
    try:
        import arcpy
    except:
        logging.error('No ArcGIS licenses available to run this tool.  Program terminating.')
        sys.exit(1)
    run_app()
else:
    try:
        # Import arcpy
        import arcpy
    except:
        logging.error('No ArcGIS licenses available to run this tool.  Program terminating.')
        sys.exit(1)
