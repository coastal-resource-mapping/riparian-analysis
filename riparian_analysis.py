"""
For documentation purpose, run the script with -h flag
"""

import os
import sys
import logging
import collections

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
    gdb, aoi_file, aoi_fld, aoi_name, vri, tsa, tfl, private, bec, fwa, lake_ha, harvest, buffer_dist, dem, roads, \
        streams, bridges, blocks, fish, logger = get_input_parameters()

    final_lakes, criteria_lakes, study_area, vri_aoi = extract_lakes(aoi_file, aoi_fld, aoi_name, vri, tsa, tfl,
                                                                     private, bec, fwa, lake_ha, harvest, gdb, logger)

    lakes, buffer_lakes = buffer_analysis(final_lakes, criteria_lakes, buffer_dist, gdb, logger)

    watersheds, dem_aoi = watershed_buffer(study_area, dem, lakes, buffer_lakes,
                                           vri_aoi, roads, streams, bridges, gdb, logger)

    watersheds, bec_label_fields, bec_zone_fields, non_forest_fields =\
        watershed_characteristics(watersheds, final_lakes, streams, tsa, tfl, vri_aoi,
                                  private, blocks, fish, roads, bec, dem_aoi, gdb, logger)

    export_tables(final_lakes, criteria_lakes, watersheds, gdb, bec_label_fields,
                  bec_zone_fields, non_forest_fields, logger)


###############################################################################
# handler (defines core functions of the application)

def get_input_parameters():
    """
        Parse arguments and set up logger
    """
    try:
        parser = ArgumentParser(
            description='Populate this with a description of the tool')
        parser.add_argument('gdb', help='Path to Output Geodatabase')
        parser.add_argument('aoi_file', help='Path to AOI')
        parser.add_argument('aoi_fld', nargs='?', help='AOI Name Field')
        parser.add_argument('aoi_name', nargs='?', help='AOI Name Value')
        parser.add_argument('vri', help='Path to VRI')
        parser.add_argument('tsa', help='Path to TSA')
        parser.add_argument('tfl', help='Path to TFL')
        parser.add_argument('private', help='Path to Private Land')
        parser.add_argument('bec', help='Path to BEC')
        parser.add_argument('fwa', help='Path to FWA Lakes')
        parser.add_argument('lake_ha', nargs='?', help='Minimum Lake Size')
        parser.add_argument('harvest', help='Harvest Data Constraint')
        parser.add_argument('buffer', help='Buffer distances for lakes '
                                           '(comma separated distances in metres eg. 10,30,50)')
        parser.add_argument('dem', help='Path to DEM')
        parser.add_argument('roads', help='Path to Roads')
        parser.add_argument('streams', help='Path to Streams')
        parser.add_argument('bridges', help='Path to Coastal Bridges')
        parser.add_argument('blocks', help='Path to Cut Blocks')
        parser.add_argument('fish', help='Path to Fish Observations')
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

        return args.gdb, args.aoi_file, args.aoi_fld, args.aoi_name, args.vri, args.tsa, args.tfl, args.private,\
            args.bec, args.fwa, args.lake_ha, args.harvest, args.buffer, args.dem, args.roads, args.streams,\
            args.bridges, args.blocks, args.fish, logger

    except Exception as e:
        logging.error('Unexpected exception. Program terminating.')
        raise Exception('Errors exist')


def extract_lakes(aoi_file, aoi_fld, aoi_name, vri, tsa, tfl, private, bec, fwa, lake_ha, harvest, working_gdb, logger):
    """
        -Extracts lakes from the VRI file
        -Identifies administrative boundaries.
        - Derive lake characteristics.

        :param str aoi_file: Path to AOI file
        :param str aoi_fld: AOI name field
        :param str aoi_name: AOI name value
        :param str vri: Path to VRI
        :param str tsa: Path to TSA
        :param str tfl: Path to TFL
        :param str private: Path to Private Land
        :param str bec: Path to BEC
        :param str fwa: Path to FWA Lakes
        :param float lake_ha: Minimum Lake Size
        :param str harvest: Harvest Data Constraint
        :param str working_gdb: Path to Working Geodatabase
        :param logger: logger object for console and log file reporting
    """
    logger.info('********************************')
    logger.info('Initiating Step 1 - Extract Lakes Process')
    logger.info('********************************')

    # Variables
    arcpy.env.workspace = working_gdb
    field_mappings = arcpy.FieldMappings()
    fld_vri_lake_extract = 'BCLCS_LEVEL_5'
    vri_lake_values = ['LA', 'RE']
    fld_age_class = 'Age_Class'
    fld_proj_age = 'PROJ_AGE_CLASS_CD_1'
    fld_poly_id = 'WATERBODY_POLY_ID'
    fld_wtrshd_50k = 'WATERSHED_CODE_50K'
    fld_gnis_name = 'GNIS_NAME_1'
    fld_lake_area = 'Lakes_Area_Ha'
    fld_lake_prmtr = 'Lakes_Prmtr'
    fld_bec_label = 'MAP_LABEL'
    null_id_replace = 999900000

    arcpy.env.overwriteOutput = True

    fields_to_keep = ['FEATURE_ID', 'INTERPRETATION_DATE', 'PROJECT', 'BEC_ZONE_CODE', 'BEC_SUBZONE', 'BEC_VARIANT',
                      'MAP_LABEL', 'TSA_NUMBER', 'FOR_FL_ID', 'OWNER_TYPE', 'Shape', 'Shape_Area', 'Shape_Length',
                      'OBJECTID_1', fld_bec_label, fld_lake_prmtr, fld_lake_area]
    lakes_fields_delete = ['INSIDE_Z', 'INSIDE_M', 'Join_Count', 'Join_Count_1', 'Shape_Area_1', 'Shape_Length_1']

    if aoi_fld == '#':
        aoi_fld = None
    if aoi_name == '#':
        aoi_name = None
    if lake_ha == '#':
        lake_ha = None

    # Creating a VRI value string usable in a SQL statement
    vri_filter_values = ''
    for name in vri_lake_values:
        vri_filter_values += "'" + name.strip("'") + "'"
        if name != vri_lake_values[-1]:
            vri_filter_values += ', '

    # Set up path variables
    aoi_file_study_area = os.path.join(working_gdb, os.path.basename(aoi_file) + '_Study_Area')
    vri_aoi = os.path.join(working_gdb, 'VRI_Study_Area')
    vri_lakes = os.path.join(working_gdb, 'VRI_Lakes')
    bec_lakes = os.path.join(working_gdb, 'BEC_Lakes')
    tsa_lakes = os.path.join(working_gdb, 'VRI_Lakes_TSA')
    tfl_lakes = os.path.join(working_gdb, 'VRI_Lakes_TFL')
    private_lakes = os.path.join(working_gdb, 'VRI_Lakes_Private')
    fwa_lakes = os.path.join(working_gdb, 'VRI_Lakes_FWA')
    dissolve_lakes = os.path.join(working_gdb, 'VRI_Lakes_Dissolve')
    final_lakes = os.path.join(working_gdb, 'Lakes_Final')
    criteria_lakes = os.path.join(working_gdb, 'Lakes_Criteria')

    # Extract the AOI from the input dataset
    logger.info('Extracting AOI...')
    if aoi_fld and aoi_name:
        # Creating an AOI value string usable in a SQL statement
        aoi_name_list = aoi_name.split(';')
        aoi_name_values = ''
        for name in aoi_name_list:
            aoi_name_values += "'" + name.strip("'") + "'"
            if name != aoi_name_list[-1]:
                aoi_name_values += ', '
        arcpy.Select_analysis(aoi_file, aoi_file_study_area, '{0} IN ({1})'.format(aoi_fld, aoi_name_values))
    else:
        arcpy.Copy_management(aoi_file, aoi_file_study_area)

    # Clip the VRI to the AOI
    logger.info('Clipping VRI to AOI...')
    arcpy.Clip_analysis(vri, aoi_file_study_area, vri_aoi)

    # Extract lakes from the VRI using the VRI filter values (LA, RE)on the BCLCS_Level_5 field
    logger.info('Extracting Lakes from the VRI...')
    arcpy.Select_analysis(vri_aoi, vri_lakes, '{0} IN ({1})'.format(fld_vri_lake_extract, vri_filter_values))

    # Delete unnecessary fields
    fc_fields = [f.name for f in arcpy.ListFields(vri_lakes)]
    fields_delete = list(set(fc_fields) - set(fields_to_keep))
    arcpy.DeleteField_management(vri_lakes, fields_delete)

    # Join the BEC Label field to the VRI Lakes using spatial join
    logger.info('Joining BEC Label to VRI Lakes...')
    field_mappings.addTable(bec)
    field_mappings.addTable(vri_lakes)

    # Removing fields that should not be included in the join
    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(vri_lakes, bec, bec_lakes, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', field_mappings, 'WITHIN', '')

    # Joining the TSA to the lakes
    field_mappings.addTable(tsa)
    field_mappings.addTable(bec_lakes)
    logger.info('Joining TSA Information to VRI Lakes...')
    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(bec_lakes, tsa, tsa_lakes, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', field_mappings, 'WITHIN', '')

    # Joining the TFL to the lakes
    logger.info('Joining TFL Information to VRI Lakes...')
    field_mappings.addTable(tfl)
    field_mappings.addTable(tsa_lakes)

    # Removing fields that should not be included in the join
    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(tsa_lakes, tfl, tfl_lakes, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', field_mappings, 'WITHIN', '')

    # Joining Private Land to the lakes
    logger.info('Joining Private Land Information to VRI Lakes...')
    field_mappings.addTable(private)
    field_mappings.addTable(tfl_lakes)

    # Removing fields that should not be included in the join
    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    # Spatial joins to TFL layer
    arcpy.SpatialJoin_analysis(tfl_lakes, private, private_lakes, 'JOIN_ONE_TO_ONE',
                               'KEEP_ALL', field_mappings, 'WITHIN', '')

    # Joining FWA Lakes to the VRI Lakes
    logger.info('Joining FWA Lake Information to VRI Lakes...')
    arcpy.SpatialJoin_analysis(private_lakes, fwa, fwa_lakes, '', 'KEEP_ALL', '', 'INTERSECT')
    arcpy.DeleteField_management(fwa_lakes, 'Join_Count')

    # Replace any NULL Poly ID values with an incrementing ID value
    logger.info('Filling NULL Values in ' + fld_poly_id + '...')
    with arcpy.da.UpdateCursor(fwa_lakes, fld_poly_id) as u_cursor:
        for row in u_cursor:
            if not row[0]:
                row[0] = null_id_replace
                null_id_replace += 1
                u_cursor.updateRow(row)

    logger.info('Dissolving Lakes based on ' + fld_poly_id + '...')
    arcpy.Dissolve_management(fwa_lakes, dissolve_lakes, [fld_poly_id, fld_wtrshd_50k, fld_gnis_name])

    logger.info('Applying Attributes...')
    arcpy.SpatialJoin_analysis(dissolve_lakes, private_lakes, final_lakes, '', 'KEEP_ALL', '', 'INTERSECT')

    # Create and calculates area, perimeter, centroid x/y fields
    logger.info('Adding Geometry Information...')
    arcpy.AddField_management(final_lakes, fld_lake_area, 'Double')
    arcpy.CalculateField_management(final_lakes, fld_lake_area, '!SHAPE.area@HECTARES!', 'PYTHON')
    arcpy.AddField_management(final_lakes, fld_lake_prmtr, 'Double')
    arcpy.CalculateField_management(final_lakes, fld_lake_prmtr, '!SHAPE.length@METERS!', 'PYTHON')
    arcpy.AddGeometryAttributes_management(final_lakes, 'CENTROID_INSIDE')
    arcpy.DeleteField_management(final_lakes, lakes_fields_delete)

    # Output statistics to the logger
    logger.info('--------------------------------')
    lake_count = 0
    all_lakes = ''
    if not lake_ha and harvest == 'NONE':
        logger.info('No criteria selection was used')
        all_lakes = ', all lakes will be used in the analysis'
    elif not lake_ha and harvest != 'NONE':
        arcpy.Select_analysis(final_lakes, criteria_lakes, harvest)
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Administrative Boundary Criteria ({0})'.format(harvest))
    elif lake_ha and harvest == 'NONE':
        arcpy.Select_analysis(final_lakes, criteria_lakes, fld_lake_area + '{0} >= {1}'.format(fld_lake_area, lake_ha))
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Minimum Lake Size Criteria (Area >= {0} Ha)'.format(lake_ha))
    else:
        arcpy.Select_analysis(final_lakes, criteria_lakes, 
                              '({0}) AND {1} >= {2}'.format(harvest, fld_lake_area, lake_ha))
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Administrative Boundary ({0}) and '
                    'Minimum Lake Size Criteria (Area >= {1} Ha)'.format(harvest, lake_ha))
    logger.info('There are {0} lake(s) that have been selected{1}'.format(lake_count, all_lakes))
    logger.info('--------------------------------')

    # Create and categorize PROJ_AGE_CLASS_CD_1 into readable age categories
    arcpy.AddField_management(vri_aoi, fld_age_class, 'TEXT', '10')
    with arcpy.da.UpdateCursor(vri_aoi, [fld_proj_age, fld_age_class]) as u_cursor:
        for row in u_cursor:
            if row[0] == '1':
                row[1] = '1-20'
            elif row[0] == '2':
                row[1] = '21-40'
            elif row[0] == '3':
                row[1] = '41-60'
            elif row[0] == '4':
                row[1] = '61-80'
            elif row[0] == '5':
                row[1] = '81-100'
            elif row[0] == '6':
                row[1] = '101-120'
            elif row[0] == '7':
                row[1] = '121-140'
            elif row[0] == '8':
                row[1] = '141-250'
            else:
                row[1] = '251+'
            u_cursor.updateRow(row)

    # Delete any intermediate files
    logger.info('Deleting Intermediate Files')
    for fc in [vri_lakes, bec_lakes, tsa_lakes, tfl_lakes, private_lakes, fwa_lakes, dissolve_lakes]:
        arcpy.Delete_management(fc)

    logger.info('********************************')
    logger.info('Completed Step 1 - Extract Lakes Process')
    logger.info('********************************')

    return final_lakes, criteria_lakes, aoi_file_study_area, vri_aoi


def buffer_analysis(lakes_final, lakes_criteria, buffer_dist, working_gdb, logger):
    """
    - Selects lakes based of identified boundaries (TSA, TFL, Private Land, etc)
    - Creates three buffers around the selected lake

    :param str lakes_final: Lakes_Final from Extract Lakes
    :param str lakes_criteria: Lakes_Criteria from Extract Lakes
    :param str buffer_dist: Buffer distances for lakes (comma separated distances in metres eg. 10,30,50)
    :param str working_gdb: path to the working geodatabase
    :param logger: logger object for console and log file reporting
    :return:
    """
    logger.info('********************************')
    logger.info('Initiating Step 2 - Buffer Analysis Process')
    logger.info('********************************')

    # CHeck to see if a selection criteria was used in the first step.  If not, use all lakes going forward
    if int(arcpy.GetCount_management(lakes_criteria).getOutput(0)) > 0:
        lakes = lakes_criteria
    else:
        lakes = lakes_final

    # Buffer lakes using distances specified by the user
    logger.info('Buffering {0} Lake(s) using the '
                'following distances: {1}...'.format(int(arcpy.GetCount_management(lakes).getOutput(0)), buffer_dist))

    buffer_lakes = os.path.join(working_gdb, 'Lakes_Buffer')
    buffer_dist = buffer_dist.replace(' ', '').replace(',', ';')
    fld_buff_dist = 'Buffer_Distance'
    fld_buff_area = 'Buffer_Area'
    fld_buff_prmtr = 'Buffer_Prmtr'

    arcpy.MultipleRingBuffer_analysis(lakes, buffer_lakes, buffer_dist, 'Meters', 'distance', 'NONE', 'OUTSIDE_ONLY')

    # Add geometry information
    logger.info('Add Geometry Attributes...')
    arcpy.AddField_management(buffer_lakes, fld_buff_dist, 'DOUBLE')
    arcpy.CalculateField_management(buffer_lakes, fld_buff_dist, '!distance!', 'PYTHON')
    arcpy.AddField_management(buffer_lakes, fld_buff_area, 'DOUBLE')
    arcpy.CalculateField_management(buffer_lakes, fld_buff_area, '!SHAPE.area@HECTARES!', 'PYTHON')
    arcpy.AddField_management(buffer_lakes, fld_buff_prmtr, 'Double')
    arcpy.CalculateField_management(buffer_lakes, fld_buff_prmtr, '!SHAPE.length@METERS!', 'PYTHON')

    arcpy.DeleteField_management(buffer_lakes, 'distance')

    logger.info('********************************')
    logger.info('Completed Step 2 - Buffer Analysis Process')
    logger.info('********************************')

    return lakes, buffer_lakes


def watershed_buffer(aoi, dem, lakes, buffer_lakes, vri, roads, streams, bridges, working_gdb, logger):
    """
    - Creates watershed boundary with the selected lake as the pour point.
    - Adds characteristics to the buffers around the lake (created in previous tool). These characteristics include:
    - Biogeoclimatic Ecosystem Classification (BEC)
    - Age class category
    - Harvest history
    - Road density

    :param str aoi: Path to AOI
    :param str dem: Path to DEM
    :param str lakes: Lakes to run through
    :param str buffer_lakes: Buffer polygons from Buffer Analysis
    :param str vri: Path to VRI clipped to AOI
    :param str roads: Path to roads
    :param str streams: Path to streams
    :param str bridges: Path to coastal bridges
    :param str working_gdb: Path to working geodatabase
    :param logger: logger object for console and log file reporting
    :return:
    """

    logger.info('********************************')
    logger.info('Initiating Step 3 - Watershed Buffer Characteristics Process')
    logger.info('********************************')

    # Variables
    fld_poly_id = 'WATERBODY_POLY_ID'
    fld_pour_point_id = 'Value'
    fld_bec_area = 'BEC_Area_Ha'
    fld_harv_date_old = 'harvest_date'
    fld_proj_age_old = 'proj_age_1'
    fld_harv_date_new = 'Harvest_Year'
    fld_proj_age_new = 'Proj_Age'
    fld_buff_dist = 'Buffer_Distance'
    fld_buff_area = 'Buffer_Area'
    fld_buff_prmtr = 'Buffer_Prmtr'
    fld_road_length = "Road_Length"

    # Lists
    keep_fields = ['WATERBODY_POLY_ID', 'BUFF_DIST', 'BEC_ZONE_CODE', 'BEC_SUBZONE', 'BEC_VARIANT',
                   'HARVEST_DATE', 'PROJ_AGE_1', 'Age_Class', 'BEC_Area_Ha', 'INSIDE_X', 'INSIDE_Y',
                   'INTERPRETATION_DATE', 'OBJECTID_1', 'OBJECTID', 'Shape', 'Shape_Area', 'Shape_Length',
                   fld_buff_dist, fld_buff_area, fld_buff_prmtr]

    # Set environment settings
    arcpy.env.overwriteOutput = True
    arcpy.env.mask = aoi

    # Set up path variables
    dem_aoi = os.path.join(working_gdb, 'DEM_Study_Area')
    dem_fill = os.path.join(working_gdb, 'DEM_Fill')
    flow_dir = os.path.join(working_gdb, 'Flow_Direction')
    pour_point = os.path.join(working_gdb, 'Pour_Points')
    watersheds = os.path.join(working_gdb, 'Watersheds')
    watershed_poly = os.path.join(working_gdb, 'Watersheds_Polygon')
    selected_watersheds = os.path.join(working_gdb, 'Selected_Watersheds')
    lakes_buffer_watershed = os.path.join(working_gdb, 'Lakes_Buffer_Watershed')
    lakes_buffer_attributes = os.path.join(working_gdb, 'Lakes_Buffer_Attributes')
    selected_roads = os.path.join(working_gdb, 'Selected_Roads')
    selected_bridges = os.path.join(working_gdb, 'Selected_Bridges')
    selected_streams = os.path.join(working_gdb, 'Selected_Streams')

    # Check if coastal bridges was entered by the user
    if bridges == '#':
        bridges = None

    # Extract the DEM and run the watershed generation process using the lakes as pour points
    logger.info('Clipping DEM to Study Area...')
    out_extract = arcpy.sa.ExtractByMask(dem, aoi)
    out_extract.save(dem_aoi)

    logger.info('Filling DEM...')
    out_fill = arcpy.sa.Fill(dem_aoi)
    out_fill.save(dem_fill)

    logger.info('Creating Flow Direction...')
    out_flow = arcpy.sa.FlowDirection(dem_fill)
    out_flow.save(flow_dir)

    logger.info('Creating Pour Points from Lakes...')
    arcpy.PolygonToRaster_conversion(lakes, fld_poly_id, pour_point, 'CELL_CENTER', 'NONE', 25)

    logger.info('Creating Watersheds...')
    out_watershed = arcpy.sa.Watershed(flow_dir, pour_point, fld_pour_point_id)
    out_watershed.save(watersheds)

    logger.info('Converting Watersheds to Polygon...')
    arcpy.RasterToPolygon_conversion(watersheds, watershed_poly, 'SIMPLIFY', fld_pour_point_id)

    arcpy.AddField_management(vri, fld_bec_area, 'Double')
    arcpy.CalculateField_management(vri, fld_bec_area, '!SHAPE.area@HECTARES!', 'PYTHON')

    # Cleaning up watersheds to remove small segmentation
    logger.info('Cleaning up Watersheds...')
    watershed_lyr = arcpy.MakeFeatureLayer_management(watershed_poly, 'watershed_lyr')
    lake_lyr = arcpy.MakeFeatureLayer_management(lakes, 'lakes_lyr')
    arcpy.SelectLayerByLocation_management(watershed_lyr, 'INTERSECT', lake_lyr, '', 'NEW_SELECTION')
    arcpy.CopyFeatures_management(watershed_lyr, selected_watersheds)
    arcpy.Delete_management(watershed_lyr)
    arcpy.Delete_management(lake_lyr)

    arcpy.AlterField_management(selected_watersheds, 'gridcode', fld_poly_id, fld_poly_id)

    # Using the watersheds to clip the buffer zones
    logger.info('Clipping Buffer Zones to Watersheds...')
    arcpy.Clip_analysis(buffer_lakes, selected_watersheds, lakes_buffer_watershed)

    # Intersecting the buffer zones with the VRI to get attributes
    logger.info('Intersecting Buffer Zones with the VRI...')
    arcpy.Intersect_analysis([lakes_buffer_watershed, vri], lakes_buffer_attributes, 'ALL')

    logger.info('Cleaning up Fields...')
    # Delete unnecessary fields
    fc_fields = [f.name for f in arcpy.ListFields(lakes_buffer_attributes)]
    delete_fields = list(set(fc_fields) - set(keep_fields))
    arcpy.DeleteField_management(lakes_buffer_attributes, delete_fields)

    # Loops through identified layers and alters field names.
    field_list = arcpy.ListFields(lakes_buffer_attributes)
    for field in field_list:
        if field.name.lower() == fld_harv_date_old:
            arcpy.AlterField_management(lakes_buffer_attributes, field.name, fld_harv_date_new, fld_harv_date_new)
        elif field.name.lower() == fld_proj_age_old:
            arcpy.AlterField_management(lakes_buffer_attributes, field.name, fld_proj_age_new, 'Projected_Age_Class')

    # Intersecting the buffer zones with the roads
    logger.info('Intersecting Buffer Zones with Roads...')
    arcpy.Intersect_analysis([roads, lakes_buffer_attributes], selected_roads, 'ALL')
    lst_buffer_dist = sorted({row[0] for row in arcpy.da.SearchCursor(lakes_buffer_attributes,
                                                                      fld_buff_dist) if row[0]})

    for dist in lst_buffer_dist:
        dist_lyr = arcpy.MakeFeatureLayer_management(selected_roads, 'dist_lyr', fld_buff_dist + ' = ' + str(dist))
        selected_count = int(arcpy.GetCount_management(dist_lyr).getOutput(0))
        logger.info('There are {0} road(s) within the {1} metre buffer.'.format(selected_count, str(dist)))
        arcpy.Delete_management(dist_lyr)

    arcpy.AddField_management(selected_roads, fld_road_length, 'Double')
    arcpy.CalculateField_management(selected_roads, fld_road_length, '!SHAPE.length@METERS!', 'PYTHON')

    # Intersecting the buffer zones with the Coastal Bridges if they were input by the user
    if bridges:
        logger.info('Intersecting Buffer Zones with Coastal Bridges...')
        arcpy.Intersect_analysis([bridges, lakes_buffer_attributes], selected_bridges, 'ALL')
        for dist in lst_buffer_dist:
            dist_lyr = arcpy.MakeFeatureLayer_management(selected_bridges, 'dist_lyr',
                                                         fld_buff_dist + ' = ' + str(dist))
            selected_count = int(arcpy.GetCount_management(dist_lyr).getOutput(0))
            logger.info('There are {0} coastal bridges within the {1} metre buffer.'.format(selected_count, str(dist)))
            arcpy.Delete_management(dist_lyr)

    logger.info('Intersecting Buffer Zones with Streams...')
    arcpy.Intersect_analysis((lakes_buffer_attributes, streams), selected_streams, 'ALL')

    logger.info('Deleting Intermediate Files')
    for fc in [dem_fill, flow_dir, watershed_poly, watersheds, pour_point, buffer_lakes, lakes_buffer_watershed]:
        arcpy.Delete_management(fc)

    logger.info('********************************')
    logger.info('Completed Step 3 - Watershed Buffer Characteristics Process')
    logger.info('********************************')

    return selected_watersheds, dem_aoi


def watershed_characteristics(watersheds, lakes, streams, tsa, tfl, vri, private, blocks,
                              fish, roads, bec, dem, working_gdb, logger):
    """
    Adds attributes from many different files to each watershed

    :param str watersheds: path to watersheds
    :param str lakes: path to lakes
    :param str streams: path to streams
    :param str tsa: path to TSA
    :param str tfl: path to TFL
    :param str vri: path to VRI
    :param str private: path to private land
    :param str blocks: path to cut blocks
    :param str fish: path to fish observations
    :param str roads: path to roads
    :param bec: path to BEC
    :param dem: path to DEM
    :param working_gdb: path to the output geodatabase
    :param logger: logger object for console and log file reporting
    :return:
    """
    logger.info('********************************')
    logger.info('Initiating Step 4 - Watershed Characteristics Process')
    logger.info('********************************')

    fld_lakes_count = 'LAKES_COUNT'
    fld_lakes_max = 'MAX_LAKES_AREA_HA'
    fld_lakes_min = 'MIN_LAKES_AREA_HA'
    fld_lakes_total = 'LAKES_AREA_HA'
    fld_lakes_perimeter = 'LAKES_PERIMETER_M'
    fld_streams_count = 'STREAM_COUNT'
    fld_poly_id = 'WATERBODY_POLY_ID'
    fld_count_dissolve = 'COUNT_' + fld_poly_id
    fld_area = 'Shape_Area'
    fld_length = 'Shape_Length'
    fld_min_area_dissolve = 'MIN_' + fld_area
    fld_max_area_dissolve = 'MAX_' + fld_area
    fld_sum_area_dissolve = 'SUM_' + fld_area
    fld_sum_length_dissolve = 'SUM_' + fld_length
    fld_fish_obs_type = 'POINT_TYPE_CODE'
    fld_presence_obs = 'FISH_PRESENCE_OBSERVATION'
    fld_presence_smry = 'FISH_PRESENCE_SUMMARY'
    fld_road_length = 'ROAD_LENGTH_M'
    fld_road_area = 'ROAD_AREA_HA'
    fld_bec_label = 'MAP_LABEL'
    fld_new_bec_label = 'BEC_LABEL_'
    fld_bec_zone = 'ZONE'
    fld_new_bec_zone = 'BEC_ZONE_'
    fld_bec_zone_area = 'BEC_ZONE_AREA_HA_'
    fld_non_forest_type = 'NON_FORESTED_TYPE_'
    fld_non_forest_area = 'NON_FORESTED_AREA'
    fld_level_1 = 'BCLCS_LEVEL_1'
    fld_level_2 = 'BCLCS_LEVEL_2'

    # Set up path variables
    lakes_intersect = os.path.join(working_gdb, 'Lakes_Intersect')
    lakes_dissolve = os.path.join(working_gdb, 'Lakes_Dissolve')
    streams_intersect = os.path.join(working_gdb, 'Watershed_Stream_Network')
    streams_dissolve = os.path.join(working_gdb, 'Streams_Dissolve')
    bec_intersect = os.path.join(working_gdb, 'BEC_Intersect')
    bec_label_dissolve = os.path.join(working_gdb, 'BEC_Label_Dissolve')
    bec_zone_dissolve = os.path.join(working_gdb, 'BEC_Zone_Dissolve')
    bec_dissolve = os.path.join(working_gdb, 'BEC_Dissolve')
    fish_intersect = os.path.join(working_gdb, 'Fish_Intersect')
    fish_dissolve = os.path.join(working_gdb, 'Fish_Dissolve')
    roads_intersect = os.path.join(working_gdb, 'Roads_Intersect')
    roads_dissolve = os.path.join(working_gdb, 'Roads_Dissolve')
    roads_buffer = os.path.join(working_gdb, 'Roads_Buffer')
    roads_clip = os.path.join(working_gdb, 'Roads_Clip')
    vri_non_forest = os.path.join(working_gdb, 'VRI_NonForest')
    vri_intersect = os.path.join(working_gdb, 'VRI_Intersect')
    vri_dissolve = os.path.join(working_gdb, 'VRI_Dissolve')
    slope = os.path.join(working_gdb, 'Slope')
    slope_statistics = os.path.join(working_gdb, 'Slope_Statistics')

    tsa_fields = ['TSA_NUMBER', 'TSNMBRDSCR']
    tfl_fields = ['FOR_FL_ID']
    private_fields = ['OWNER_TYPE']
    block_fields = ['AREA_HA', 'HARVEST_YEAR']
    vri_fields = ['PROJECT', 'PROJ_AGE_1', 'PROJ_AGE_CLASS_CD_1', 'PROJECTED_DATE', 'SPECIES_CD_1',
                  'SPECIES_PCT_1', 'SPECIES_CD_2', 'SPECIES_PCT_2', 'SPECIES_CD_3', 'SPECIES_PCT_3', 'SPECIES_CD_4',
                  'SPECIES_PCT_4', 'SPECIES_CD_5', 'SPECIES_PCT_5', 'SPECIES_CD_6', 'SPECIES_PCT_6']
    rename_fields = [['TSNMBRDSCR', 'TSA_NAME'],
                     ['PROJ_AGE_1', 'PROJECTED_AGE'],
                     ['PROJ_AGE_CLASS_CD_1', 'PROJECTED_AGE_CLASS'],
                     ['AREA_HA', 'HARVEST_AREA_HA'],
                     ['MIN', 'SLOPE_PERC_MIN'],
                     ['MAX', 'SLOPE_PERC_MAX'],
                     ['MEAN', 'SLOPE_PERC_MEAN']]

    max_label_count = 0
    max_zone_count = 0
    dict_bec_label = collections.defaultdict(list)
    dict_bec_zone = collections.defaultdict(list)
    dict_non_forest = collections.defaultdict(list)
    bec_label_fields = []
    bec_zone_fields = []
    non_forest_fields = []

    arcpy.env.overwriteOutput = True

    # Getting statistics of lakes
    logger.info('Intersecting Watersheds with Lakes...')
    arcpy.Intersect_analysis([watersheds, lakes], lakes_intersect, 'ALL')

    logger.info('Dissolving Intersected Lakes...')
    arcpy.Dissolve_management(lakes_intersect, lakes_dissolve, [fld_poly_id],
                              '{0} SUM;{0} MIN;{0} MAX;{1} COUNT; {2} SUM'.format(fld_area, fld_poly_id, fld_length))
    # Add Fields
    arcpy.AddField_management(lakes_dissolve, fld_lakes_total, 'Double')
    arcpy.AddField_management(lakes_dissolve, fld_lakes_min, 'Double')
    arcpy.AddField_management(lakes_dissolve, fld_lakes_max, 'Double')
    arcpy.AddField_management(lakes_dissolve, fld_lakes_count, 'Double')
    arcpy.AddField_management(lakes_dissolve, fld_lakes_perimeter, 'Double')

    with arcpy.da.UpdateCursor(lakes_dissolve,
                               [fld_lakes_total, fld_lakes_min, fld_lakes_max, fld_lakes_count, fld_lakes_perimeter,
                                fld_sum_area_dissolve, fld_min_area_dissolve, fld_max_area_dissolve, fld_count_dissolve,
                                fld_sum_length_dissolve]
                               ) as u_cursor:
        for row in u_cursor:
            row[0] = row[5]/10000
            row[1] = row[6]/10000
            row[2] = row[7]/10000
            row[3] = row[8]
            row[4] = row[9]
            u_cursor.updateRow(row)

    arcpy.JoinField_management(watersheds, fld_poly_id, lakes_dissolve, fld_poly_id,
                               [fld_lakes_total, fld_lakes_min, fld_lakes_max, fld_lakes_perimeter, fld_lakes_count])

    # Adding Streams statistics
    logger.info('Intersecting Watersheds with Streams...')
    arcpy.Intersect_analysis([watersheds, streams], streams_intersect, 'ALL')

    logger.info('Dissolving Intersected Streams...')
    arcpy.Dissolve_management(streams_intersect, streams_dissolve, [fld_poly_id], '{0} COUNT'.format(fld_poly_id))

    # Add Fields
    arcpy.AddField_management(streams_dissolve, fld_streams_count, 'Double')
    with arcpy.da.UpdateCursor(streams_dissolve, [fld_streams_count, fld_count_dissolve]) as u_cursor:
        for row in u_cursor:
            row[0] = row[1]
            u_cursor.updateRow(row)

    arcpy.JoinField_management(watersheds, fld_poly_id, streams_dissolve, fld_poly_id, [fld_streams_count])

    # Adding TSA fields
    logger.info('Adding TSA Attributes...')
    add_attributes(watersheds, tsa, tsa_fields, working_gdb)

    # Adding BEC attributes dynamically dependant upon how many intersect the watersheds
    logger.info('Adding BEC Attributes...')
    arcpy.Intersect_analysis([watersheds, bec], bec_intersect, 'ALL')
    arcpy.Dissolve_management(bec_intersect, bec_dissolve, [fld_poly_id, fld_bec_label])
    arcpy.Dissolve_management(bec_dissolve, bec_label_dissolve, [fld_poly_id], '{0} COUNT'.format(fld_bec_label))

    with arcpy.da.SearchCursor(bec_dissolve, [fld_poly_id, fld_bec_label],
                               sql_clause=(None, 'ORDER BY {0}, {1}'.format(fld_poly_id, fld_bec_label))) as s_cursor:
        for row in s_cursor:
            dict_bec_label[row[0]].append(row[1])

    arcpy.Dissolve_management(bec_intersect, bec_dissolve, [fld_poly_id, fld_bec_zone])
    arcpy.Dissolve_management(bec_dissolve, bec_zone_dissolve, [fld_poly_id],
                              '{0} COUNT;{1} SUM'.format(fld_bec_zone, fld_area))

    with arcpy.da.SearchCursor(bec_dissolve, [fld_poly_id, fld_bec_zone, fld_area],
                               sql_clause=(None, 'ORDER BY {0}, {1}'.format(fld_poly_id,
                                                                            fld_bec_zone, fld_area))) as s_cursor:
        for row in s_cursor:
            dict_bec_zone[row[0]].append([row[1], row[2]/10000])

    with arcpy.da.SearchCursor(bec_label_dissolve, ['COUNT_' + fld_bec_label]) as s_cursor:
        for row in s_cursor:
            if int(row[0]) > max_label_count:
                max_label_count = int(row[0])

    with arcpy.da.SearchCursor(bec_zone_dissolve, ['COUNT_' + fld_bec_zone]) as s_cursor:
        for row in s_cursor:
            if int(row[0]) > max_zone_count:
                max_zone_count = int(row[0])

    for i in range(1, max_label_count + 1):
        arcpy.AddField_management(watersheds, fld_new_bec_label + str(i), 'TEXT')
        bec_label_fields.append(fld_new_bec_label + str(i))

    for i in range(1, max_zone_count + 1):
        arcpy.AddField_management(watersheds, fld_new_bec_zone + str(i), 'TEXT')
        arcpy.AddField_management(watersheds, fld_bec_zone_area + str(i), 'DOUBLE')

        bec_zone_fields.extend([fld_new_bec_zone + str(i), fld_bec_zone_area + str(i)])

    with arcpy.da.UpdateCursor(watersheds, [fld_poly_id] + bec_label_fields) as u_cursor:
        for row in u_cursor:
            if row[0] in dict_bec_label.keys():
                label_list = dict_bec_label[row[0]]
                label_index = 1
                for label in label_list:
                    row[label_index] = label
                    label_index += 1
                u_cursor.updateRow(row)

    with arcpy.da.UpdateCursor(watersheds, [fld_poly_id] + bec_zone_fields) as u_cursor:
        for row in u_cursor:
            if row[0] in dict_bec_zone.keys():
                zone_list = dict_bec_zone[row[0]]
                zone_index = 1
                for zone in zone_list:
                    for item in zone:
                        row[zone_index] = item
                        zone_index += 1
                u_cursor.updateRow(row)

    # Adding TFL attributes
    logger.info('Adding TFL Attributes...')
    add_attributes(watersheds, tfl, tfl_fields, working_gdb)

    # Adding Private Land attributes
    logger.info('Adding Private Land Attributes...')
    add_attributes(watersheds, private, private_fields, working_gdb)

    # Adding Cut Block attributes
    logger.info('Adding Cut Block Attributes...')
    add_attributes(watersheds, blocks, block_fields, working_gdb)

    # Adding VRI attributes
    logger.info('Adding VRI Attributes...')
    add_attributes(watersheds, vri, vri_fields, working_gdb)

    arcpy.Select_analysis(vri, vri_non_forest,
                          '{0} = \'N\' OR (({0} = \'V\' AND {1} = \'N\'))'.format(fld_level_1, fld_level_2))
    arcpy.Intersect_analysis([vri_non_forest, watersheds], vri_intersect, 'ALL')
    arcpy.Dissolve_management(vri_intersect, vri_dissolve, [fld_poly_id, fld_level_1, fld_level_2])

    with arcpy.da.SearchCursor(vri_dissolve, [fld_poly_id, fld_level_1, fld_level_2, fld_area]) as s_cursor:
        for row in s_cursor:
            dict_non_forest[row[0]] = [row[1], row[2], row[3]/10000]

    arcpy.AddField_management(watersheds, fld_non_forest_type + '1', 'TEXT')
    arcpy.AddField_management(watersheds, fld_non_forest_type + '2', 'TEXT')
    arcpy.AddField_management(watersheds, fld_non_forest_area, 'DOUBLE')
    non_forest_fields.extend([fld_non_forest_type + '1', fld_non_forest_type + '2', fld_non_forest_area])

    with arcpy.da.UpdateCursor(watersheds, [fld_poly_id] + non_forest_fields) as u_cursor:
        for row in u_cursor:
            if row[0] in dict_non_forest.keys():
                non_forest = dict_non_forest[row[0]]
                row[1] = non_forest[0]
                if non_forest[0] == 'V':
                    row[2] = non_forest[1]
                row[3] = non_forest[2]
            u_cursor.updateRow(row)

    # Adding Fish Observation attributes
    logger.info('Adding Fish Observation Attributes...')
    arcpy.Intersect_analysis([watersheds, fish], fish_intersect, 'ALL')
    arcpy.Dissolve_management(fish_intersect, fish_dissolve, [fld_poly_id, fld_fish_obs_type],
                              '{0} COUNT'.format(fld_poly_id))

    for fish_type in ['Summary', 'Observation']:
        fish_lyr = arcpy.MakeFeatureLayer_management(fish_dissolve, 'fish_lyr',
                                                     '{0} = \'{1}\''.format(fld_fish_obs_type, fish_type))
        arcpy.JoinField_management(watersheds, fld_poly_id, fish_lyr, fld_poly_id, [fld_count_dissolve])

        if fish_type == 'Summary':
            use_field = fld_presence_smry
        else:
            use_field = fld_presence_obs

        arcpy.AlterField_management(watersheds, fld_count_dissolve, use_field)

    # Adding Road attributes
    logger.info('Adding Road Attributes...')
    arcpy.Intersect_analysis([watersheds, roads], roads_intersect, 'ALL')
    arcpy.Dissolve_management(roads_intersect, roads_dissolve, fld_poly_id, '{0} SUM'.format(fld_length))
    arcpy.JoinField_management(watersheds, fld_poly_id, roads_dissolve, fld_poly_id, [fld_sum_length_dissolve])
    arcpy.AlterField_management(watersheds, fld_sum_length_dissolve, fld_road_length)

    arcpy.Buffer_analysis(roads_intersect, roads_buffer, '3 Meters', 'FULL', 'ROUND', 'LIST', fld_poly_id, 'PLANAR')
    arcpy.Clip_analysis(roads_buffer, watersheds, roads_clip)
    arcpy.AddField_management(roads_clip, fld_road_area, 'DOUBLE')
    arcpy.CalculateField_management(roads_clip, fld_road_area, '!SHAPE.area@HECTARES!', 'PYTHON')
    arcpy.JoinField_management(watersheds, fld_poly_id, roads_clip, fld_poly_id, [fld_road_area])

    # Creating slope surface and adding attributes
    logger.info('Adding Slope Attributes...')
    slope_ras = arcpy.sa.Slope(dem, 'PERCENT_RISE')
    slope_ras.save(slope)

    slope_tab = arcpy.sa.ZonalStatisticsAsTable(watersheds, fld_poly_id, slope_ras, slope_statistics, 'DATA', 'ALL')
    arcpy.JoinField_management(watersheds, fld_poly_id, slope_statistics, fld_poly_id, ['MIN', 'MAX', 'MEAN'])

    logger.info('Cleaning up Fields...')
    alter_fields(watersheds, rename_fields)

    logger.info('Deleting Intermediate Files')
    for fc in [streams_dissolve, fish_dissolve, fish_intersect, roads_intersect, roads_dissolve, roads_buffer,
               roads_clip, bec_intersect, bec_dissolve, bec_label_dissolve, bec_zone_dissolve, vri_dissolve,
               vri_intersect, vri_non_forest, slope, slope_statistics, lakes_dissolve, lakes_intersect]:
        arcpy.Delete_management(fc)

    logger.info('********************************')
    logger.info('Completed Step 4 - Watershed Characteristics Process')
    logger.info('********************************')

    return watersheds, bec_label_fields, bec_zone_fields, non_forest_fields


def export_tables(lakes_final, lakes_criteria, watersheds, working_gdb,
                  bec_label_fields, bec_zone_fields, non_forest_fields, logger):
    """
    Export three spatial files as csv
    :param str lakes_final: path to lakes_final feature class
    :param str lakes_criteria: path to lakes criteria feature class
    :param str watersheds: path to watersheds feature class
    :param str working_gdb: path to working geodatabase
    :param bec_label_fields: list of bec label fields to include
    :param bec_zone_fields: list of bec zone fields to include
    :param non_forest_fields: list of vri non-forested fields to include
    :param logger: logging object for logging to the console and log file
    :return:
    """

    logger.info('********************************')
    logger.info('Initiating Export Results')
    logger.info('********************************')
    output_dir = os.path.dirname(working_gdb)
    time_str = dt.now().strftime('%Y%m%d_%H%M%S')

    final_csv = os.path.join(output_dir, 'Lakes_Final_{0}.csv'.format(time_str))
    criteria_csv = os.path.join(output_dir, 'Lakes_Criteria_{0}.csv'.format(time_str))
    watersheds_csv = os.path.join(output_dir, 'Selected_Watersheds_{0}.csv'.format(time_str))
    logger.info('Exporting Lakes_Final...')

    species_fields = []
    for i in range(1, 7):
        species_fields.extend(['SPECIES_CD_' + str(i), 'SPECIES_PCT_' + str(i)])

    lakes_fields = ['WATERBODY_POLY_ID', 'GNIS_NAME_1', 'WATERSHED_CODE_50K', 'MAP_LABEL', 'FEATURE_ID',
                    'INTERPRETATION_DATE', 'PROJECT', 'BEC_ZONE_CODE', 'BEC_SUBZONE', 'BEC_VARIANT', 'TSA_NUMBER',
                    'FOR_FL_ID', 'OWNER_TYPE', 'Lakes_Area_Ha', 'Lakes_Prmtr', 'INSIDE_X', 'INSIDE_Y']
    watershed_fields = ['WATERBODY_POLY_ID', 'TSA_NUMBER', 'TSA_NAME', 
                        'PROJECT'] + bec_zone_fields + bec_label_fields + \
                       ['FOR_FL_ID', 'OWNER_TYPE', 'LAKES_AREA_HA', 'MIN_LAKES_AREA_HA', 'MAX_LAKES_AREA_HA',
                        'LAKES_COUNT', 'LAKES_PERIMETER_M', 'HARVEST_AREA_HA', 'HARVEST_YEAR', 'PROJECTED_AGE',
                        'PROJECTED_AGE_CLASS', 'PROJECTED_DATE'] + non_forest_fields + species_fields + \
                       ['STREAM_COUNT', 'FISH_PRESENCE_SUMMARY', 'FISH_PRESENCE_OBSERVATION', 'ROAD_LENGTH_M',
                        'ROAD_AREA_HA', 'SLOPE_PERC_MIN', 'SLOPE_PERC_MAX', 'SLOPE_PERC_MEAN', 'Shape_Length',
                        'Shape_Area']

    str_lakes = ''
    for item in lakes_fields:
        str_lakes += item
        if item == lakes_fields[-1]:
            str_lakes += '\n'
        else:
            str_lakes += ','

    str_watersheds = ''
    for item in watershed_fields:
        str_watersheds += item
        if item == watershed_fields[-1]:
            str_watersheds += '\n'
        else:
            str_watersheds += ','

    with open(final_csv, 'w') as f:
        f.write(str_lakes)

        with arcpy.da.SearchCursor(lakes_final, lakes_fields) as s_cursor:
            for row in s_cursor:
                csv_line = ''
                for i in range(0, len(lakes_fields)):
                    csv_line += str(row[i])
                    if i < len(lakes_fields) - 1:
                        csv_line += ','
                    else:
                        csv_line += '\n'
                f.write(csv_line.replace('None', ''))

    if int(arcpy.GetCount_management(lakes_criteria).getOutput(0)) > 0:
        logger.info('Exporting Lakes_Criteria...')
        with open(criteria_csv, 'w') as f:
            f.write(str_lakes)

            with arcpy.da.SearchCursor(lakes_criteria, lakes_fields) as s_cursor:
                for row in s_cursor:
                    csv_line = ''
                    for i in range(0, len(lakes_fields)):
                        csv_line += str(row[i])
                        if i < len(lakes_fields) - 1:
                            csv_line += ','
                        else:
                            csv_line += '\n'
                    f.write(csv_line.replace('None', ''))

    logger.info('Exporting Selected_Watersheds...')
    with open(watersheds_csv, 'w') as f:
        f.write(str_watersheds)
        with arcpy.da.SearchCursor(watersheds, watershed_fields) as s_cursor:
            for row in s_cursor:
                csv_line = ''
                for i in range(0, len(watershed_fields)):
                    csv_line += str(row[i])
                    if i < len(watershed_fields) - 1:
                        csv_line += ','
                    else:
                        csv_line += '\n'
                f.write(csv_line.replace('None', ''))

    logger.info('********************************')
    logger.info('Completed Export Results')
    logger.info('********************************')

    return


def add_attributes(input_fc, join_fc, fields, working_gdb):
    """
    Function to add specific attributes from one feature class to another

    :param str input_fc: Path to input feature class
    :param str join_fc: Path to join feature class
    :param list fields: list of fields to join
    :param str working_gdb: Path to the working geodatabase
    :return:
    """

    temp_fc = os.path.join(working_gdb, 'Temp_Join')
    field_mappings = arcpy.FieldMappings()

    field_mappings.addTable(input_fc)
    field_mappings.addTable(join_fc)

    input_fields = [field.name for field in arcpy.ListFields(input_fc)]
    keep_fields = input_fields + fields

    for field in field_mappings.fields:
        if field.name not in keep_fields:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))

    arcpy.SpatialJoin_analysis(input_fc, join_fc, temp_fc, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', field_mappings, 'INTERSECT')
    arcpy.DeleteField_management(temp_fc, ['Join_Count', 'TARGET_FID', 'OBJECTID_1'])
    arcpy.Copy_management(temp_fc, input_fc)
    field_mappings.removeAll()

    del field_mappings
    arcpy.Delete_management(temp_fc)

    return


def alter_fields(input_fc, fields):
    """
    Function to rename fields within a feature class
    :param str input_fc: Path to the input feature class
    :param list fields: list of fields to alter
    :return:
    """

    for pair in fields:
        cur_name = pair[0]
        new_name = pair[1]

        arcpy.AlterField_management(input_fc, cur_name, new_name)

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
        import arcpy
    except:
        logging.error('No ArcGIS licenses available to run this tool.  Program terminating.')
        sys.exit(1)
