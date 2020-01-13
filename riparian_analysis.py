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
    gdb, aoi_file, aoi_fld, aoi_name, vri, tsa, tfl, private, \
        bec, fwa, lake_ha, harvest, buffer_dist, dem, roads, streams, bridges, logger = get_input_parameters()

    final_lakes, criteria_lakes, study_area, vri_aoi = extract_lakes(aoi_file, aoi_fld, aoi_name, vri, tsa, tfl, private,
                                                                     bec, fwa, lake_ha, harvest, gdb, logger)

    lakes, buffer_lakes = buffer_analysis(final_lakes, criteria_lakes, buffer_dist, gdb, logger)

    watershed_buffer(study_area, dem, lakes, buffer_lakes, vri_aoi, roads, streams, bridges, gdb, logger)


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
               args.bridges, logger

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

    # set overwrite option
    arcpy.env.overwriteOutput = True

    # Lists
    dont_delete_fields = ['FEATURE_ID', 'INTERPRETATION_DATE', 'PROJECT', 'Shape_Area', 'Shape_Length', 'BEC_ZONE_CODE',
                          'BEC_SUBZONE', 'BEC_VARIANT', 'MAP_LABEL', 'Shape', 'OBJECTID_1']
    fields_to_keep = ['FEATURE_ID', 'INTERPRETATION_DATE', 'PROJECT', 'BEC_ZONE_CODE', 'BEC_SUBZONE', 'BEC_VARIANT',
                      'TSA_NUMBER', 'FOR_FL_ID', 'OWNER_TYPE', fld_bec_label, fld_lake_prmtr, fld_lake_area]
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

    logger.info('Extracting AOI...')
    if aoi_fld and aoi_name:
        # Creating an AOI value string usable in a SQL statement
        aoi_name_list = aoi_name.split(';')
        aoi_name_values = ''
        for name in aoi_name_list:
            aoi_name_values += "'" + name.strip("'") + "'"
            if name != aoi_name_list[-1]:
                aoi_name_values += ', '
        arcpy.Select_analysis(aoi_file, aoi_file_study_area, '{} IN ({})'.format(aoi_fld, aoi_name_values))
    else:
        arcpy.Copy_management(aoi_file, aoi_file_study_area)

    logger.info('Clipping VRI to AOI...')
    arcpy.Clip_analysis(vri, aoi_file_study_area, vri_aoi)
    logger.info('Extracting Lakes from the VRI...')
    arcpy.Select_analysis(vri_aoi, vri_lakes, '{} IN ({})'.format(fld_vri_lake_extract, vri_filter_values))

    # Delete unnecessary fields
    fc_fields = [f.name for f in arcpy.ListFields(vri_lakes)]
    fields_delete = list(set(fc_fields) - set(dont_delete_fields))
    arcpy.DeleteField_management(vri_lakes, fields_delete)

    logger.info('Joining BEC Label to VRI Lakes...')

    field_mappings.addTable(bec)
    field_mappings.addTable(vri_lakes)

    for field in field_mappings.fields:
        if field.name != fld_bec_label:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(vri_lakes, bec, bec_lakes, "JOIN_ONE_TO_ONE", "KEEP_ALL", field_mappings, "WITHIN", "")

    # Spatial Join to link the tsa to lakes polygon.
    field_mappings.addTable(tsa)
    field_mappings.addTable(bec_lakes)

    logger.info('Joining TSA Information to VRI Lakes...')

    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(bec_lakes, tsa, tsa_lakes, "JOIN_ONE_TO_ONE", "KEEP_ALL", field_mappings, "WITHIN", "")

    logger.info('Joining TFL Information to VRI Lakes...')

    # Spatial Join to link the tfl to lakes polygon.
    field_mappings.addTable(tfl)
    field_mappings.addTable(tsa_lakes)

    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    arcpy.SpatialJoin_analysis(tsa_lakes, tfl, tfl_lakes, "JOIN_ONE_TO_ONE", "KEEP_ALL", field_mappings, "WITHIN", "")

    logger.info('Joining Private Land Information to VRI Lakes...')
    field_mappings.addTable(private)
    field_mappings.addTable(tfl_lakes)

    # Remove all output fields you don't want.
    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))
    # Spatial joins to TFL layer
    arcpy.SpatialJoin_analysis(tfl_lakes, private, private_lakes, "JOIN_ONE_TO_ONE",
                               "KEEP_ALL", field_mappings, "WITHIN", "")

    logger.info('Joining FWA Lake Information to VRI Lakes...')
    arcpy.SpatialJoin_analysis(private_lakes, fwa, fwa_lakes, "", "KEEP_ALL", "", "INTERSECT")
    arcpy.DeleteField_management(fwa_lakes, "Join_Count")

    logger.info('Filling NULL Values in ' + fld_poly_id + '...')
    # Loops through and assigns a unique id in the WATERBODY_POLY_ID to the records that have a 'Null' ID.
    with arcpy.da.UpdateCursor(fwa_lakes, fld_poly_id) as u_cursor:
        for row in u_cursor:
            if not row[0]:
                row[0] = null_id_replace
                null_id_replace += 1
                u_cursor.updateRow(row)

    logger.info('Dissolving Lakes based on ' + fld_poly_id + '...')
    arcpy.Dissolve_management(fwa_lakes, dissolve_lakes, [fld_poly_id, fld_wtrshd_50k, fld_gnis_name])

    logger.info('Applying Attributes...')
    arcpy.SpatialJoin_analysis(dissolve_lakes, private_lakes, final_lakes, "", "KEEP_ALL", "", "INTERSECT")

    # Create and calculates area, perimeter, centroid x/y fields
    logger.info('Adding Geometry Information...')
    arcpy.AddField_management(final_lakes, fld_lake_area, "Double")
    arcpy.CalculateField_management(final_lakes, fld_lake_area, '!SHAPE.area@HECTARES!', "PYTHON")
    arcpy.AddField_management(final_lakes, fld_lake_prmtr, "Double")
    arcpy.CalculateField_management(final_lakes, fld_lake_prmtr, '!SHAPE.length@METERS!', "PYTHON")
    arcpy.AddGeometryAttributes_management(final_lakes, "CENTROID_INSIDE")
    arcpy.DeleteField_management(final_lakes, lakes_fields_delete)

    logger.info('--------------------------------')
    if not lake_ha and harvest == 'NONE':
        
        logger.info('No criteria selection was used')
    elif not lake_ha and harvest != 'NONE':
        arcpy.Select_analysis(final_lakes, criteria_lakes, harvest)
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Administrative Boundary Criteria ({})'.format(harvest))
    elif lake_ha and harvest == 'NONE':
        arcpy.Select_analysis(final_lakes, criteria_lakes, fld_lake_area + ' >= ' + lake_ha)
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Minimum Lake Size Criteria (Area >= {} Ha)'.format(lake_ha))
    else:
        arcpy.Select_analysis(final_lakes, criteria_lakes, '({}) AND {} >= {}'.format(harvest, fld_lake_area, lake_ha))
        lake_count = int(arcpy.GetCount_management(criteria_lakes).getOutput(0))
        logger.info('Extracting Lakes using Administrative Boundary ({}) and '
                    'Minimum Lake Size Criteria (Area >= {} Ha)'.format(harvest, lake_ha))
    logger.info('There are {} lake(s) that have been selected'.format(lake_count))
    logger.info('--------------------------------')

    # Create and categorize PROJ_AGE_CLASS_CD_1 into readable age categories
    arcpy.AddField_management(vri_aoi, fld_age_class, "TEXT", "10")
    with arcpy.da.UpdateCursor(vri_aoi, [fld_proj_age, fld_age_class]) as u_cursor:
        for row in u_cursor:
            if row[0] == "1":
                row[1] = "1-20"
            elif row[0] == "2":
                row[1] = "21-40"
            elif row[0] == "3":
                row[1] = "41-60"
            elif row[0] == "4":
                row[1] = "61-80"
            elif row[0] == "5":
                row[1] = "81-100"
            elif row[0] == "6":
                row[1] = "101-120"
            elif row[0] == "7":
                row[1] = "121-140"
            elif row[0] == "8":
                row[1] = "141-250"
            else:
                row[1] = "251+"
            u_cursor.updateRow(row)

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

    if int(arcpy.GetCount_management(lakes_criteria).getOutput(0)) > 0:
        lakes = lakes_criteria
    else:
        lakes = lakes_final

    logger.info('Buffering {} Lake(s) using the '
                'following distances: {}...'.format(int(arcpy.GetCount_management(lakes).getOutput(0)), buffer_dist))

    buffer_lakes = os.path.join(working_gdb, 'Lakes_Buffer')
    buffer_dist = buffer_dist.replace(' ', '').replace(',', ';')
    fld_buff_dist = 'Buffer_Distance'
    fld_buff_area = 'Buffer_Area'
    fld_buff_prmtr = 'Buffer_Prmtr'

    arcpy.MultipleRingBuffer_analysis(lakes, buffer_lakes, buffer_dist, 'Meters', 'distance', 'NONE', 'OUTSIDE_ONLY')

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

    if bridges == '#':
        bridges = None

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

    logger.info('Cleaning up Watersheds...')
    watershed_lyr = arcpy.MakeFeatureLayer_management(watershed_poly, 'watershed_lyr')
    lake_lyr = arcpy.MakeFeatureLayer_management(lakes, 'lakes_lyr')
    arcpy.SelectLayerByLocation_management(watershed_lyr, 'INTERSECT', lake_lyr, '', 'NEW_SELECTION')
    arcpy.CopyFeatures_management(watershed_lyr, selected_watersheds)
    arcpy.Delete_management(watershed_lyr)
    arcpy.Delete_management(lake_lyr)

    logger.info('Clipping Buffer Zones to Watersheds...')
    arcpy.Clip_analysis(buffer_lakes, selected_watersheds, lakes_buffer_watershed)

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

    logger.info('Intersecting Buffer Zones with Roads...')
    arcpy.Intersect_analysis([roads, lakes_buffer_attributes], selected_roads, 'ALL')
    lst_buffer_dist = sorted({str(row[0]) for row in arcpy.da.SearchCursor(lakes_buffer_attributes,
                                                                           fld_buff_dist) if row[0]})

    for dist in lst_buffer_dist:
        dist_lyr = arcpy.MakeFeatureLayer_management(selected_roads, 'dist_lyr', fld_buff_dist + ' = ' + dist)
        selected_count = int(arcpy.GetCount_management(dist_lyr).getOutput(0))
        logger.info('There are {} road(s) within the {} metre buffer.'.format(selected_count, dist))
        arcpy.Delete_management(dist_lyr)

    arcpy.AddField_management(selected_roads, fld_road_length, 'Double')
    arcpy.CalculateField_management(selected_roads, fld_road_length, '!SHAPE.length@METERS!', 'PYTHON')

    if bridges:
        logger.info('Intersecting Buffer Zones with Coastal Bridges...')
        arcpy.Intersect_analysis([bridges, lakes_buffer_attributes], selected_bridges, 'ALL')
        for dist in lst_buffer_dist:
            dist_lyr = arcpy.MakeFeatureLayer_management(selected_bridges, 'dist_lyr', fld_buff_dist + ' = ' + dist)
            selected_count = int(arcpy.GetCount_management(dist_lyr).getOutput(0))
            logger.info('There are {} coastal bridges within the {} metre buffer.'.format(selected_count, dist))
            arcpy.Delete_management(dist_lyr)

    logger.info('Intersecting Buffer Zones with Streams...')
    arcpy.Intersect_analysis((lakes_buffer_attributes, streams), selected_streams, 'ALL')

    logger.info('********************************')
    logger.info('Completed Step 3 - Watershed Buffer Characteristics Process')
    logger.info('********************************')

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
