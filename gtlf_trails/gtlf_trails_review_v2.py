
##### GTLF - Trails #####
#
# Contractor: Alec B.
# Description: An ask came out to translate the publication queries and guidance doc queries into python. Probably a duplicated effort but its nice for the sprint team to have these so down the line
# team memebers can perform calculations with them. As it stands, the script just performs the publication queries and prints out some states by state. No further action was asked as of 02/21/2020.
# Currently pointing at the national pub.

# 03/04/2020 update: Adding in creation of GTLF - Trails feature subset. A blank geodatabase will be created and populated with two features:
#   1. 'Trails_Not_Assessed'
#   2. 'Trails Not Assessed for Public'
# Can use this to track changes over time since the data is easy to create and store since subset is small.

# Imports
import os
import time
import datetime
import arcpy
import sys
import pandas as pd
import re
# End Imports

# Global
########
# Edit Globals as user sees fit
date = str(datetime.date.today()) # Today's date
d = re.sub('-', '', date)
root = r'\\blm\dfs\loc\EGIS\ProjectsNational\NationalDataQuality\Sprint\analysis_tools\_sde_connections'

source = '_national_publication.sde' # sde
connector = 'ilmocpub.ILMOCDBO' # connector between dataset and feature
dataset = 'GTLF' # Dataset
feature = 'gtlf_ln' # feature class

fields = ['*'] # Field selector ; default is all

sde_connection = r'{}\{}\{}.{}\{}.{}'.format(root, source, connector, dataset, connector, feature)

########
# End Globals

### Functions ###
#
# Query Execute #
def execute_query(sde, f, sql_statement):
    
    print ('Executing: ' + sql_statement + '\n')
    global t
    t = []

    with arcpy.da.SearchCursor(sde, f, sql_statement) as cursor:
        for row in cursor:
            t.append(row)
    del row, cursor # Cleanup

#
#  Find Unqiue Vals Function   #
def unique_values(table , field):
    with arcpy.da.SearchCursor(table, [field]) as cursor:
        return sorted({row[0] for row in cursor})
#
# Dataframe #
def df_create(x):

    global df
    df = pd.DataFrame(x)
    df.columns = field_names
    
#
### End Functions ###

# Check sde_connection
######################

if arcpy.Exists(sde_connection):
    print ('Established connection')
else:
    print ('Failed to establish connection')
    sys.exit()

## Continue.....

###########################
#### Check Field Names ####
###########################

field_names = [f.name for f in arcpy.ListFields(sde_connection)] # Gets the unicode versions
field_names = [str(r) for r in field_names] # Gets the regular text/string versions
#print (field_names)
##########################

# GTLF - Trails - Queries From Guidance Document (Does not fully capture what is in database!)#
##################
# 
# New query (After 02/28/2020)
# Trails Not Assessed for Public
# PLAN_ROUTE_DSGNTN_AUTH='BLM'  PLAN_ASSET_CLASS = 'Not Assessed - Trail'  DSTRBTE_EXTRNL_CODE = 'Yes'
where11 = """PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_ROUTE_DSGNTN_AUTH = '{}'""".format('Not Assessed - Trail', 'YES', 'Yes', 'yes', 'BLM')

# Existing Queries to Expose Managed Trails with TMP designation #
# Trails Managed for Public Motorized Use
# PLAN_ROUTE_DSGNTN_AUTH='BLM'  PLAN_ASSET_CLASS = 'Transportation System - Trail'   DSTRBTE_EXTRNL_CODE = 'Yes'   PLAN_MODE_TRNSPRT = 'Motorized'   PLAN_OHV_ROUTE_DSGNTN = 'Open'
where10 = """PLAN_ROUTE_DSGNTN_AUTH = '{}' AND PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_MODE_TRNSPRT = '{}' AND PLAN_OHV_ROUTE_DSGNTN = '{}'""".format('BLM', 'Transportation System - Trail', 'YES', 'Motorized', 'Open')

# Trails Managed for Limited Public Motorized Use
# PLAN_ROUTE_DSGNTN_AUTH='BLM'  PLAN_ASSET_CLASS = 'Transportation System - Trail'   DSTRBTE_EXTRNL_CODE = 'Yes'   PLAN_MODE_TRNSPRT = 'Motorized'   PLAN_OHV_ROUTE_DSGNTN = 'Limited'
where9 = """PLAN_ROUTE_DSGNTN_AUTH = '{}' AND PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_MODE_TRNSPRT = '{}' AND PLAN_OHV_ROUTE_DSGNTN = '{}'""".format('BLM', 'Transportation System - Trail', 'YES', 'Motorized', 'Limited')

# Trails Managed for Public Non-Motorized Use
# PLAN_ROUTE_DSGNTN_AUTH='BLM'  PLAN_ASSET_CLASS = 'Transportation System - Trail'   DSTRBTE_EXTRNL_CODE = 'Yes'   PLAN_MODE_TRNSPRT = 'Non-Motorized'   PLAN_OHV_ROUTE_DSGNTN = 'Closed'
where8 = """PLAN_ROUTE_DSGNTN_AUTH = '{}' AND PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_MODE_TRNSPRT = '{}' AND PLAN_OHV_ROUTE_DSGNTN = '{}'""".format('BLM', 'Transportation System - Trail', 'YES', 'Non-Motorized', 'Closed')

# Trails Managed for Public Non-Mechanized Use
# PLAN_ROUTE_DSGNTN_AUTH='BLM'  PLAN_ASSET_CLASS = 'Transportation System - Trail'   DSTRBTE_EXTRNL_CODE = 'Yes'   PLAN_MODE_TRNSPRT = 'Non-Mechanized'  PLAN_OHV_ROUTE_DSGNTN = 'Closed'
where7 = """PLAN_ROUTE_DSGNTN_AUTH = '{}' AND PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_MODE_TRNSPRT = '{}' AND PLAN_OHV_ROUTE_DSGNTN = '{}'""".format('BLM', 'Transportation System - Trail', 'YES', 'Non-Mechanized', 'Closed')


###### GTLF - Trails - Queries From Guidance Document ######

#####################################
# GTLF Queries Used in Publication  #
#####################################
# 1. Roads Managed for Public Motorized Use
where1 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}' OR PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Road', 'Transportation System - Primitive Road', 'Road', 'Primitive Road', 'YES', 'Yes', 'yes', 'Motorized', 'Open', 'OPEN')

# 2. Roads Managed for Limited Public Motorized Use
where2 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}' OR PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Road', 'Transportation System - Primitive Road', 'Road', 'Primitive Road', 'YES', 'Yes', 'yes', 'Motorized', 'Limited')

# 3. Trails Managed for Public Motorized Use
where3 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}' OR PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Trail', 'YES', 'Yes', 'yes', 'Motorized', 'Open', 'OPEN')

# 4. Trails Managed for Limited Public Motorized Use
where4 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Trail', 'YES', 'Yes', 'yes', 'Motorized', 'Limited')

# 5. Trails Managed for Public Non-Motorized Use
where5 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Trail', 'YES', 'Yes', 'yes', 'Non-Motorized', 'Closed')

# 6. Trails Managed for Public Non-Mechanized Use
where6 = """(PLAN_ROUTE_DSGNTN_AUTH = '{}' OR PLAN_ROUTE_DSGNTN_AUTH = '{}') AND \
(PLAN_ASSET_CLASS = '{}') AND \
(DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}' OR DSTRBTE_EXTRNL_CODE = '{}') AND \
(PLAN_MODE_TRNSPRT = '{}') AND \
(PLAN_OHV_ROUTE_DSGNTN = '{}')""".format('BLM', 'BLM Road', 'Transportation System - Trail', 'YES', 'Yes', 'yes', 'Non-Mechanized', 'Closed')

#####################################
# Query Added in to help with Tracking:
where12 = """PLAN_ASSET_CLASS = '{}' AND PLAN_ROUTE_DSGNTN_AUTH = '{}'""".format('Not Assessed - Trail', 'BLM')
#

# Query dictionary to run ; Query definitions above
query_list = {
    'Trails Managed for Public Motorized Use': where3,
    'Trails Managed for Limited Public Motorized Use': where4,
    'Trails Managed for Public Non-Motorized Use': where5,
    'Trails Managed for Public Non-Mechanized Use': where6
    }
# Yes and no by itself
query_list3 = {
    'Trails Not Assessed (Yes & No)': where12
    }
#
##### We currently care about these two queries 03/11/2020:
query_list2 = {
    'Trails Not Assessed for Public': where11,
    'Trails Not Assessed (Yes & No)': where12
    }

# Stats to collect:
query_label = [] # Query name
record_count_by = [] # Total_count by query

master = pd.DataFrame(columns = field_names) # Master dataframe of all queries

for i in query_list2:
    # Stats to collect:
    query_label = [] # Query name
    record_count_by = [] # Total_count by query
    
    master = pd.DataFrame(columns = field_names) # Master dataframe of all queries
    execute_query(sde_connection, fields, query_list2[i])
    df_create(t)
    #
    master = pd.concat([master, df], ignore_index=True)
    #
    query_label.append(i)
    record_count_by.append(len(df.index))
    # Total records from queries
    master_total = len(master.index)

    print (date)
    print ('\n')

    print ('Queries used:')
    print (query_label)
    print ('\n')

    print ('Records by state:')
    print (master.groupby('ADMIN_ST')['BLM_MILES'].count())
    print ('Total: ' + str(master_total))
    print ('\n')

    print ('BLM Miles by state:')
    print (master.groupby('ADMIN_ST')['BLM_MILES'].sum())
    print ('Total: ' + str(master['BLM_MILES'].sum()))

#######################
#######################

#### Create Feature Class from National PUB
# Set overwrite option
arcpy.env.overwriteOutput = True
out_folder_path = r'\\blm\dfs\loc\EGIS\ProjectsNational\NationalDataQuality\Sprint\analysis_tools\GTLF_review\gtlf_trails\gtlf_trail_views'
out_name = 'gtlf-trails_%s.gdb' % d
gdb_root = out_folder_path + os.sep + out_name

try:
    if os.path.exists(gdb_root):
        print (out_name + ' exists! Passing...')
        sys.exit()
    else:
        arcpy.CreateFileGDB_management(out_folder_path, out_name)
        print ('Empty .gdb created')
except:
    print ('Failed to create .gdb, exiting...')
    sys.exit()

full_path1 = out_folder_path + os.sep + out_name + os.sep + 'NOCTrails_Not_Assessed' #Will be temp layer
full_path2 = out_folder_path + os.sep + out_name + os.sep + 'NOCTrails_Not_Assessed_forPublic' #Will be temp layer
#
#where11 = """PLAN_ASSET_CLASS = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND DSTRBTE_EXTRNL_CODE = '{}' AND PLAN_ROUTE_DSGNTN_AUTH = '{}'""".format('Not Assessed - Trail', 'YES', 'Yes', 'yes', 'BLM')
#where12 = """PLAN_ASSET_CLASS = '{}' AND PLAN_ROUTE_DSGNTN_AUTH = '{}'""".format('Not Assessed - Trail', 'BLM')

try:
    print ('Creating subset of gtlf as Trails_Not_Assessed.....')
    arcpy.MakeFeatureLayer_management(sde_connection, full_path1, where_clause = where12) # temp
    # Write the selected features to a new featureclass
    arcpy.CopyFeatures_management(full_path1, out_folder_path + os.sep + out_name + os.sep + 'Trails_Not_Assessed')
    print ('Feature created')
except:
    print ('Failed to create subset of gtlf as Trails_Not_Assessed')

try:
    print ('Creating subset of gtlf as Trails_Not_Assessed_forPublic.....')
    arcpy.MakeFeatureLayer_management(sde_connection, full_path2, where_clause = where11) # temp
    # Write the selected features to a new featureclass
    arcpy.CopyFeatures_management(full_path2, out_folder_path + os.sep + out_name + os.sep + 'Trails_Not_Assessed_forPublic')
    print ('Feature created')
except:
    print ('Failed to create subset of gtlf as Trails_Not_Assessed_forPublic')

print ('Script Completed')
# Wait for 20 seconds
time.sleep(20)

#################################################################
#################################################################
### Quick Summary Storage ###
'''
02/21/2020
Queries used:
['Trails Managed for Public Non-Motorized Use', 'Trails Managed for Public Non-Mechanized Use',
'Trails Managed for Limited Public Motorized Use', 'Trails Managed for Public Motorized Use']

Records by state:

ADMIN_ST
AK     635
CA     140
CO    6097
ES      13
ID     982
MT     171
WY      53
Name: BLM_MILES, dtype: int64
Total = 8091

BLM Miles by state:

ADMIN_ST
AK     578.668122
CA      57.729157
CO    2740.082286
ES       6.561067
ID     860.117362
MT      72.431713
WY      14.798000
Name: BLM_MILES, dtype: float64
Total = 4330.387707
#######################################################################
03/02/2020
Queries used:
['Trails Managed for Public Non-Motorized Use', 'Trails Managed for Public Non-Mechanized Use',
'Trails Managed for Limited Public Motorized Use', 'Trails Managed for Public Motorized Use']

Records by state:
ADMIN_ST
AK     635
AZ    2294
CA     718
CO    6654
ID     982
MT     171
NV     104
UT     343
WY      56
Name: BLM_MILES, dtype: int64
Total: 11965


BLM Miles by state:
ADMIN_ST
AK     578.668122
AZ     523.290078
CA     363.875804
CO    2994.536435
ID     860.117362
MT      72.431709
NV     128.342699
UT     123.975667
WY      13.612000
Name: BLM_MILES, dtype: float64
Total: 5658.84987591
####################################################

Executing: PLAN_ASSET_CLASS = 'Not Assessed - Trail' AND DSTRBTE_EXTRNL_CODE = 'YES' AND DSTRBTE_EXTRNL_CODE = 'Yes' AND DSTRBTE_EXTRNL_CODE = 'yes' AND PLAN_ROUTE_DSGNTN_AUTH = 'BLM'

2020-03-11


Queries used:
['Trails Not Assessed for Public']


Records by state:
ADMIN_ST
AK       68
AZ      398
CA      141
CO      112
MT     3094
NM      373
NV        5
OR    11364
Name: BLM_MILES, dtype: int64
Total: 15555


BLM Miles by state:
ADMIN_ST
AK      346.585982
AZ      136.815842
CA      232.759388
CO       26.812647
MT      651.348698
NM      475.067789
NV       20.284131
OR   -11364.000000
Name: BLM_MILES, dtype: float64
Total: -9474.32552159
Executing: PLAN_ASSET_CLASS = 'Not Assessed - Trail' AND PLAN_ROUTE_DSGNTN_AUTH = 'BLM'

2020-03-11


Queries used:
['Trails Not Assessed (Yes & No)']


Records by state:
ADMIN_ST
AK      288
AZ      398
CA      141
CO     4247
ES       66
MT     3110
NM      373
NV      181
OR    11364
Name: BLM_MILES, dtype: int64
Total: 15555


BLM Miles by state:
ADMIN_ST
AK      978.318818
AZ      136.815842
CA      232.759388
CO      421.599944
ES        7.110108
MT      651.418456
NM      475.067789
NV      233.814186
OR   -11364.000000
Name: BLM_MILES, dtype: float64
Total: -8227.09546828
'''
