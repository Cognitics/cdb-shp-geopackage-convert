'''
Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the 
Software, and to permit persons to whom the Software is furnished to do so, subject 
to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION 
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import os
import sys
import dbfread

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

def removeFileIfExists(theFile):
    if(os.path.exists(theFile)):
        os.remove(theFile)
        if(os.path.exists(theFile)):
            print("Failed to remove " + theFile)


def removeShapeFile(shpFile):
    removeFileIfExists(shpFile)
    removeFileIfExists(shpFile[0:-3] + "dbf")
    removeFileIfExists(shpFile[0:-3] + "dbt")
    removeFileIfExists(shpFile[0:-3] + "shx")

def getFeatureClassSelector(fclassSelector):
    # If it's a polygon (T005)
        # T006 Polygon feature class attributes
        if(fclassSelector=='T005'):
            return 'T006'
        # If it's a point feature (T001)
        # T002 Point feature class attributes
        elif(fclassSelector=='T001'):
            return 'T002'
        # If it's a lineal (T003)
        # T004 Lineal feature class attributes
        elif(fclassSelector=='T003'):
            return 'T004'
        # If it's a Lineal Figure Point Feature (T007)
        # T008 Lineal Figure Point feature class attributes
        elif(fclassSelector=='T007'):
            return'T008'
        # If it's a Polygon Figure Point Feature (T009)
        # T010 Polygon figure point feature class attributes
        elif(fclassSelector=='T009'):
            return'T010'

def getExtendedAttributesSelector(fclassSelector):
    # If it's a polygon (T005)
        # T018 Polygon Feature Extended-level attributes
        if(fclassSelector=='T005'):
            return 'T018'
        # If it's a point feature (T001)
        # T016 Point Feature Extended-level attributes
        elif(fclassSelector=='T001'):
            return 'T016'
        # If it's a lineal (T003)
        # T017 Lineal Feature Extended-level attributes
        elif(fclassSelector=='T003'):
            return 'T017'
        # If it's a Lineal Figure Point Feature (T007)
        # T019 Lineal Figure Extended-level attributes
        elif(fclassSelector=='T007'):
            return'T019'
        # If it's a Polygon Figure Point Feature (T009)
        # T020 Polygon Figure Extended-level attributes
        elif(fclassSelector=='T009'):
            return'T020'


def getSelector2(shpFilename):
    base = os.path.basename(shpFilename)
    selector2 = base[18:22]
    return selector2


def getFeatureClassAttrFileName(shpFilename):
    #get the selector of the feature table
    featuresSelector2 = getSelector2(shpFilename)
    #get the corresponding feature class table
    fcAttrSelector = getFeatureClassSelector(featuresSelector2)
    if(fcAttrSelector==None):
        return None

    dbfFilename = shpFilename.replace(featuresSelector2,fcAttrSelector)
    dbfFilename = dbfFilename.replace('.shp','.dbf')
    return dbfFilename

def getExtendedAttrFileName(shpFilename):
    #get the selector of the feature table
    featuresSelector2 = getSelector2(shpFilename)
    #get the corresponding feature class table
    fcAttrSelector = getExtendedAttributesSelector(featuresSelector2)
    if(fcAttrSelector==None):
        return None
    dbfFilename = shpFilename.replace(featuresSelector2,fcAttrSelector)
    dbfFilename = dbfFilename.replace('.shp','.dbf')
    return dbfFilename

def getRelationshipAttrFileName(shpFilename):
    #get the selector of the feature table
    featuresSelector2 = getSelector2(shpFilename)
    if(featuresSelector2=="T003"):
        relAttrSelector = "T011"
    else:
        return None
    dbfFilename = shpFilename.replace(featuresSelector2,relAttrSelector)
    return dbfFilename


def getFeatureAttrTableName(shpFilename):
    tableName = os.path.basename(shpFilename)[0:-4]
    return tableName


def getOutputGeoPackageFilePath(shpFilename,cdbInputPath,cdbOutputPath):
    baseShapefileName = os.path.basename(shpFilename)
    inputDir = os.path.dirname(shpFilename)
    outputDir = inputDir.replace(cdbInputPath,cdbOutputPath)
    fulloutputFilePath = os.path.join(outputDir,baseShapefileName)
    fullGPKGOutputFilePath = fulloutputFilePath[0:-4] + '.gpkg'
    return fullGPKGOutputFilePath


#Return a dictionary of dictionaries 
#The top level dictionary maps CNAME values to a dictionary of key/value pairs representing column names -> values
def readDBF(dbfFilename):
    cNameRecords = {}

    rowNum = 1
    for record in dbfread.DBF(dbfFilename,load=True):
        recordFields = {}        
        
        for field in record.keys():
            recordFields[field] = record[field]
            #print(record)

        if('CNAM' in record):
            print(str(record['CNAM']))
            cNameRecords[record['CNAM']] = recordFields
        # The ID column is a special column in the DBF file, and the dbfreader doesn't
        # give this to us. Just in case it does in the future, we use it if it's there
        # otherwise, we track it ourselves.
        elif('ID' in record):
            cNameRecords[record['ID']] = recordFields
        else:
            cNameRecords[str(rowNum)] = recordFields
        rowNum = rowNum + 1
    return cNameRecords


def convertDBF(sqliteCon,dbfFilename,dbfTableName,tableDescription, addToGeoPackageContents=True):
    dbfTable = readDBF(dbfFilename)
    if(len(dbfTable)==0):
        return None
    convertedFields = []
    cursor = sqliteCon.cursor()
    cursor.execute("BEGIN TRANSACTION")
    dbfFields = dbfread.DBF(dbfFilename).fields
    createString = "CREATE TABLE '" + dbfTableName + "' ('ID' INTEGER PRIMARY KEY AUTOINCREMENT "
    firstField = True
    for fieldno in range(len(dbfFields)):
        # add column
        field = dbfFields[fieldno]
        convertedFields.append(field)
        createString += ','
        createString += "'" + field.name + "' "
        createFieldTypeString  = "TEXT"
        if(field.type=='F' or field.type=='O' or field.type=='N'):
            createFieldTypeString  = "REAL"
        elif(field.type == 'I'):
            createFieldTypeString  = "INTEGER"
        firstField = False
        createString += createFieldTypeString
    createString += ")"
    
    cursor.execute(createString)
    if(addToGeoPackageContents == True):
        contentsString = "insert into gpkg_contents (table_name,data_type,identifier,description,last_change) VALUES(?,'attributes',?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
        contentsAttrs = (dbfTableName,dbfTableName,dbfTableName + " " + tableDescription)
        cursor.execute(contentsString,contentsAttrs)

    for rowPK in dbfTable.keys():
        #print(record)
        insertValues = []
        insertValuesString = ""
        insertString = ""
        row = dbfTable[rowPK]
        for key,value in row.items():
            if(len(insertString)>0):
                insertString += ","
                insertValuesString += ","
            else:
                    insertString = "INSERT INTO " + dbfTableName + " ("
                    insertValuesString += " VALUES ("
            insertString += key
            insertValues.append(value)
            insertValuesString += "?"
        insertValuesString += ")"
        insertString += ") "
        insertString += insertValuesString
        #print(insertString)
        cursor.execute(insertString,tuple(insertValues))
    cursor.execute("COMMIT TRANSACTION")
    return convertedFields
