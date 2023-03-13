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
import subprocess

import dbfconvert
import converter

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

import sqlite3

version_num = int(gdal.VersionInfo('VERSION_NUM'))
print("GDAL Version " + str(version_num))

#import debugpy
#debugpy.listen(("0.0.0.0", 5678))
#print("Waiting for client to attach...")
#debugpy.wait_for_client()

if version_num < 2020300:
    sys.exit('ERROR: Python bindings of GDAL 2.2.3 or later required due to GeoPackage performance issues.')

def cleanPath(path):
    cleanPath = path.replace("\\",'/')
    return cleanPath

def getOutputLayerName(shpFilename):
    filenameOnly = os.path.basename(shpFilename[0:-4])
    return filenameOnly

def getFilenameComponents(shpFilename):
    components = {}
    filenameOnly = os.path.basename(shpFilename)
    baseName,ext = os.path.splitext(filenameOnly)
    filenameParts = baseName.split("_")
    datasetCode = filenameParts[1]
    components['datasetcode'] = datasetCode
    componentSelector1 = filenameParts[2]
    components['selector1'] = componentSelector1
    componentSelector2 = filenameParts[3]
    components['selector2'] = componentSelector2
    lod = filenameParts[4]
    components['lod'] = lod
    uref = filenameParts[5]
    components['uref'] = uref
    rref = filenameParts[6]
    components['rref'] = rref

    return components


def copyFeaturesFromShapeToGeoPackage(shpFilename, gpkgFilename, flattenFCAttrs, removeConverted):
    hasCNAM = False
    dbfFCFilename = None
    dbfEAFilename = None

    if(flattenFCAttrs):
        dbfFCFilename = converter.getFeatureClassAttrFileName(shpFilename)
        dbfEAFilename = converter.getExtendedAttrFileName(shpFilename)
        if(shpFilename == dbfFCFilename or shpFilename == dbfEAFilename):
            return None

    convertedFields = []
    fClassRecords = {}
    layerComponents = getFilenameComponents(shpFilename)
    if(dbfFCFilename != None and os.path.isfile(dbfFCFilename)):
        fClassRecords = dbfconvert.readDBF(dbfFCFilename)

    dataSource = ogr.Open(shpFilename)
    if(dataSource==None):
        print("Unable to open " + shpFilename)
        return 0
    layer = dataSource.GetLayer(0)
    if(layer == None):
        print("Unable to read layer from " + shpFilename)
        return 0
    layerDefinition = layer.GetLayerDefn()
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    outLayerName = getOutputLayerName(shpFilename)

    ogrDriver = ogr.GetDriverByName("GPKG")
    gpkgFile = ogrDriver.CreateDataSource(gpkgFilename)

    if(gpkgFile == None):
        print("Unable to create " + gpkgFilename)
        return
    gpkgFile.StartTransaction()
    outLayer = gpkgFile.GetLayerByName(outLayerName)
    fieldIdx = 0
    fieldIndexes = {}
    if(outLayer!=None):
        outputLayerDefinition = outLayer.GetLayerDefn()
        #track field indexes for existing layers? 
        for i in range(outputLayerDefinition.GetFieldCount()):
            fieldName =  outputLayerDefinition.GetFieldDefn(i).GetName()
            convertedFields.append(fieldName)
            fieldIndexes[fieldName] = fieldIdx
            fieldIdx += 1
    else:
        outLayer = gpkgFile.CreateLayer(outLayerName,srs,geom_type=layerDefinition.GetGeomType(),options=["FID=id"])
        
        # Add fields
        for i in range(layerDefinition.GetFieldCount()):
            fieldName =  layerDefinition.GetFieldDefn(i).GetName()
            if(fieldName=='CNAM'):
                hasCNAM = True
            fieldTypeCode = layerDefinition.GetFieldDefn(i).GetType()
            fieldType = layerDefinition.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
            fieldWidth = layerDefinition.GetFieldDefn(i).GetWidth()
            GetPrecision = layerDefinition.GetFieldDefn(i).GetPrecision()
            fieldDef = ogr.FieldDefn(fieldName,fieldTypeCode)
            outLayer.CreateField(fieldDef)
            convertedFields.append(fieldName)
            fieldIndexes[fieldName] = fieldIdx
            fieldIdx += 1

        #Create fields for featureClass Attributes
        for recordCNAM, row in fClassRecords.items():
            for fieldName,fieldValue in row.items():
                if(fieldName in convertedFields):
                    continue
                fieldTypeCode = ogr.OFTString
                if(isinstance(fieldValue,float)):
                    fieldTypeCode = ogr.OFSTFloat32
                if(isinstance(fieldValue,int)):
                    fieldTypeCode = ogr.OFTInteger
                if(isinstance(fieldValue,bool)):
                    fieldTypeCode = ogr.OFSTBoolean
                fieldDef = ogr.FieldDefn(fieldName,fieldTypeCode)

                outLayer.CreateField(fieldDef)
                convertedFields.append(fieldName)
                fieldIndexes[fieldName] = fieldIdx
                fieldIdx += 1
            #read one record to get the field name/types
            break

    layerDefinition = outLayer.GetLayerDefn()
    layer.ResetReading()
    featureCount = 0
    inFeature = layer.GetNextFeature()
    #copy the features
    while inFeature is not None:
        featureCount += 1
        outFeature = ogr.Feature(layerDefinition)
        #Copy the geometry and attributes 
        outFeature.SetFrom(inFeature)

        if(hasCNAM):
            cnamValue = inFeature.GetField('CNAM')
            fclassRecord = fClassRecords[cnamValue]

            #flatten attributes from the feature class attributes table
            if(cnamValue in fClassRecords.keys()):
                fclassFields = fClassRecords[cnamValue]
                for field in fclassFields.keys():
                    outFeature.SetField(fieldIndexes[field],fclassFields[field])

        #write the feature
        outLayer.CreateFeature(outFeature)
        outFeature = None
        inFeature = layer.GetNextFeature()
    gpkgFile.CommitTransaction()
    if(removeConverted):
        dataSource = None
        converter.removeShapeFile(shpFilename)
        #even though the FC file is a dbf, some tools make an empty shapefile
        #so we'll try to delete that one too.
        if(flattenFCAttrs):
            converter.removeShapeFile(dbfFCFilename[0:-3] + "shp")
    return featureCount


def getOutputGeoPackageFilePath(shpFilename,cdbInputPath,cdbOutputPath):
    baseShapefileName = os.path.basename(shpFilename)
    inputDir = os.path.dirname(shpFilename)
    outputDir = inputDir.replace(cdbInputPath,cdbOutputPath)
    fulloutputFilePath = os.path.join(outputDir,baseShapefileName)
    fullGPKGOutputFilePath = fulloutputFilePath[0:-4] + '.gpkg'
    return fullGPKGOutputFilePath

#create the extended attributes table
def createExtendedAttributesTable(sqliteCon,shpFilename, removeConverted):
    #create the table
    if(sqliteCon == None):
        print("Unable to access database when creating extended attributes table")
        return None
    extendedAttributesDBFFilename = converter.getExtendedAttrFileName(shpFilename)

    dbfTableName = getExtendedAttrTableName(shpFilename)
    if(os.path.exists(extendedAttributesDBFFilename)):
        if(os.path.getsize(extendedAttributesDBFFilename)!=0):
            converter.convertDBF(sqliteCon,extendedAttributesDBFFilename, dbfTableName,'Extended Attributes')
        if(removeConverted):
            converter.removeShapeFile(extendedAttributesDBFFilename[0:-3] + "shp")
    return None

def getExtendedAttrTableName(shpFilename):
    extendedAttributesDBFFilename = converter.getExtendedAttrFileName(shpFilename)
    shpBaseFilename = os.path.basename(extendedAttributesDBFFilename)
    dbfTableName = shpBaseFilename[0:-4]
    return dbfTableName

#convert a relationship dbf into a GeoPackage, as a table in an existing file.
def convertRelationshipAttrShapeFile(sqliteCon,relAttrFileName, cdbInputDir, cdbOutputDir, removeConverted):    

    if(os.path.exists(relAttrFileName) == False):
        return None
    if(os.path.getsize(relAttrFileName)==0):
        if(removeConverted):
            converter.removeShapeFile(relAttrFileName[0:-3] + "shp")
        return None
    
    relAttrTableName = converter.getFeatureAttrTableName(relAttrFileName)

    converter.convertDBF(sqliteCon,relAttrFileName,
        relAttrTableName, 'Relationship Attributes', False)

    if(removeConverted):
        converter.removeShapeFile(relAttrFileName[0:-3] + "shp")
    return None

#convert a shapefile into a GeoPackage file using GDAL.
def convertShapeFile(shpFilename, cdbInputDir, cdbOutputDir, removeConverted):    
    
    #see if it's a relationship file
    selector2 = converter.getSelector2(shpFilename)
    if(selector2=="T011"):
        outputGeoPackageFile = getOutputGeoPackageFilePath(shpFilename,cdbInputDir, cdbOutputDir)
        sqliteCon = sqlite3.connect(outputGeoPackageFile)
        relFileName = shpFilename
        if(relFileName):
            convertRelationshipAttrShapeFile(sqliteCon,relFileName[0:-3] + "dbf", cdbInputDir, cdbOutputDir, removeConverted)
        sqliteCon.close()
        return None
    
    #make sure it's a real feature file if it's not a relationship file.
    fcAttrName = converter.getFeatureClassAttrFileName(shpFilename)    
    if(fcAttrName==None):
        return None

    #Create the features table, adding the feature class columns
    outputGeoPackageFile = getOutputGeoPackageFilePath(shpFilename,cdbInputDir, cdbOutputDir)
    # Make whatever directories we need for the output file.
    parentDirectory = os.path.dirname(cleanPath(outputGeoPackageFile))
    if not os.path.exists(parentDirectory):
        os.makedirs(parentDirectory)

    featureTableName = converter.getFeatureAttrTableName(shpFilename)
    copyFeaturesFromShapeToGeoPackage(shpFilename,outputGeoPackageFile, True, removeConverted)
    sqliteCon = sqlite3.connect(outputGeoPackageFile)
    createExtendedAttributesTable(sqliteCon,shpFilename, removeConverted)    
    sqliteCon.close()

        
    return

def translateCDB(cdbInputDir, cdbOutputDir, removeConverted):
    sys.path.append(cdbInputDir)
    import generateMetaFiles
    shapeFiles = generateMetaFiles.generateMetaFiles(cDBRoot)

    for shapefile in shapeFiles:
        convertShapeFile(shapefile,cdbInputDir,cdbOutputDir, removeConverted)

def printUsage():
    print("Usage: Convert.py [--REMOVE_SHP] <Input Root CDB Directory> <Output Directory for GeoPackage Files>")
    print("Note: Only the GeoPackage files will be placed in the output directory.")
    print("      The input and output directories can be the same. If so, it is highly")
    print("      recommended that you make a copy of the CDB first, especially")
    print("      if you use the --REMOVE_SHP, which will remove all shapefiles and")
    print("      the associated files such as shx, dbf, etc.")
    print("")
    print("      Only the GeoPackage files will be placed in the output directory, no other files will be copied.")


if(len(sys.argv) < 3):
    printUsage()
    exit()
removeConverted = False
nextArg = 1
if(sys.argv[1]=="--REMOVE_SHP"):
    if(len(sys.argv) != 4):
        printUsage()
        exit()

    removeConverted = True
    nextArg = nextArg + 1
cDBRoot = sys.argv[nextArg]
nextArg = nextArg + 1
outputDirectory = sys.argv[nextArg]

if(removeConverted and (cDBRoot != outputDirectory)):
    print("Error: To use --REMOVE_SHP, the input and output directories must be the same")
    exit()

translateCDB(cDBRoot,outputDirectory,removeConverted)

