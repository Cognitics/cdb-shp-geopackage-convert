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

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
from dbfread import DBF

def generateMetaFiles(cDBRoot):
    shapeFiles = []
    shapeExtents = {}
    print("Generating metadata files for " + cDBRoot)
    fileCount = 0
    totalSize = 0

    gdal.UseExceptions()
    #create python file for the dictionary

    # shapeFiles = ['xxx.shp','yyy.shp']
    metaPath = os.path.join(cDBRoot,'shapemeta.py')
    shapeMetaData = open(metaPath,'w')
    shapeMetaData.write('shapeMetaData = {\n')

    pyFile = open(os.path.join(cDBRoot, 'shapeindex.py'),'w')
    pyFile.write('shapeFiles = [')
    first = True
    for root, dirs, files in os.walk(cDBRoot):
        path = root.split(os.sep)
        hasShapeFile = False
        for file in files:
            base,ext = os.path.splitext(file)
            filePath = os.path.join(root,file)
            if((ext==".shp") or 
            (ext==".dbf") or
            (ext==".dbt") or
            (ext==".shx")):
                fileCount += 1
                fileSize = os.path.getsize(filePath)
                totalSize += fileSize
                if(ext==".shp"):
                    if(first != True):
                        pyFile.write(",")
                    first = False
                    # Add this file to the list if it's a shape file
                    pyFile.write("\n\tr'" + filePath + "'")
                    shapeFile = filePath
                    shapeFiles.append(filePath)
                    if(fileSize==0):
                        continue
                    try:
                        dataSource = ogr.Open(shapeFile)
                        if(dataSource == None):
                            print("Unable to open " + shapeFile)
                            continue
                        layer = dataSource.GetLayer(0)
                        if(layer == None):
                            print("Unable to read layer from " + shapeFile)
                            continue
                        #envelope = ogr.
                        west,east,south,north = layer.GetExtent(True)
                        extents = {}
                        extents['north'] = north
                        extents['south'] = south
                        extents['east'] = east
                        extents['west'] = west
                        shapeExtents[shapeFile] = extents
                        if(west != 0):
                            shapeMetaData.write("r'{}': ".format(shapeFile))
                            shapeMetaData.write("[{},{},{},{}],\n".format(north,south,east,west))
                    except Exception:
                        pass

    pyFile.write(']\n')
    shapeMetaData.write("'' : [0,0,0,0]\n")
    shapeMetaData.write('}')
    shapeMetaData.close()
    print("Total File Count:" + str(fileCount))
    print("Total File Size:" + str(totalSize))
    return shapeFiles
