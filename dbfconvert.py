
import dbfread

#Return a dictionary of dictionaries 
#The top level dictionary maps CNAME values to a dictionary of key/value pairs representing column names -> values
def readDBF(dbfFilename):
    cNameRecords = {}
    dbfields = None
    try:
        dbfFields = dbfread.DBF(dbfFilename).fields
        for record in dbfread.DBF(dbfFilename,load=True):
            recordFields = {}

            for field in record.keys():
                recordFields[field] = record[field]
                #print(record)

            cNameRecords[record['CNAM']] = recordFields
    except dbfread.exceptions.DBFNotFound:
        return None
    return cNameRecords


