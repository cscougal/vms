# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 09:21:21 2017

@author: CS08
"""
import sys
sys.path.append(r"C:\Users\cs08\Documents\Projects\Python_Scripts")

import pyodbc
import pandas as pd
import time
import ogr
import osgeo.osr as osr

try:
    import archook #The module which locates arcgis
    archook.get_arcpy()
    import arcpy
except ImportError:
    import arcpy

arcpy.overwriteoutput = True
arcpy.env.overwriteOutput = True


def vmsConvertNew(input_db,outputDir,name,spe,output_type):
    
    
    '''This function takes quarterly VMS data(accessdb) and converts them 
        into a feature class within a geodatabase, it requires an input DB,
        output GDB and a name. New version can output to shapefile or csv.
        Function is no longer nesscary due to migration to POSTGRES'''

     
    arcpy.env.workspace = outputDir
    
    print ("Starting")   
    t0=time.time()
    
    spekg=spe+"Kg"
    spe= str(spekg[0:3]+"LiveKg")

    
    sql = '''SELECT Latitude,Longitude,SightingDate,
            Sighting_YY,Speed,IFISHgear,PingInterval,            
            '''+str(spekg)+''','''+str(spekg[0:3])+"LiveKg"+''',
            ECIntNo,Rectangle,inPort,inCoast,RSSNo,Enginepower,VessLength,
            Country_code,VOYAGE_ID,Activity_ID,Mesh_Size
            from VMSExtraction
            where Speed between 1 and 6 
            and '''+str(spekg)+''' >0 and inPort = 0'''
            
    #SQL statement determining what data to retrieve
    #alter parameters or add arguments for varying parameters

    driver_string =  'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='
      
    cnn=pyodbc.connect(driver_string + input_db[0:-6]+'Q1.mdb;Uid=;Pwd=;')
    cnn1=pyodbc.connect(driver_string + input_db[0:-6]+'Q2.mdb;Uid=;Pwd=;')
    cnn2=pyodbc.connect(driver_string + input_db[0:-6]+'Q3.mdb;Uid=;Pwd=;')
    cnn3=pyodbc.connect(driver_string + input_db[0:-6]+'Q4.mdb;Uid=;Pwd=;')
    
    q1 = pd.read_sql(sql, cnn)
    q2= pd.read_sql(sql, cnn1)
    q3= pd.read_sql(sql, cnn2)
    q4= pd.read_sql(sql, cnn3)
       
    data = pd.concat([q1,q2,q3,q4])
    
    #pandas dataframe for each quater mdb file - combined together
    
    print (len(data))
       
    data["SightingDate"] = data["SightingDate"].astype(str)
    data.reset_index(inplace=True)
                                                        
    field_names=["SightingDate","Year","Speed","GEAR","PING","Weight",
                 "Live_Weight","ECIntNo","Rectangle","inPort","inCoast",
                "Lat","Lon","RSSNo","Enginepower","VessLength","Country_code",
                "VOYAGE_ID","Activity_ID","Mesh_Size","DATE"]
    
    field_types=["TEXT","SHORT","SHORT","TEXT","DOUBLE","DOUBLE","DOUBLE",
                 "TEXT","TEXT","SHORT","SHORT","DOUBLE","DOUBLE","TEXT",
                 "DOUBLE","DOUBLE","TEXT","DOUBLE","DOUBLE","DOUBLE","DATE"]
         
    if output_type =="FeatureClass":        
        arcpy.CreateFeatureclass_management(outputDir,name,"POINT","#",
                                            "DISABLED","DISABLED",4326)   
    #arcgis feature class. creates empty fc then populates within cursor below          
        
        for i,j in zip(field_names,field_types):
              arcpy.AddField_management(name,i,j, "","","","",
                                        "NULLABLE","REQUIRED","")  
              #adds field names and types
        
           
        with arcpy.da.InsertCursor(name, ["SHAPE@XY"] + field_names) as cursor:
    
    
            for i in data.iterrows():
                cursor.insertRow([[float(i[1][2]),float(i[1][1])],str(i[1][3]),
                                   int(i[1][4]),int(i[1][5]),str(i[1][6]),
                                   float(i[1][7]),float(i[1][8]),
                                   float(i[1][9]),str(i[1][10]),str(i[1][11]),
                                   int(i[1][12]),int(i[1][13]),float(i[1][2]),
                                   float(i[1][1]),str(i[1][14]),
                                   float(i[1][15]),float(i[1][16]),
                                   str(i[1][17]),float(i[1][18]),
                                   float(i[1][19]),int(i[1][20]),
                                   str(i[1][3])])  
        
        #inserts values into fields within fc

            
    elif output_type =="Shapefile":
        #opensource shapefile version
    
        
    
        field_types = ["OFTString","OFTInteger","OFTInteger","OFTString",
                      "OFTReal","OFTReal","OFTReal","OFTString","OFTString",
                      "OFTInteger","OFTInteger","OFTReal","OFTReal",
                      "OFTString","OFTReal","OFTReal","OFTString","OFTReal",
                      "OFTReal","OFTReal","OFTDateTime"]        
        
        driver = ogr.GetDriverByName('ESRI Shapefile')
        data_source = driver.CreateDataSource(outputDir+"\\"+name+".shp")
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        layer = data_source.CreateLayer(outputDir+"\\"+name, srs, ogr.wkbPoint)
        
        for i,j in zip(field_names,field_types):
                field_name = ogr.FieldDefn(i, eval("ogr."+str(j)))
                field_name.SetWidth(24)
                field_name.SetPrecision(6)
                layer.CreateField(field_name)
                
        #similar to arc version, create empty shapefile add fields
                
        for row in data.iterrows():
            #itereates through df and inserts into shapefile
            
            feature = ogr.Feature(layer.GetLayerDefn())
            wkt = "POINT(%f %f)" %  (float(row[1][2]) , float(row[1][1]))
            #creates the geoemtry from lat lon coords
            
            feature.SetField("SightingDate"[0:10], row[1][3])
            feature.SetField("Year", row[1][4])
            feature.SetField("Speed", row[1][5])
            feature.SetField("GEAR", row[1][6]) 
            feature.SetField("PING", row[1][7])
            feature.SetField("Weight", row[1][8])
            feature.SetField("Live_Weight"[0:10], row[1][9])
            feature.SetField("ECIntNo", row[1][10]) 
            feature.SetField("Rectangle", row[1][11])
            feature.SetField("inPort", row[1][12])
            feature.SetField("inCoast", row[1][13])
            feature.SetField("Lon", float(row[1][2]))   
            feature.SetField("Lat", float(row[1][1]) )
            feature.SetField("RSSNo", row[1][14]) 
            feature.SetField("Enginepower"[0:10], row[1][15])
            feature.SetField("VessLength"[0:10], row[1][16])
            feature.SetField("Country_code"[0:10], row[1][17])   
            feature.SetField("VOYAGE_ID", row[1][18])  
            feature.SetField("Activity_ID"[0:10], row[1][19])
            feature.SetField("Mesh_Size", row[1][20])   
            feature.SetField("DATE", row[1][3])  
                        
            point = ogr.CreateGeometryFromWkt(wkt)
            feature.SetGeometry(point)
            layer.CreateFeature(feature)
            feature.Destroy()
        data_source.Destroy()  
    
    else:
        #simple save to csv version
        data.to_csv(outputDir+"\\"+name+".csv",index=False)
        
    t1=time.time()
    total=t1-t0
    print ("Finished in " +str(total)  + "seconds")
    
    
    
    
name ="sce_"
spe="sce"
input_db=r"C:\Users\cs08\vms_testing\sce_2017_q1.mdb"


vmsConvertNew(input_db,r"C:\Users\cs08\vms_testing_output\\",
              name+"2017",spe,output_type="Shapefile")
  


    
  
    
    
    
    
