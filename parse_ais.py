# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 09:34:21 2018

@author: CS08
"""

import glob
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import csv
import pynmea2


def parseAIS(input_dir,output_dir):
    
    
    ''' This function takes raw AIS csv's within a directory and parses them
    into usable data. Function removes incomplete nmea strings and returns 
    valid AIS records as a shapefile'''
       
    errors=[]
    csv_list = glob.glob(input_dir)
    #gets all csvs in dir
    
    for c in csv_list:
        try:
            f = open(c,'rU')
            reader = csv.reader(f)
            #allows us to bypass any encoding errors in underlying data
            
            csv_list = []
            for l in reader:
                csv_list.append(l)
            f.close()
            
            df = pd.DataFrame(csv_list)   
            
            df_filter = df[(df[0] =="$GPGGA") | (df[0] =="GPGGA")].loc[:,0:7]
            df_filter=df_filter[(df_filter != "").all(1)]
            #Filters accoridng to valid gps tags. And ensures no empty values
            
            df_filter = df_filter[df_filter[3].isin(["N","S"])]
            df_filter = df_filter[df_filter[5].isin(["E","W"])]
            #Only keeps values with valid direction/compass
                           
            gps = df_filter.astype(str).apply(','.join, axis=1).tolist()
            #this combines seperate columns into one string
            #This is important as it allow us to use pynmea package to parse
            
            lon=[]
            lat=[]
            time_stamp=[]
            
            for i in gps:
                
                msg = pynmea2.parse(i)
                
                try:
                    lon.append( msg.longitude)
                    
                except AttributeError:
                    lon.append(-9999)
                    
                try:
                    lat.append(msg.latitude)
                    
                except AttributeError:
                    lat.append(-9999) 
                    
                try:      
                    time_stamp.append(msg.timestamp.strftime('%H:%M:%S'))
                    
                except AttributeError:
                    time_stamp.append("none")
                    
            #follwoing code converts nmea string into valid coordinates with
            #time stamp info. If any element is missing returns -9999/"none"
                    
            df = pd.DataFrame({"lon":lon,"lat":lat,"time_stamp":time_stamp})             
            df = df[df["lon"] != -9999]
            df = df[df["lat"] != -9999]
            df["my_idx"]= df.index
            
            #Create df with values, removes any entries with invalid lat\lon  
                
            geom = [Point(xy) for xy in zip(df["lon"],df["lat"])]
            #Creates a valid geometry for each gps point
            
            crs = {'init': 'epsg:4326'}
            
            gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geom)   
            #this creates a valid Geodataframe with crs in wgs84
            save_name = os.path.basename(c)[0:8]+".shp"
            
            gdf.to_file(output_dir + "points_" + save_name)  
            #saves as shapefile to output dir           
            print ("processed "+c + " succesfully")
                                  
            
        except Exception as e:
            print(c+ " has not been processed correctly")
            errors.append(c)
            #catchs any errors from input csvs
    
    print("Completed Processing")


#input parameters
input_dir=r"E:\*.csv"
output_dir = r"C:\Users\cs08\Documents\Projects\Random_old\endeavour_tracks\points\\"

#valid fucntion example
parseAIS(input_dir,output_dir)




