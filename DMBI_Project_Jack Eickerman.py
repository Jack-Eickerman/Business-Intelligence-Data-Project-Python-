#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  9 14:41:17 2025

@author: jackeickerman
"""


#Tool Boxes

import pandas as pd
import pymysql


#------------------------------------------------------------------------------


#Establishing a Dataframe

csv_reading_copy = pd.read_csv("Landfills_in_America.csv")
csv_reading_copy = csv_reading_copy.copy()

#------------------------------------------------------------------------------

#Cleansing the data that is unnecessary for the project

csv_reading_copy = csv_reading_copy.drop_duplicates("Landfill Name")
csv_reading_copy = csv_reading_copy.drop("RNG Delivery Method", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("LFG Collection System In Place?", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("Project Name", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("LFG Use Details", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("End User(s)", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("Project Developer(s)", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("Project Owner(s)", axis = "columns")
csv_reading_copy = csv_reading_copy.drop("Product Supplier(s)", axis = "columns")


#------------------------------------------------------------------------------

#Taking the average of the numerical columns for processing

mean1 = csv_reading_copy["LFG Collected (mmscfd)"].mean()
mean2 = csv_reading_copy["LFG Flared (mmscfd)"].mean()
mean3 = csv_reading_copy["Actual MW Generation"].mean()
mean4 = csv_reading_copy["Rated MW Capacity"].mean()
mean5 = csv_reading_copy["LFG Flow to Project (mmscfd)"].mean()
mean6 = csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Direct"].mean()
mean7 = csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Avoided"].mean()


#------------------------------------------------------------------------------

#Replacing the cells of numeric columns that have 0 or NAN with the respective averages

LFGCOLLECTED = csv_reading_copy["LFG Collected (mmscfd)"].fillna(mean1)
csv_reading_copy["LFG Collected (mmscfd)"] = LFGCOLLECTED

LFGFLARED = csv_reading_copy["LFG Flared (mmscfd)"].fillna(mean2)
csv_reading_copy["LFG Flared (mmscfd)"] = LFGFLARED

ACTUALMWGENERATION = csv_reading_copy["Actual MW Generation"].fillna(mean3)
csv_reading_copy["Actual MW Generation"] = ACTUALMWGENERATION

RATEDMWCAPACITY = csv_reading_copy["Rated MW Capacity"].fillna(mean4)
csv_reading_copy["Rated MW Capacity"] = RATEDMWCAPACITY

LFGFLOW = csv_reading_copy["LFG Flow to Project (mmscfd)"].fillna(mean5)
csv_reading_copy["LFG Flow to Project (mmscfd)"] = LFGFLOW

CYERD = csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Direct"].replace(0,mean6)
csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Direct"] = CYERD

CYERA = csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Avoided"].replace(0,mean7)
csv_reading_copy["Current Year Emission Reductions (MMTCO2e/yr) - Avoided"] = CYERA

LFGFLARED2 = csv_reading_copy["LFG Flared (mmscfd)"].replace(0,mean2)
csv_reading_copy["LFG Flared (mmscfd)"] = LFGFLARED2


#------------------------------------------------------------------------------

#Stripping the spaces and other notations that might effect the transfer process to MySQL

LANDFILL_OWNER_ORGANIZATION_COMMA_DESTROYER = csv_reading_copy["Landfill Owner Organization(s)"].str.replace(',','')
csv_reading_copy["Landfill Owner Organization(s)"] = LANDFILL_OWNER_ORGANIZATION_COMMA_DESTROYER
LOO_UNDERSCORE = csv_reading_copy["Landfill Owner Organization(s)"].str.replace(' ','_')
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_UNDERSCORE
LOO_APOSTROPHE = csv_reading_copy["Landfill Owner Organization(s)"].str.replace("'","")
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_APOSTROPHE
LOO_PERIOD = csv_reading_copy["Landfill Owner Organization(s)"].str.replace('.','')
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_PERIOD
LOO_SEMICOLON = csv_reading_copy["Landfill Owner Organization(s)"].str.replace(';','')
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_SEMICOLON
LOO_PARENTHESES1 = csv_reading_copy["Landfill Owner Organization(s)"].str.replace('(','')
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_PARENTHESES1
LOO_PARENTHESES2 = csv_reading_copy["Landfill Owner Organization(s)"].str.replace(')','')
csv_reading_copy["Landfill Owner Organization(s)"] = LOO_PARENTHESES2

LANDFILL_NAME_HYPHEN_DESTROYER = csv_reading_copy["Landfill Name"].str.replace('-','_')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_HYPHEN_DESTROYER
LANDFILL_NAME_UNDERSCORE = csv_reading_copy["Landfill Name"].str.replace(' ','_')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_UNDERSCORE
LANDFILL_NAME_PERIOD = csv_reading_copy["Landfill Name"].str.replace('.','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_PERIOD
LANDFILL_NAME_SPACER = csv_reading_copy["Landfill Name"].str.replace(' ','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_SPACER
LANDFILL_NAME_COMMA_DESTROYER =  csv_reading_copy["Landfill Name"].str.replace(',','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_COMMA_DESTROYER
LANDFILL_NAME_HASH = csv_reading_copy["Landfill Name"].str.replace('#','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_HASH
LANDFILL_NAME_PARENTHESES1 = csv_reading_copy["Landfill Name"].str.replace('(','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_PARENTHESES1
LANDFILL_NAME_PARENTHESES2 = csv_reading_copy["Landfill Name"].str.replace(')','')
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_PARENTHESES2
LANDFILL_NAME_APOSTROPHE = csv_reading_copy["Landfill Name"].str.replace("'","")
csv_reading_copy["Landfill Name"] = LANDFILL_NAME_APOSTROPHE

LFG_ENERGY_PT_UNDERSCORE = csv_reading_copy["LFG Energy Project Type"].str.replace(' ','_')
csv_reading_copy["LFG Energy Project Type"] = LFG_ENERGY_PT_UNDERSCORE

CITY_SPACER = csv_reading_copy["City"].str.replace(' ','_')
csv_reading_copy["City"] = CITY_SPACER
CITY_APOSTROPHE = csv_reading_copy["City"].str.replace("'","")
csv_reading_copy["City"] = CITY_APOSTROPHE

COUNTY_SPACER = csv_reading_copy["County"].str.replace(' ','_')
csv_reading_copy["County"] = COUNTY_SPACER
COUNTY_APOSTROPHE = csv_reading_copy["County"].str.replace("'","")
csv_reading_copy["County"] = COUNTY_APOSTROPHE

PROJECT_TYPE_SPACER = csv_reading_copy["Project Type Category"].str.replace(' ','_')
csv_reading_copy["Project Type Category"] = PROJECT_TYPE_SPACER


#------------------------------------------------------------------------------

#Final cleansing of any NAN's in the dataframe and the reset of the index

csv_reading_copy = csv_reading_copy.dropna()
csv_reading_copy = csv_reading_copy.reset_index()
csv_reading_copy = csv_reading_copy.drop("index", axis = "columns")


#------------------------------------------------------------------------------

#Most important part is to "string" the non-numeric columns with quotations

csv_reading_copy.update('"' + csv_reading_copy[["Self-Developed", "LFG Energy Project Type", "Project Finish Date", "Project Start Date", "Current Project Status", "Project ID", "Ownership Type", "Project Type Category", "State", "County", "City", "Landfill Name", "Landfill Owner Organization(s)"]].astype(str) + '"')


#------------------------------------------------------------------------------

#Establishing the connection to MySQL

LetsConnect = pymysql.connect(database = 'DMBI_Project', user = 'root', password = 'Jack2day!')
cursor = LetsConnect.cursor()


#------------------------------------------------------------------------------

#Code for collecting the data in the dataframe and storing it into one variable

Insert_query = "INSERT INTO Landfills_in_America VALUES "


for x in range(csv_reading_copy.shape[0]):
    Insert_query += '('
    
    for y in range(csv_reading_copy.shape[1]):
        Insert_query += str(csv_reading_copy[csv_reading_copy.columns.values[y]][x]) + ', '
    
    Insert_query = Insert_query[:-2] + '), '

Insert_query = Insert_query[:-2] + ';'


#------------------------------------------------------------------------------

#Executing the code and transfering data to MySQL software

cursor.execute(Insert_query)
LetsConnect.commit()
LetsConnect.close()


#------------------------------------------------------------------------------

#If code has worked properly, this message will display

print()
print()
print("-------------------  Connection Complete  -------------------")
print()








