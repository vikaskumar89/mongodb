import pandas as pd
import csv
import json
import sys, getopt, pprint

######## merge csv files for table 1

file1 = pd.read_csv("order.csv")
file2 = pd.read_csv("order-line.csv")
file3 = pd.read_csv("customer.csv", usecols = ['C_W_ID','C_D_ID','C_ID','C_FIRST','C_MIDDLE','C_LAST'])
file4 = pd.read_csv("item.csv")
file5 = pd.merge(pd.merge(pd.merge(file1, file2, left_on=['O_W_ID','O_D_ID','O_ID'], right_on=['OL_W_ID','OL_D_ID','OL_O_ID']),file3,
left_on=['O_W_ID','O_D_ID','O_C_ID'],right_on=['C_W_ID','C_D_ID','C_ID'],how='outer'),file4,left_on=['OL_I_ID'],right_on=['I_ID'],how='outer')
file5.drop(['OL_W_ID','OL_D_ID','OL_O_ID','O_C_ID','C_W_ID','C_D_ID','I_ID','I_IM_ID','I_DATA'], axis=1, inplace=True)
file5.rename(columns={'O_W_ID':'W_ID','O_D_ID':'D_ID','I_NAME':'OL_I_NAME','I_PRICE':'OL_I_PRICE','C_FIRST':'C_FIRST_NAME','C_MIDDLE':'C_MIDDLE_NAME','C_LAST':'C_LAST_NAME'}, inplace=True)
file5.rename(columns={'OL_NUMBER':'ORDERLINE.OL_NUMBER','O_ALL_LOCAL':'ORDERLINE.O_ALL_LOCAL','OL_I_ID':'ORDERLINE.OL_I_ID','OL_SUPPLY_W_ID':'ORDERLINE.OL_SUPPLY_W_ID','OL_QUANTITY':'ORDERLINE.OL_QUANTITY','OL_I_NAME':'ORDERLINE.OL_I_NAME','OL_DIST_INFO':'ORDERLINE.OL_DIST_INFO','OL_AMOUNT':'ORDERLINE.OL_AMOUNT','OL_I_PRICE':'ORDERLINE.OL_I_PRICE','OL_DELIVERY_D':'ORDERLINE.OL_DELIVERY_D'},inplace=True)
file5.drop_duplicates()
file5.to_csv("table1.csv", index= False)

