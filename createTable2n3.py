import pandas as pd

file1 = pd.read_csv("warehouse.csv")
file2 = pd.read_csv("district.csv")
file3 = pd.read_csv("customer.csv")
file4 = pd.merge(pd.merge(file1, file2, left_on=['W_ID'], right_on=['D_W_ID']),file3, right_on=['C_W_ID','C_D_ID'], left_on=['W_ID','D_ID'])
file4.drop(['D_W_ID','C_W_ID','C_D_ID'], axis=1, inplace=True)
file4.rename(columns={'C_FIRST':'C_FIRST_NAME','C_MIDDLE':'C_MIDDLE_NAME','C_LAST':'C_LAST_NAME'}, inplace=True)

file5 = file4.drop(['W_YTD','D_YTD','D_NEXT_O_ID'],axis=1)
file5.drop_duplicates()
file5.to_csv("table2.csv", index= False)

file6 = file4.drop_duplicates(subset=['W_ID','D_ID','D_NEXT_O_ID','W_YTD','D_YTD'])
file6.to_csv("table3.csv", index = False, columns = ['W_ID','D_ID','D_NEXT_O_ID','W_YTD','D_YTD'])

