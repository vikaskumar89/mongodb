import pandas as pd

file1 = pd.read_csv("item.csv")
file2 = pd.read_csv("stock.csv")
file3 = pd.merge(file1, file2, left_on=['I_ID'], right_on=['S_I_ID'])
file3.drop(['S_I_ID'], axis=1, inplace=True)
file3.rename(columns={'S_W_ID':'W_ID'}, inplace=True)
file3.drop_duplicates()
file3.to_csv("table4.csv", index= False)
