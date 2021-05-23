import pandas as pd
import os

def rename_funct(dataframe):
    columns = list(dataframe.columns.values.tolist())
    new_columns = columns
    print("These are the column names:")
    print(columns)
    done = False
    while not done:
        new_columns = columns.copy()
        print("Please input the preferred column name. Press enter (or empty) to use the default column name.")
        ii = 0
        for c in columns:
            #print(columns[ii])
            val = input("New column name for '"+columns[ii]+"': ")
            #print (val)
            if val!="":
                new_columns[ii] = val
            ii = ii + 1
        print("These are the old column names:")
        print(columns)
        print("The new columns name will be:")
        print(new_columns)
        inq_1 = input("Is this the correct preferred column name? (Y/N): ")
        if(inq_1 == "Y" or inq_1 == "y"):
            dataframe.columns = new_columns
            done = True
        else:
            print("")

    return dataframe
    #os.system('cls' if os.name == 'nt' else 'clear')

