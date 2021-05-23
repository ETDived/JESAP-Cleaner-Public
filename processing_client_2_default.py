import pandas as pd
import os
import numpy as np
import column_rename


#
def read_mapping():
    mapping_df = pd.read_csv('mapping_je_type.csv')

    return mapping_df


def define_final_amount(row):
    if row['D/C'] == 'H':
        return (row['Loc.curr.amount'] * -1)
    else:
        return row['Loc.curr.amount']


def define_doc_amount(row):
    if row['D/C'] == 'H':
        return (row['Amount'] * -1)
    else:
        return row['Amount']


def define_je_type(row, mapping_df):
    return mapping_df.loc[mapping_df['TCODE'] == row['TCode'], 'JE Type'].item()


def manipulate_data(company_code, input_path, fy):
    print('Adding column')
    working_path = input_path + company_code + '/'
    removed_dupe_path = input_path + 'removed_dupe'
    files = sorted(os.listdir(removed_dupe_path))
    df = [''] * len(files)
    ii = 0
    sum_df = 0
    attempt = 0
    for infiles in files:
        print("Attempting reading temp files " + infiles, end=" ")
        file_path = os.path.join(removed_dupe_path, infiles)
        try:
            df[ii] = pd.read_csv(file_path,
                                 delimiter='|',
                                 low_memory=False,
                                 encoding='ISO-8859-15',
                                 dtype={'DocumentNo': 'string', 'Year': 'string', 'Period': 'string', 'Itm': 'string',
                                        'PK': 'string'})  # ,quoting=csv.QUOTE_NONE

            # print(df[ii].tail(10))
            if (ii == 0):
                sum_df = df[ii]
            else:
                temp_sum_df = sum_df
                sum_df = temp_sum_df.append(df[ii], ignore_index=True)
            # print(df[ii].tail())
            ii = ii + 1
            print("- Done reading")
        except Exception as error_reading:
            print(error_reading)
            print("===================================== ERROR READING ===============================")
            attempt = 1
    # try:
    # test = sum_df.loc[sum_df['DocumentNo'].isnull(),'TCode']
    # print(test)
    if (attempt == 0):
        sum_df_fin = sum_df.dropna(subset=['DocumentNo'])
        sum_df_fin.drop('G/L Acct', axis=1, inplace=True)
        sum_df_fin.drop('Clearing', axis=1, inplace=True)
        sum_df_fin.drop('Clrng doc.', axis=1, inplace=True)
        sum_df_fin['G/L'] = sum_df_fin['G/L'].astype(np.int64)
        sum_df_fin['Amount'] = sum_df_fin['Amount'].str.replace(',', '').astype(float)
        sum_df_fin['Loc.curr.amount'] = sum_df_fin['Loc.curr.amount'].str.replace(',', '').astype(float)
        sum_df_fin['Final_Amount'] = sum_df_fin.apply(lambda row: define_final_amount(row), axis=1)
        sum_df_fin['Doc_Amount'] = sum_df_fin.apply(lambda row: define_doc_amount(row), axis=1)
        mapping_df = read_mapping()
        merged = pd.merge(sum_df_fin, mapping_df, on="TCode", how="left")
        unmapped_tcode = merged.loc[merged['Source'].isnull(), 'TCode']

        val = input("Do you want to change columns name? (Press Y and enter if yes): ")
        if (val == 'Y' or val == 'y'):
            merged = column_rename.rename_funct(merged)

        if (len(unmapped_tcode) != 0):
            print(
                "Following TCode exists within document and has not been mapped. Will cancel exporting process.\nPlease update the mapping csv.")
            distinct_unmapped_tcode = unmapped_tcode.drop_duplicates()
            # print(merged.loc[merged['Source'].isnull(),'TCode'])
            print(distinct_unmapped_tcode)
            return False
        else:
            # sum_df_fin['Source'] = sum_df_fin.apply (lambda row: define_je_type(row,mapping_df), axis=1)
            # print('formatted df')
            # print(merged.tail(5))
            # merged.info()
            output_path = 'output/' + company_code + '/'
            try:
                os.mkdir(output_path)
            except:
                print('Output path already exists')
            print("Start exporting...")
            merged.to_csv(output_path + company_code + '_' + fy + '_cleaned.txt', index=False, sep='|')
            non_dec = merged.loc[merged['Period'] != '12']
            non_dec.to_csv(output_path + company_code + '_Jan_Nov_' + fy + '_cleaned.txt', index=False, sep='|')
            dec = merged.loc[merged['Period'] == '12']
            dec.to_csv(output_path + company_code + '_Dec_' + fy + '_cleaned.txt', index=False, sep='|')
            sum_by_gl = merged.groupby(['G/L'])['Final_Amount'].sum().reset_index()
            sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_sumGL.txt', index=False, sep='|')
            # shutil.rmtree(removed_dupe_path)
            print('File for ' + company_code + ' successfuly cleansed.')
            print('====================================================================================')
            return True
            # merged.to_csv('4output_comma.csv', index=False)
            # test_print()
    else:
        print("Error reading " + company_code)
        return False
