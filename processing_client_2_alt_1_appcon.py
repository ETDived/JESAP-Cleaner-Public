import warnings

import pandas as pd
import os
import shutil

pd.options.mode.chained_assignment = None  # default='warn'


def process_heading():
    mapping_df = pd.read_csv('mapping_je_type.csv')
    heading_df = pd.read_csv('FAGL_Header.csv', delimiter='|', low_memory=False, encoding='ISO-8859-15')
    merged = pd.merge(heading_df, mapping_df, on="TCode", how="left")
    unmapped_tcode = merged.loc[merged['Source'].isnull(), 'TCode']
    if (len(unmapped_tcode) != 0):
        print("Following TCode exists within document and has not been mapped.\nPlease update the mapping csv.")
        distinct_unmapped_tcode = unmapped_tcode.drop_duplicates()
        # print(merged.loc[merged['Source'].isnull(),'TCode'])
        print(distinct_unmapped_tcode)
    return merged


def define_final_lc1(row):
    if row['Debit/Credit'] == 'H':
        return (row['LC Amount'] * -1)
    else:
        return row['LC Amount']


def define_doc_amount(row):
    if row['Debit/Credit'] == 'H':
        return (row['OrigTrnsCrcyAmt'] * -1)
        # return (row['Amount in TC']*-1)
    else:
        return row['OrigTrnsCrcyAmt']
        # return row['Amount in TC']


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
            df[ii] = pd.read_csv(file_path, delimiter='|', low_memory=False, encoding='ISO-8859-15',
                                 dtype={'Document Number': 'int64',
                                        'Fiscal Year': 'string',
                                        'Period': 'string',
                                        'Line Item': 'string',
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

        print("Removing unncessary column")
        try:
            sum_df_fin = sum_df.dropna(subset=['DocumentNo'])
        except Exception as error_reading:
            print(error_reading)


        output_path = 'output/' + company_code + '/'

        try:
            os.makedirs(output_path)
        except Exception as e:
            print(e)
            print('Output path already exists')
            shutil.rmtree(output_path)
            os.makedirs(output_path)

        grouped = sum_df_fin.groupby(['CoCd', 'User Name', 'TCode', 'Doc. Type']).DocumentNo.nunique().reset_index()
        grouped.to_csv(output_path + company_code + '_00_' + fy + '_Final.txt', index=False, sep='|')


        print('File for ' + company_code + ' successfuly cleansed.')
        print('====================================================================================')
        return True
        # merged.to_csv('4output_comma.csv', index=False)
        # test_print()
    else:
        print("Error reading " + company_code)
        return False
