import warnings

import pandas as pd
import os
import numpy as np
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
    else:
        return row['OrigTrnsCrcyAmt']


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
                                 dtype={'Document Number': 'int64', 'Fiscal Year': 'string', 'Period': 'string',
                                        'Line Item': 'string', 'PK': 'string'})  # ,quoting=csv.QUOTE_NONE

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
            sum_df_fin = sum_df.dropna(subset=['Document Number'])
            sum_df_fin.drop('Buffer1', axis=1, inplace=True)
            sum_df_fin.drop('Buffer2', axis=1, inplace=True)
            sum_df_fin.drop('Client', axis=1, inplace=True)
            sum_df_fin.drop('Ledger', axis=1, inplace=True)
            sum_df_fin.drop('Company Code', axis=1, inplace=True)
            sum_df_fin.drop('Line Item', axis=1, inplace=True)
            sum_df_fin.drop('Transaction', axis=1, inplace=True)
            sum_df_fin.drop('Transactn Type', axis=1, inplace=True)
            sum_df_fin.drop('Base Unit', axis=1, inplace=True)
            sum_df_fin.drop('Ref. Transactn', axis=1, inplace=True)
            sum_df_fin.drop('Record Type', axis=1, inplace=True)
            sum_df_fin.drop('Version', axis=1, inplace=True)
            sum_df_fin.drop('Logical system', axis=1, inplace=True)
            sum_df_fin.drop('Cost Element', axis=1, inplace=True)
            sum_df_fin.drop('Cost Center', axis=1, inplace=True)
            sum_df_fin.drop('Functional Area', axis=1, inplace=True)
            # sum_df_fin.drop('Business Area', axis=1, inplace=True)
            sum_df_fin.drop('CO Area', axis=1, inplace=True)
            sum_df_fin.drop('Segment', axis=1, inplace=True)
            sum_df_fin.drop('Order', axis=1, inplace=True)
            sum_df_fin.drop('WBS Element', axis=1, inplace=True)
            sum_df_fin.drop('CF.Activity', axis=1, inplace=True)
            sum_df_fin.drop('CF.T-Partner', axis=1, inplace=True)
            sum_df_fin.drop('IFRS Tax Cat', axis=1, inplace=True)
            sum_df_fin.drop('Sender cost ctr', axis=1, inplace=True)
            sum_df_fin.drop('Partner PC', axis=1, inplace=True)
            sum_df_fin.drop('Partner FArea', axis=1, inplace=True)
            sum_df_fin.drop('Trdg Part.BA', axis=1, inplace=True)
            sum_df_fin.drop('Trading Partner', axis=1, inplace=True)
            sum_df_fin.drop('Partner Segment', axis=1, inplace=True)
            sum_df_fin.drop('Amount in TC', axis=1, inplace=True)
            sum_df_fin.drop('Amnt in GrpCrcy', axis=1, inplace=True)
            sum_df_fin.drop('Other Crcy Amnt', axis=1, inplace=True)
            sum_df_fin.drop('Quantity', axis=1, inplace=True)
            # sum_df_fin.drop('OrigTrnsCrcyAmt', axis=1, inplace=True)
            sum_df_fin.drop('Or.trans.currny', axis=1, inplace=True)
            sum_df_fin.drop('Fiscal Year.1', axis=1, inplace=True)
            sum_df_fin.drop('Document Number.1', axis=1, inplace=True)
            sum_df_fin.drop('Line item', axis=1, inplace=True)
            sum_df_fin.drop('Posting key', axis=1, inplace=True)
            sum_df_fin.drop('Doc.status', axis=1, inplace=True)
            sum_df_fin.drop('Item category', axis=1, inplace=True)
            sum_df_fin.drop('Changed', axis=1, inplace=True)
            sum_df_fin.drop('Time Stamp', axis=1, inplace=True)
        except Exception as error_reading:
            print(error_reading)

        # sum_df_fin['G/L'] = sum_df_fin['G/L'].round()
        sum_df_fin['Account Number'] = sum_df_fin['Account Number'].astype(np.int64)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            sum_df_fin['OrigTrnsCrcyAmt'] = sum_df_fin['OrigTrnsCrcyAmt'].str.replace('.', '')
            sum_df_fin['OrigTrnsCrcyAmt'] = sum_df_fin['OrigTrnsCrcyAmt'].str.replace('-', '')
            sum_df_fin['OrigTrnsCrcyAmt'] = sum_df_fin['OrigTrnsCrcyAmt'].str.replace(',', '.').astype(float)
            sum_df_fin['LC Amount'] = sum_df_fin['LC Amount'].str.replace('.', '')
            sum_df_fin['LC Amount'] = sum_df_fin['LC Amount'].str.replace('-', '')
            sum_df_fin['LC Amount'] = sum_df_fin['LC Amount'].str.replace(',', '.').astype(float)

        sum_df_fin['Final_LC1'] = sum_df_fin.apply(lambda row: define_final_lc1(row), axis=1)
        sum_df_fin['Doc_Amount'] = sum_df_fin.apply(lambda row: define_doc_amount(row), axis=1)
        processed_header = process_heading()
        merged = pd.merge(sum_df_fin, processed_header, on="Document Number", how="left")

        merged.drop('LC Amount', axis=1, inplace=True)
        merged.drop('OrigTrnsCrcyAmt', axis=1, inplace=True)
        merged.drop('Debit/Credit', axis=1, inplace=True)
        merged.drop('TCode', axis=1, inplace=True)

        output_path = 'output/' + company_code + '/'
        try:
            os.makedirs(output_path)
        except Exception as e:
            print(e)
            print('Output path already exists')
            shutil.rmtree(output_path)
            os.makedirs(output_path)

        unmapped_tcode = []
        # merged_interim = merged.loc[merged['Posting period'] < 10]
        merged_non_dec = merged.loc[merged['Posting period'] != 12]
        # merged_oct_nov = merged.loc[merged['Posting period'].isin[10, 11]]
        merged_dec = merged.loc[merged['Posting period'] == 12]
        merged_non_dec.to_csv(output_path + company_code + '_00_' + fy + '_cleaned.txt', index=False, sep='|')
        merged_dec.to_csv(output_path + company_code + '_00_' + fy + '_Dec_cleaned.txt', index=False, sep='|')
        sum_by_gl = merged.groupby(['Account Number'])['Final_LC1'].sum().reset_index()
        sum_by_gl.to_csv(output_path + company_code + '_00_' + fy + '_sumGL_LC1.txt', index=False, sep='|')

        cocd_5000 = merged.loc[merged['Profit Center'] == 5000000]
        cocd_5000_non_dec = merged_non_dec.loc[merged_non_dec['Profit Center'] == 5000000]
        cocd_5000_dec = merged_dec.loc[merged_dec['Profit Center'] == 5000000]
        cocd_5000_non_dec.to_csv(output_path + company_code + '_01_5000000_' + fy + '_cleaned.txt', index=False, sep='|')
        cocd_5000_dec.to_csv(output_path + company_code + '_01_5000000_' + fy + '_Dec_cleaned.txt', index=False, sep='|')
        sum_by_gl = cocd_5000.groupby(['Account Number'])['Final_LC1'].sum().reset_index()
        sum_by_gl.to_csv(output_path + company_code + '_01_5000000_' + fy + '_sumGL_LC1.txt', index=False, sep='|')

        cocd_non_5000 = merged.loc[merged['Profit Center'] != 5000000]
        cocd_non_5000_non_dec = merged_non_dec.loc[merged_non_dec['Profit Center'] != 5000000]
        cocd_non_5000_dec = merged_dec.loc[merged_dec['Profit Center'] != 5000000]
        cocd_non_5000_non_dec.to_csv(output_path + company_code + '_02_non_5000000_' + fy + '_cleaned.txt', index=False,
                                    sep='|')
        cocd_non_5000_dec.to_csv(output_path + company_code + '_02_non_5000000_' + fy + '_Dec_cleaned.txt', index=False,
                                sep='|')
        sum_by_gl = cocd_non_5000.groupby(['Account Number'])['Final_LC1'].sum().reset_index()
        sum_by_gl.to_csv(output_path + company_code + '_02_non_5000000_' + fy + '_sumGL_LC1.txt', index=False, sep='|')

        """
        interim_5000 = merged_interim.loc[merged_interim['Profit Center'] == 5000000]
        interim_non_5000 = merged_interim.loc[merged_interim['Profit Center'] != 5000000]
        interim_5000.to_csv(output_path + company_code + '_03_int_5000000_' + fy + '_cleaned.txt', index=False, sep='|')
        interim_non_5000.to_csv(output_path + company_code + '_03_int_non_5000000_' + fy + '_cleaned.txt', index=False, sep='|')
        sum_by_gl = interim_5000.groupby(['Account Number'])['Final_LC1'].sum().reset_index()
        sum_by_gl.to_csv(output_path + company_code + '_03_int_5000000_' + fy + '_sumGL_LC1.txt', index=False, sep='|')
        sum_by_gl = interim_non_5000.groupby(['Account Number'])['Final_LC1'].sum().reset_index()
        sum_by_gl.to_csv(output_path + company_code + '_03_int_non_5000000_' + fy + '_sumGL_LC1.txt', index=False, sep='|')
        """

        print('File for ' + company_code + ' successfuly cleansed.')
        print('====================================================================================')
        return True
    else:
        print("Error reading " + company_code)
        return False
