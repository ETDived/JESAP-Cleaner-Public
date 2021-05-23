import warnings

import pandas as pd
import os
import numpy as np
import column_rename

pd.options.mode.chained_assignment = None  # default='warn'


#
def read_mapping():
    mapping_df = pd.read_csv('mapping_je_type.csv')

    return mapping_df


def define_final_lc1(row):
    if row['D/C'] == 'H':
        return (row['Amount in LC'] * -1)
    else:
        return row['Amount in LC']


def define_final_lc2(row):
    if row['D/C'] == 'H':
        return (row['LC2 amount'] * -1)
    else:
        return row['LC2 amount']


def define_final_lc3(row):
    if row['D/C'] == 'H':
        return (row['LC3 amount'] * -1)
    else:
        return row['LC3 amount']


def define_doc_amount(row):
    if row['D/C'] == 'H':
        return (row['Amount'] * -1)
    else:
        return row['Amount']


def define_je_type(row, mapping_df):
    return mapping_df.loc[mapping_df['TCODE'] == row['TCode'], 'JE Type'].item()


def manipulate_data(company_code, input_path, fy):
    # Select which LC Amount from raw data that want to be used
    lc = 0  # 0 = all, 1, 2, 3

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
                                 dtype={'DocumentNo': 'string',
                                        'Year': 'string',
                                        'Period': 'string',
                                        'Itm': 'string',
                                        'PK': 'string'})

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

    if (attempt == 0):
        sum_df_fin = sum_df.dropna(subset=['DocumentNo'])
        sum_df_fin.drop('Buffer1', axis=1, inplace=True)
        sum_df_fin.drop('Buffer2', axis=1, inplace=True)
        # sum_df_fin['G/L'] = sum_df_fin['G/L'].round()
        sum_df_fin['G/L'] = sum_df_fin['G/L'].astype(np.int64)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            if lc == 0 or lc == 1:
                try:
                    sum_df_fin['Amount in LC'] = sum_df_fin['Amount in LC'].str.replace(',', '').astype(float)
                except:
                    print("no amount in LC")
                    try:
                        sum_df_fin['Loc.curr.amount'] = sum_df_fin['Amount in LC'].str.replace('.', '')
                        sum_df_fin['Loc.curr.amount'] = sum_df_fin['Amount in LC'].str.replace(',', '.').astype(float)
                    except:
                        print("no valid lc amount")
            if lc == 0 or lc == 2:
                sum_df_fin['LC2 amount'] = sum_df_fin['LC2 amount'].str.replace('.', '')
                sum_df_fin['LC2 amount'] = sum_df_fin['LC2 amount'].str.replace(',', '.').astype(float)
            if lc == 0 or lc == 3:
                sum_df_fin['LC3 amount'] = sum_df_fin['LC3 amount'].str.replace('.', '')
                sum_df_fin['LC3 amount'] = sum_df_fin['LC3 amount'].str.replace(',', '.').astype(float)
            sum_df_fin['Amount'] = sum_df_fin['Amount'].str.replace(',', '').astype(float)

        if lc == 0 or lc == 1:
            sum_df_fin['Final_LC1'] = sum_df_fin.apply(lambda row: define_final_lc1(row), axis=1)
        if lc == 0 or lc == 2:
            sum_df_fin['Final_LC2'] = sum_df_fin.apply(lambda row: define_final_lc2(row), axis=1)
        if lc == 0 or lc == 3:
            sum_df_fin['Final_LC3'] = sum_df_fin.apply(lambda row: define_final_lc3(row), axis=1)

        sum_df_fin['Doc_Amount'] = sum_df_fin.apply(lambda row: define_doc_amount(row), axis=1)
        mapping_df = read_mapping()
        merged = pd.merge(sum_df_fin, mapping_df, on="TCode", how="left")
        unmapped_tcode = merged.loc[merged['Source'].isnull(), 'TCode']

        print("Dropping unnecessary columns.")
        merged.drop('CoCd', axis=1, inplace=True)
        merged.drop('Doc. Date', axis=1, inplace=True)
        merged.drop('TCode', axis=1, inplace=True)
        merged.drop('D/C', axis=1, inplace=True)
        merged.drop('Amount in LC', axis=1, inplace=True)
        merged.drop('Crcy', axis=1, inplace=True)
        merged.drop('LC2 amount', axis=1, inplace=True)
        merged.drop('LCur2', axis=1, inplace=True)
        merged.drop('LC3 amount', axis=1, inplace=True)
        merged.drop('LCur3', axis=1, inplace=True)
        merged.drop('Amount', axis=1, inplace=True)
        # merged.drop('Final_LC1', axis=1, inplace=True)
        # merged.drop('Final_LC2', axis=1, inplace=True)
        # merged.drop('Final_LC3', axis=1, inplace=True)

        non_dec = merged.loc[merged['Period'] != '12']
        dec = merged.loc[merged['Period'] == '12']
        doc_type_ip = merged.loc[merged['Doc. Type'] == 'IP']
        text_specific = merged.loc[merged['Text'] == 'Payroll On-Cycle Posting']

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
            output_path = 'output/' + company_code + '/'
            try:
                os.makedirs(output_path)
            except Exception as e:
                print(e)
                print('Output path already exists')
            print("Start importing...")


            if lc == 0 or lc == 1:
                merged.to_csv(output_path + '01_' + company_code + '_' + fy + '_cleaned.txt', index=False, sep='|')
                sum_by_gl = merged.groupby(['G/L'])['Final_LC1'].sum().reset_index()
                sum_by_gl.to_csv(output_path + '01_' + company_code + '_' + fy + '_sumGL_LC1.txt', index=False, sep='|')

                doc_type_ip.to_csv(output_path + '02_' + company_code + '_' + fy + '_IP_cleaned.txt', index=False,
                                   sep='|')
                sum_by_gl = doc_type_ip.groupby(['G/L'])['Final_LC1'].sum().reset_index()
                sum_by_gl.to_csv(output_path + '02_' + company_code + '_' + fy + '_IP_sumGL_LC1.txt', index=False,
                                 sep='|')

                text_specific.to_csv(
                    output_path + '03_' + company_code + '_' + fy + '_Payroll_on_cycle_posting_cleaned.txt',
                    index=False, sep='|')
                sum_by_gl = text_specific.groupby(['G/L'])['Final_LC1'].sum().reset_index()
                sum_by_gl.to_csv(
                    output_path + '03_' + company_code + '_' + fy + '_Payroll_on_cycle_posting_sumGL_LC1.txt',
                    index=False, sep='|')

                # non_dec.to_csv(output_path + company_code + '_' + fy + '_octnov_cleaned.txt', index=False, sep='|')
                # sum_by_gl = non_dec.groupby(['G/L'])['Final_LC1'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_octnov_sumGL_LC1.txt', index=False, sep='|')

                # dec.to_csv(output_path + company_code + '_' + fy + '_dec_cleaned.txt', index=False, sep='|')
                # sum_by_gl = dec.groupby(['G/L'])['Final_LC1'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_dec_sumGL_LC1.txt', index=False, sep='|')
            if lc == 0 or lc == 2:
                merged.to_csv(output_path + company_code + '_' + fy + '_cleaned.txt', index=False, sep='|')
                sum_by_gl = merged.groupby(['G/L', 'Period'])['Final_LC2'].sum().reset_index()
                sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_sumGL_LC2.txt', index=False, sep='|')

                # non_dec.to_csv(output_path + company_code + '_' + fy + '_octnov_cleaned.txt', index=False, sep='|')
                # sum_by_gl = non_dec.groupby(['G/L'])['Final_LC2'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_octnov_sumGL_LC2.txt', index=False, sep='|')

                # dec.to_csv(output_path + company_code + '_' + fy + '_dec_cleaned.txt', index=False, sep='|')
                # sum_by_gl = dec.groupby(['G/L'])['Final_LC2'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_dec_sumGL_LC2.txt', index=False, sep='|')
            if lc == 0 or lc == 3:
                merged.to_csv(output_path + company_code + '_' + fy + '_cleaned.txt', index=False, sep='|')
                sum_by_gl = merged.groupby(['G/L'])['Final_LC3'].sum().reset_index()
                sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_sumGL_LC3.txt', index=False, sep='|')

                # non_dec.to_csv(output_path + company_code + '_' + fy + '_octnov_cleaned.txt', index=False, sep='|')
                # sum_by_gl = non_dec.groupby(['G/L'])['Final_LC3'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_octnov_sumGL_LC3.txt', index=False, sep='|')

                # dec.to_csv(output_path + company_code + '_' + fy + '_dec_cleaned.txt', index=False, sep='|')
                # sum_by_gl = dec.groupby(['G/L'])['Final_LC3'].sum().reset_index()
                # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_dec_sumGL_LC3.txt', index=False, sep='|')

            # sum_by_gl = merged.groupby(['G/L'])['Final_LC2'].sum().reset_index()
            # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_sumGL_LC2.txt', index=False, sep='|')
            # sum_by_gl = merged.groupby(['G/L'])['Final_LC3'].sum().reset_index()
            # sum_by_gl.to_csv(output_path + company_code + '_' + fy + '_sumGL_LC3.txt', index=False, sep='|')
            # shutil.rmtree(removed_dupe_path)
            print('File for ' + company_code + ' successfuly cleansed.')
            print('====================================================================================')
            return True
            # merged.to_csv('4output_comma.csv', index=False)
            # test_print()
    else:
        print("Error reading " + company_code)
        return False
