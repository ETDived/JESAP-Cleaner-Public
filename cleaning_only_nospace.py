import os
import sys
import shutil
import io
import processing_client_1_default
import processing_client_1_alt_1
import processing_client_2_default
import approach_fbl_appcon
import pandas as pd
import csv

"""Read from raw SAP output file and convert into CSV (header row will be
duplicated)

Parameters
----------
read_path : str
    The folder location of targeted file(s)
temp_path : str
    The folder location for temp file(s)
company_code : str
    The company code OR the folder name of current data.

Returns
-------
bool
    will return True if all process was successfully done, False otherwise.
"""
def temp_read(read_path, temp_path, company_code):
    # Read and list all files inside the working path
    files = sorted(os.listdir(read_path))
    ii = 0

    for infile in files:
        target_path = temp_path +'cleaned/'+ company_code+'_'+str(ii)+'.csv'
        print('Cleaning ' + os.path.join(read_path, infile))

        # SAP output use ISO-8859-15 encoding most of the time.
        # Change if necessary.
        with io.open(os.path.join(read_path, infile), 'r', encoding='ISO-8859-15', newline='') as f_input, \
             io.open(target_path, 'w', encoding='ISO-8859-15', newline='') as f_output:

            # Filter to read only row start or end with '|'
            input_lines = filter(lambda x: len(x) > 2 and x[0] == '|' and x[1] != '-', f_input)

            # Initiate reader and writer object
            csv_input = csv.reader(input_lines, delimiter='|', quoting=csv.QUOTE_NONE)  # ,quoting=csv.QUOTE_NONE
            csv_output = csv.writer(f_output, delimiter='|')

            # Save last readed row. Said row will be printed within try block
            # below if errors encountered.
            last_row = ""
            try:
                n_row = 0
                for row_n, row in enumerate(csv_input):
                    last_row = row
                    if n_row > 1:
                        csv_output.writerow(col.strip() for col in row[1:-1])
                    n_row += 1
            except:
                print(last_row)
                print("Bad data, last successfully readed row printed above. Please check data.")
                return False
        ii = ii + 1

    return True

def combine_csv(temp_path, output_path, company_code):
    cleaned_path = temp_path + 'removed_dupe/'
    target_path_temp = output_path + company_code + '_combined_temp' + '.csv'
    target_path_fin = output_path + company_code + '_cleaned_combined' + '.csv'
    files = sorted(os.listdir(cleaned_path))
    ii = 0
    # combine all files in the list
    #print(files)
    combined_csv_raw = pd.concat([pd.read_csv(os.path.join(cleaned_path, f), delimiter='|', low_memory=False, encoding='ISO-8859-15', dtype={'DocumentNo':'string','Year':'string','Period':'string','User Name':'string'}) for f in files])
    combined_csv = combined_csv_raw.dropna(subset=['DocumentNo'])
    # export to csv
    combined_csv.to_csv(target_path_temp, index=False,  sep='|', encoding='ISO-8859-15')

    with io.open(os.path.join(target_path_temp), 'r', encoding='ISO-8859-15') as inputfile, \
            io.open(target_path_fin, "w", newline="", encoding='ISO-8859-15') as outputfile:

        # Initiate reader and writer object.
        csv_in = csv.reader(inputfile, delimiter='|', quoting=csv.QUOTE_NONE)
        csv_out = csv.writer(outputfile, delimiter='|')

        # Get the title (header) row.
        title = next(csv_in)
        # print(title)
        # header_temp = title
        # print(header_temp)
        updated_title = title[:]
        #additional_buffer = "Buffer1"
        #updated_title.append(additional_buffer)
        #additional_buffer = "Buffer2"
        #updated_title.append(additional_buffer)

        # print(updated_title)
        csv_out.writerow(updated_title)

        # This try block will check every row whether it match with the
        # title (header) row. Row will be skipped if it match the title row.
        # Never encounter any error, but the try block is there just for
        # precaution.
        # Please do contact me if you found error in this part.
        try:
            aa = 0
            for row in csv_in:
                if row != title:
                    csv_out.writerow(row)
                """
                if aa == 58:
                    print(row)
                    print(title)
                    #print(header_temp)
                aa = aa + 1
                """
        except Exception as error_write:
            print(error_write)
            return False

    ii = ii + 1
    os.remove(target_path_temp)
"""Read from cleaned SAP output file (CSV) and convert into bar delimited CSV
with duplicate header row removed.

Parameters
----------
temp_path : str
    The folder location for temp file(s)
company_code : str
    The company code OR the folder name of current data.

Returns
-------
bool
    will return True if all process was successfully done, False otherwise.
"""
def temp_remove_header(temp_path, company_code):
    cleaned_path = temp_path +'cleaned/'
    files = sorted(os.listdir(cleaned_path))
    ii = 0

    for infile in files:
        target_path = temp_path+'removed_dupe/' + company_code+'_'+str(ii)+'.csv'
        print('Removing duplicate header '+target_path+' ('+str(((ii+1)/len(files))*100)+'%)')

        with io.open(os.path.join(cleaned_path, infile),'r',encoding='ISO-8859-15') as inputfile, \
             io.open(target_path,"w",newline="",encoding='ISO-8859-15') as outputfile:

            # Initiate reader and writer object.
            csv_in = csv.reader(inputfile,delimiter='|',quoting=csv.QUOTE_NONE)
            csv_out = csv.writer(outputfile, delimiter='|')

            # Get the title (header) row.
            title = next(csv_in)
            #print(title)
            #header_temp = title
            #print(header_temp)
            updated_title = title[:]
            additional_buffer = "Buffer1"
            updated_title.append(additional_buffer)
            additional_buffer = "Buffer2"
            updated_title.append(additional_buffer)

            #print(updated_title)
            csv_out.writerow(updated_title)

            # This try block will check every row whether it match with the
            # title (header) row. Row will be skipped if it match the title row.
            # Never encounter any error, but the try block is there just for
            # precaution.
            # Please do contact me if you found error in this part.
            try:
                aa = 0
                for row in csv_in:
                    if row != title:
                        csv_out.writerow(row)
                    """
                    if aa == 58:
                        print(row)
                        print(title)
                        #print(header_temp)
                    aa = aa + 1
                    """
            except Exception as error_write:
                print(error_write)
                return False

        ii = ii + 1

    return True

"""Primary/major method. Start with staging (create directory etc.), cleaning
raw data, and processing. All heavy duty process is separated into different
method to make changes easier.

Parameters
----------
company_code : str
    The company code OR the folder name of current data.
input_path : str
    The folder location of raw file(s)
fy : str
    Indicate the FY for the in-progress data
delete_temp : bool, optional
    A flag used to delete the temp file or not

Returns
-------
bool
    will return True if all process was successfully done, False otherwise.
"""
def primary_read(company_code, input_path, fy, delete_temp):
    # Precatuion steps. Set the size limit for csv reading ---------------------
    maxInt = sys.maxsize

    while True:
        # decrease the maxInt value by factor 10
        # as long as the OverflowError occurs.
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)
    #---------------------------------------------------------------------------

    # Staging Phase (Reading file, creating dir etc) ---------------------------
    temp_path = 'temp/'+company_code+'_'+fy+'/'
    read_path = input_path+company_code+'/'
    output_path = 'output/' + company_code + '/'
    print("Entering Staging Phase, removing all existing temp files....")

    # Create the '.../temp/' dir incase this is the first time user run this
    # program. If '.../temp/' dir already exists, line #74 will throws exception
    try:
        os.makedirs('temp/')
    except:
        print('Temp path already exists')

    # Create temp path for working Folder/CoCd. Line #82 will throws exception
    # if path already exists. Path may already exists if user run this program
    # before with delete remove argument/flag.
    try:
        os.makedirs(temp_path)
    except:
        print("Temp path for in-progress company/folder already exists")

    # Create 'cleaned' temp path for the cleaning stage. Line #90 will throws
    # exception if path already exists. Path may already exists if user run this
    # program before with delete remove argument/flag.
    try:
        os.makedirs(temp_path+'cleaned/')
    except:
        print("Existing 'cleaned/' detected, deleting.")
        shutil.rmtree(temp_path+'cleaned/')
        os.makedirs(temp_path+'cleaned/')

    try:
        os.makedirs(temp_path+'combined/')
    except:
        print("Existing 'cleaned/' detected, deleting.")
        shutil.rmtree(temp_path+'combined/')
        os.makedirs(temp_path+'combined/')


    try:
        os.makedirs(output_path)
    except Exception as e:
        print(e)
        print('Output path already exists')
    print("Start importing...")

    # Create 'removed_dupe' temp path. Line #82 will throws exception if path
    # already exists. Path may already exists if user run this program before
    # with delete remove argument/flag.
    try:
        os.makedirs(temp_path+'removed_dupe/')
    except:
        print("Existing 'removed_dupe/' detected, deleting.")
        #shutil.rmtree(temp_path+'removed_dupe/')
        #os.makedirs(temp_path+'removed_dupe/')
    #---------------------------------------------------------------------------

    # Data processing stage ----------------------------------------------------
    # Invoke temp_read method


    if (temp_read(read_path, temp_path, company_code) == False):
        try:
            shutil.rmtree(input_path+'zz_pending/'+company_code+'/')
        except:
            pass
        try:
            shutil.move(os.getcwd()+'/'+read_path, os.getcwd()+'/'+input_path+'zz_pending/')
        except Exception as error_moving:
            print(error_moving)

        print("Error processing '"+company_code+" "+fy+"'. Raw file will be moved to 'zz_pending'.")
        return False



    # Invoke temp_remove_header method
    temp_remove_header(temp_path, company_code)

    combine_csv(temp_path, output_path, company_code)
    print("Cleaning done. Check")
    print(output_path)
    if (delete_temp):
        shutil.rmtree(temp_path + 'cleaned/')
        shutil.rmtree(temp_path + 'removed_dupe/')
        shutil.rmtree(temp_path)
    #---------------------------------------------------------------------------
