import os
import cleaning_client_1
import cleaning_client_1_nospace
import cleaning_faglflexa
import cleaning_client_2
import cleaning_client_2_nospace
import cleaning_only
import cleaning_only_nospace


def main():
    list_input_path = ['input/']
    list_input_fy = ['20']
    delete_temp = True  # True if you want to delete temp file after finished processing, or false if you want to keep it
    entity_type = 1     # Select accordingly, please see the switcher line of block below
    have_space = True   # Please check if the raw data have space after it's first (most left) bar. Use true if there's any space, and false if there's no space after the first bar.

    # ========== do not change codes below if you're not sure with what you're doing
    ii = 0
    switcher = {
        1: client_1,
        2: client_2,
        3: cleaning_only,
        4: faglflexa
    }
    func = switcher.get(entity_type, lambda: "Invalid entity")
    for input_path in list_input_path:
        list_cocd = sorted(os.listdir(input_path))
        for infiles in list_cocd:
            if infiles == 'zz_done' or infiles == 'zz_pending':
                print('Skipping' + infiles)
            else:
                print('====================================================================================')
                print('Working on ' + infiles)
                func(infiles, input_path, list_input_fy[ii], delete_temp, have_space)
        ii = ii + 1


def client_1(infiles, input_path, list_input_fy, delete_temp, space):
    if space:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_client_1.primary_read(infiles, input_path, list_input_fy, delete_temp)
    else:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_client_1_nospace.primary_read(infiles, input_path, list_input_fy, delete_temp)


def client_2(infiles, input_path, list_input_fy, delete_temp, space):
    if space:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_client_2.primary_read(infiles, input_path, list_input_fy, delete_temp)
    else:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_client_2_nospace.primary_read(infiles, input_path, list_input_fy, delete_temp)


def cleaning_only(infiles, input_path, list_input_fy, delete_temp, space):
    if space:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_only.primary_read(infiles, input_path, list_input_fy, delete_temp)
    else:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_only_nospace.primary_read(infiles, input_path, list_input_fy, delete_temp)


def faglflexa(infiles, input_path, list_input_fy, delete_temp, space):
    if space:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_faglflexa.primary_read(infiles, input_path, list_input_fy, delete_temp)
    else:
        print('====================================================================================')
        print('Working on ' + infiles)
        cleaning_faglflexa.primary_read(infiles, input_path, list_input_fy, delete_temp)


if __name__ == '__main__':
    main()
