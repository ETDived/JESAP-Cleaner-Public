# JESAP-Cleaner (Public build)

JESAP-Cleaner is a tool created to make cleaning Journal Entry .txt file from SAP easier. This repository contained the build to be used for public (removing all possible entities refeferences). 

User will only need to provide raw .txt file and the output will be a pipe ( | ) delimited .txt file. The output will also contains following additional columns:

* **Final Amount (Final_Amount)**

  Obtained from 'Loc.curr.amount' column, the value will have either minus or positive based on the value of it's 'D/C' column. With 'H' having negative value while 'S' having positive value.


* **Document Amount (Doc_Amount)**

  Obtained from 'Amount' column, the value will have either minus or positive based on the value of it's 'D/C' column. With 'H' having negative value while 'S' having positive value.


* **Source**

  Obtained by mapping the 'TCode' column with provided mapping matrix (.csv).
## Installation

Install Python 3. If you're using Windows feel free to use this [tutorial](https://phoenixnap.com/kb/how-to-install-python-3-windows). It's highly recommended to install and use **virtualenv** as it will stop you from installing required package system-wide.

Once the **virtualenv** is activated (or if you're not using it, once you're in Python terminal), install required packages by running this command.
```bash
pip install -r requirements.txt
```

## Usage
Put your raw data using similiar structure:
```
|-- input
|   |-- Company_AA
|   |   |-- AA_Jan.txt
|   |   |-- AA_Feb.txt
|   |   |-- AA_Mar.txt
|   |-- Company_AB
|   |   |-- AB_Jan.txt
|   |   |-- AB_Feb.txt
|   |   |-- AB_Mar.txt
|   |-- Company_AC
|   |   |-- AC_Jan.txt
|   |   |-- AC_Feb.txt
|   |   |-- AC_Mar.txt
|-- main.py
|-- README.md
|-- requirements.txt
```
You may name the 'input' folder as you please as long as you're creating that folder within the working directory (same folder with the Python files).

Open main.py with your editor of choice.
```python
def main():
    list_input_path = ['input/','input_last_FY/']
    list_input_fy = ['20','19']
    delete_temp = True  
    entity_type = 1     
    have_space = True 
```
As seen above, you may separate different folder based on their FY. Change both list according to your data/needs. 
* Set *delete_temp* flag to **False** if you want the cache/temp files to not be removed. You can use it to check faulty lines.
* Set *entity_type* to indicate which client (or data structure to be exact) that going to be processed. There are total 4 possible choice.
* Set *have_space* by checking your data. If your data have space after it's first delimited/pipe like example below,
  ```python
  | CoCd|DocumentNo|Year|Doc. Type|Doc. Date|Pstng Date|Period|Entry Dte|Time|Username|TCode......
  ```
  set the variable to **True**. While if your data does not have space after it's first delimited/pipe like example below,
  ```python
  |CoCd|DocumentNo|Year|Doc. Type|Doc. Date|Pstng Date|Period|Entry Dte|Time|Username|TCode......
  ```
  set the variable to **False**.

Additionaly, for **client_1** there's possibility that your raw data have multiple *Local Currency* column (e.g; *Amount in LC, LC2 Amount, LC3 Amount*). 

Please navigate to either ***processing_client_1_default.py*** or ***processing client_1_alt.py*** and go to line 50, there should be this line of code:
```python
def manipulate_data(company_code, input_path, fy):
    # Select which LC Amount from raw data that want to be used
    lc = 0  # 0 = all, 1, 2, 3
```
By default, the variable will be set to **0**, and **all 3 column** will be processed. If you only need one column, feel free to changes the variable accordingly.


Once everything sets, open your Python terminal and run main.py
## To do
* Assign thread for each company in input folder make the script process multiple folder/company concurrently
* Add simple text input so user doesn't need to manually edit python files
* Add GUI 
* Create executable or Windows installer to make it easier to run
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[GNU GPLv3](https://choosealicense.com/licenses/gpl-3.0/)
