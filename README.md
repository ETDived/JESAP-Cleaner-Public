# JESAP-Cleaner

JESAP-Cleaner is a tool created to make cleaning Journal Entry .txt file from SAP easier. User will only need to provide raw .txt file and the output will be a pipe ( | ) delimited .txt file. The output will also contains following additional columns:

* **Final Amount (Final_Amount)**

  Obtained from 'Loc.curr.amount' column, the value will have either minus or positive based on the value of it's 'D/C' column. With 'H' having negative value while 'S' having positive value.


* **Document Amount (Doc_Amount)**

  Obtained from 'Amount' column, the value will have either minus or positive based on the value of it's 'D/C' column. With 'H' having negative value while 'S' having positive value.

* **Source**

  Obtained by mapping the 'TCode' column with provided mapping matrix (.csv).
## Installation

**I never tried running this tool on Windows, therefore I can't guarantee it will work.**

Install Python 3. If you're using Windows feel free to use this [tutorial](https://phoenixnap.com/kb/how-to-install-python-3-windows). It's highly recommended to install and use **virtualenv** as it will stop you from installing required package system-wide.

Once the **virtualenv** is activated (or if you're not using it, once you're in Python terminal), install **pandas** by using this command.
```bash
pip install pandas
```

## Usage
Put your raw data using similiar structure:
```
|-- input
|   |-- AA
|   |   |-- AA_Jan.txt
|   |   |-- AA_Feb.txt
|   |   |-- AA_Mar.txt
|   |-- AB
|   |   |-- AB_Jan.txt
|   |   |-- AB_Feb.txt
|   |   |-- AB_Mar.txt
|   |-- AC
|   |   |-- AC_Jan.txt
|   |   |-- AC_Feb.txt
|   |   |-- AC_Mar.txt
```
You may name the 'input' folder as you please as long as you're creating that folder within the working directory (same folder with the Python files).

Open main.py with your editor of choice.
```python
def main():
    list_input_path = ['input/','input_19/']
    list_input_fy = ['20','19']
    removed_temp = True
```
As seen above, you may separate different folder based on their FY. Change both list according to your data/needs. Set *removed_temp* flag to **False** if you want the cache/temp files to not be removed. You can use it to check faulty lines.

Once everything sets, open your Python terminal and run main.py
## To do
* Multi threading to make the tool process multiple folder/company concurrently
* Add additional error handling
* Add logging for easier trouble pin-point process
* Add simple text input so user doesn't need to manually edit python files
* Clean the code
* Add GUI (I'm bad at this, so don't get your hope high)
* Create .exe or Windows installer to make it easier to run (need GUI first so yeah don't wait)
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[GNU GPLv3](https://choosealicense.com/licenses/gpl-3.0/)
