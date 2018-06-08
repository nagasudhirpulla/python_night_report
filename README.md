## Instruction for enabling python in excel macros
* Install Anaconda
* Open Anaconda Prompt as **Administrator**
* Add conda-forge channel for anaconda ```conda config --add channels conda-forge```
* Update xlwings using conda-forge ```conda update -c conda-forge xlwings```
* Install postgres library ```conda install -c conda-forge psycopg2```
* Install xlwings addin ```xlwings addin install```
* Create a project using the command ```xlwings quickstart project_name```
* Add the folder path of pythonw.exe and python.exe in system environment variable named **Path**

## Excel settings for enabling python macros
* In Interpreter input at top left write path for pythonw.exe ```C:\ProgramData\Anaconda2\pythonw.exe```
* Tick the RunPython: Use UDF Server checkbox in the top right
* In File->Options->Trust Center->Trust Center Settings->Macro Settings->Tick the ```Trust access to VBA project object model``` checkbox
* If xlwings addin is not shown on the top ribbon, goto DEVELOPER->ADD-Ins and browse for xlwings.xlam file

### Links
* Increase git postbuffer for large file sizes during push or clone - https://stackoverflow.com/questions/6842687/the-remote-end-hung-up-unexpectedly-while-git-cloning/19286776
* xlwings installation - http://docs.xlwings.org/en/stable/addin.html#installation
* For setting path variables if an issue is found like ```python command not recognized``` follow https://www.pythoncentral.io/add-python-to-path-python-is-not-recognized-as-an-internal-or-external-command/ and set path variables for folders containing python.exe and pythonw.exe (python interpreter)
* python get relative path files https://stackoverflow.com/questions/1270951/how-to-refer-to-relative-paths-of-resources-when-working-with-a-code-repository ```https://stackoverflow.com/questions/1270951/how-to-refer-to-relative-paths-of-resources-when-working-with-a-code-repository```. Open files from relative location in pandas https://stackoverflow.com/questions/35384358/how-to-open-my-files-in-data-folder-with-pandas-using-relative-path ```pd.read_csv('../../../data_folder/data.csv')```
* Recalculate all cells - https://spreadsheeto.com/recalculate-and-refresh-formulas/
* xlwings importing issue in github - https://github.com/ZoomerAnalytics/xlwings/issues/634