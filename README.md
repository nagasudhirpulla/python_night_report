## Instruction for enabling python in excel macros
* Install Anaconda
* Add conda-forge channel for anaconda ```conda config --add channels conda-forge```
* Update xlwings using conda-forge. For this open command prompt as Administrator and type ```conda update -c conda-forge xlwings```
* Install xlwings addin ```xlwings addin install```
* Create a project using the command ```xlwings quickstart project_name```


### Links
* xlwings installation - http://docs.xlwings.org/en/stable/addin.html#installation
* For setting path variables if an issue is found like ```python command not recognized``` follow https://www.pythoncentral.io/add-python-to-path-python-is-not-recognized-as-an-internal-or-external-command/ and set path variables for folders containing python.exe and pythonw.exe (python interpreter)