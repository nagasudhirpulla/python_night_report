import xlwings as xw
import analysis_modules.analysis_helper as analysis_helper

@xw.func
def hello(name):
    return "hello {0}".format(name)

@xw.func
def set_limits_in_db():
    wb = xw.Book.caller()
    analysis_helper.set_limits_in_db(wb)
    
@xw.func

def analyse_and_print_violations():
    wb = xw.Book.caller()
    violationsDF = analysis_helper.analyse_violations_db()
    sheetName = 'VIOLATIONS'
    wb.sheets[sheetName].range("A1").options(index=False).value = violationsDF