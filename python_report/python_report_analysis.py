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

@xw.func
@xw.arg('fetch_date', doc='fetch_date')
@xw.arg('num_days', doc='num_days')
def get_last_n_days_deviations(fetch_date, num_days):
    wb = xw.Book.caller()
    violationsDF = analysis_helper.get_last_n_days_deviations(fetch_date, num_days)
    sheetName = 'DEVIATIONS'
    wb.sheets[sheetName].range("A1").options(index=False).value = violationsDF    