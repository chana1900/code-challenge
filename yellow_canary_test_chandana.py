import pandas as pd
import os
from datetime import datetime, timedelta

#######################################################
# NOTE:
# 1. print is used for simplicity; logging should be 
#    used in a prod environment with suitable levels
#    such as Debug, Info, Warning, Error and Critical
#
# 2. there is a typo in column name 'ote_treament' in 
#    the source file(PayCodes tab), same name is used 
#    regardless.
#######################################################

### prompt to read the excel file
def read_from_file():
    try:
        filename = input("Enter the path and filename: ")
        
        if not filename:
            # default path
            filename = "/Users/chandanananayakkara/Documents/python-projects/yellow_canary/Sample_Super_Data.xlsx"
        
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File not found: {filename}")

        # Read data from Excel file
        xls = pd.ExcelFile(filename)
        
        return xls
    
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        exit(1)
    
    except Exception as e:
        print(f"ERROR: An unexpectd error occured while reading file: {e}")
        exit(1)

### load data in excel file into data frames
def load_data_from_file():
    try:
        # read data from Excel file
        xls = read_from_file()
        
        # load data into data frames
        disbursement_data = pd.read_excel(xls, 'Disbursements')
        payslips_data = pd.read_excel(xls, 'Payslips')
        paycodes_data = pd.read_excel(xls, 'PayCodes')

        # Check for empty worksheets
        if disbursement_data.empty:
            raise ValueError("Disbursements worksheet is empty")
        if payslips_data.empty:
            raise ValueError("Payslips worksheet is empty")
        if paycodes_data.empty:
            raise ValueError("PayCodes worksheet is empty")

        return disbursement_data, payslips_data, paycodes_data
    
    except pd.errors.EmptyDataError:
        print("ERROR: All worksheets are empty. Please check your Excel file.")
        exit(1)

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while loading data: {e}")
        exit(1)

### calculate super percentage OTE payments
def calculate_super(amount):
    super_percentage = 0.095 # 9.5% super
    return amount * super_percentage

### find the quater which belongs to the disbursement payment
def determine_quarter(disbursement_date):
    try:    
        quarters = {
            1: (1, 29, 4, 28),
            2: (4, 29, 7, 28),
            3: (7, 29, 10, 28),
            4: (10, 28, 1, 28)
        }
        for qtr, (start_month,start_day,end_month,end_day) in quarters.items():
            year = disbursement_date.year
            q_year = year
            
            # disbursements made from Jan 1st to Jan 28 belongs to previous year qtr 4 
            if disbursement_date.month == 1 and disbursement_date.day <= 28 and end_month == 1:
                year -= 1
                q_year = year
            
            start_date = datetime(year ,start_month,start_day).date()
            
            year = disbursement_date.year
            if end_month == 1 :
                year += 1
            end_date   = datetime(year, end_month,end_day).date()
            
            if start_date <= disbursement_date <= end_date:
                qtr = f"{q_year}Q{qtr}"
                return qtr

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while determining quarter: {e}")
        exit(1)

### Calculate super payable by joining payslips and paycodes and filtered for 'OTE'
def calculate_super_payable(payslips_df, paycodes_df):
    try:
        # Convert 'pay_code' column to uppercase and remove white spaces for consistency
        paycodes_df['pay_code'] = paycodes_df['pay_code'].str.upper().str.strip()
        payslips_df['code'] = payslips_df['code'].str.upper().str.strip()

        # find qtr for payments made and convert to str type
        payslips_df['y_qtr'] = payslips_df['end'].dt.to_period('Q').astype(str)

        # Join Payslips with PayCodes on 'code'
        super_payable_merged_df = pd.merge(payslips_df, paycodes_df, how='left', left_on='code', right_on='pay_code')

        # check and WARN if ote_treatment is NULL
        super_payable_empty_ote =   super_payable_merged_df[super_payable_merged_df['ote_treament'].isnull()]
        if not super_payable_empty_ote.empty:
            print(f"\nWarning : The following records do not have a matching OTE\n {super_payable_empty_ote}")

        #calculate total OTE, filtered by OTE
        super_payable_group_df = super_payable_merged_df.query("ote_treament == 'OTE'").groupby(["employee_code","y_qtr"]).agg(total_ote=('amount','sum'))
        super_payable_group_df['total_super_payable'] = super_payable_group_df['total_ote'].apply(calculate_super)
        
        return super_payable_group_df

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while calculating super payable: {e}")
        exit(1)

### Calculate disbursements for employees by quarter
def calculate_disbursements(disbursements_df):
    try:
        # payment_made is converted to date type
        payment_made_date = pd.to_datetime(disbursements_df['payment_made']).dt.date
        
        # resolve quarter for disbursements
        disbursements_df['y_qtr'] = payment_made_date.apply(determine_quarter)
        
        # calculate the total disbursed amount for each quarter
        disbursement_group_df = disbursements_df.groupby(["employee_code","y_qtr"]).agg(total_disbursed=('sgc_amount','sum'))

        return disbursement_group_df

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while calculating disbursements: {e}")
        exit(1)

### calculate variance between super payable and disbursed amount
def calculate_variance(super_payable_df, disbursed_df):
    try:
        # join super payable and disbursed data frames on employee_code and y_qtr
        final_merged_df = pd.merge(super_payable_df, disbursed_df, how='left', left_on=['employee_code','y_qtr'], right_on=['employee_code','y_qtr']).fillna(0)
        
        # find the difference between total super paid and total disbursed 
        final_merged_df['variance'] = final_merged_df['total_super_payable'] - final_merged_df['total_disbursed']

        return final_merged_df

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while calculating variance: {e}")
        exit(1)

def execute_pipeline():
    
    # load data from file
    disbursement_data, payslips_data, paycodes_data = load_data_from_file()

    # get super payable
    super_payable_df = calculate_super_payable(payslips_data,paycodes_data)

    # get disbursements
    disbursed_df = calculate_disbursements(disbursement_data)
    
    # get variance calculation 
    variance_df = calculate_variance(super_payable_df, disbursed_df)   

    # format float to 2 decimals for display purpose
    pd.options.display.float_format = '{:.2f}'.format
    
    # display employee_code, quarter, total_ote, total_super_payable, total_disbursed and variance
    print(f"\nResults:\n {variance_df}")

    
if __name__ == "__main__":
    execute_pipeline()