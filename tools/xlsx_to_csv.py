import pandas as pd
import sys, os
env = os.path.dirname(os.path.abspath(__file__))

def xlsx_to_csv(xlsx_file, csv_file):
    # Read the Excel file into a Pandas DataFrame
    df = pd.read_excel(xlsx_file)
    
    # Save the DataFrame to CSV format
    df.to_csv(csv_file, index=False)

xlsx_to_csv(f'{env}/Exam Details.xlsx', f'{env}/Exam_Details.csv')