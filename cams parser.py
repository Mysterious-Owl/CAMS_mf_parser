import pandas as pd
import pdfplumber
import re
from os import path
import numpy as np


def file_processing(file_path, doc_pwd, txt_file):
    final_text = ""
    with pdfplumber.open(file_path, password=doc_pwd) as pdf:
        for i in range(len(pdf.pages)):
            txt = pdf.pages[i].extract_text()
            final_text = final_text + "\n" + txt
        pdf.close()
    with open(txt_file, 'w') as f:
        f.write(final_text)
    return final_text


def extract_text(txt_file, final_csv):
    with open(txt_file, 'r') as f:
        doc_txt = f.read()

    # Defining RegEx patterns
    folio_pat = re.compile(r"(^Folio No:\s\d+?)", flags=re.IGNORECASE)
    # Extracting Folio information
    fund_name = re.compile(r".*[Fund].*ISIN.*", flags=re.IGNORECASE)
    trans_details = re.compile(
        r"(^\d{2}-\w{3}-\d{4})(\s.+?\s(?=[\d(]))([\d(]+[,.]\d+[.\d)]+)(\s[\d(,.)]+)(\s[\d,.]+)(\s[\d,.]+)"
    )

    # Extracting Transaction data
    line_items = []
    fun_name = folio = ""
    for i in doc_txt.splitlines():
        if fund_name.match(i):
            fun_name = i

        if folio_pat.match(i):
            folio = i

        txt = trans_details.search(i)
        if txt:
            line_items.append({
                "Folio": folio,
                "Fund_name": fun_name,
                "Date": txt.group(1),
                "Remarks": txt.group(2),
                "Amount": txt.group(3),
                "Units": txt.group(4),
                "Price": txt.group(5),
                "Unit_balance": txt.group(6)
            })

    df = pd.DataFrame(line_items)

    df = formatter(df)
    save_data(df, final_csv)
    # return df
    # df.to_csv(final_csv, index=False)


def save_data(df, final_csv):
    if path.isfile(final_csv):
        old_df = pd.read_csv(final_csv)
        old_df['Date'] = pd.to_datetime(old_df['Date'])
        min_date = df['Date'].min()
        old_df = old_df[old_df['Date'] < min_date]
        df = pd.concat([old_df, df])
    df.sort_values('Date', ascending=False).to_csv(final_csv, index=False)


def formatter(df):
    def clean_txt(x: pd.Series):
        x.replace(r",", "", regex=True, inplace=True)
        x.replace(r"\(", "-", regex=True, inplace=True)
        x.replace(r"\)", " ", regex=True, inplace=True)
        return x

    def name_cleaner(x: str):
        x = re.findall(r'-(.+)isin', x)[0]
        len_con = lambda i: len(x) if i == -1 else i
        keywords = ['direct', 'growth', 'growth plan']
        indexs = [len_con(x.rfind(i)) for i in keywords]
        x = x[:min(indexs)]
        while x[-1] in (' ', '-', ' '):
            x = x[:-1]
        x = x.title()
        to_capitalize = ['Bse', 'Fof', 'Us', 'Sbi']
        for i in to_capitalize:
            x = x.replace(i, i.upper())

        return x

    invst_type_mapper = {
        '.*sys.*': 'SIP',
        '.*redemption.*': 'Redemption',
        '.*purchase.*': 'Lumpsum'
    }
    fund_type_mapper = {
        '.*idcw.*': 'IDCW',
        '.*growth.*': 'Growth'
    }
    invst_channel_mapper = {
        '.*direct.*': 'Direct',
        '.*regular.*': 'Regular',
    }
    amc_mapper = {
        '.*axis.*': 'Axis',
        '.*sbi.*': 'SBI',
        '.*nippon.*': 'Nippon',
        '.*quant.*': 'Quant',
        '.*bharat.*': 'Edelweiss',
        '.*edelweiss.*': 'Edelweiss',
        '.*aditya.*': 'Aditya Birla',
        '.*parag.*': 'Parag Parikh',
        '.*uti.*': 'UTI',
    }
    clean_txt(df.Amount)
    clean_txt(df.Units)
    clean_txt(df.Price)
    clean_txt(df.Unit_balance)
    df = df.astype({
        "Amount": "float",
        "Units": "float",
        "Price": "float",
        "Unit_balance": "float"
    })
    df['Date'] = pd.to_datetime(df['Date'])
    df['Name'] = df['Fund_name'].str.lower().apply(name_cleaner)
    df['Investment Type'] = df.Remarks.str.lower().replace(invst_type_mapper, regex=True)
    df['Fund Type'] = df.Fund_name.str.lower().replace(fund_type_mapper, regex=True)
    df.loc[df['Fund_name'].str.lower() == df['Fund Type'], 'Fund Type'] = 'Growth'
    df['Investment Channel'] = df.Fund_name.str.lower().replace(invst_channel_mapper, regex=True)
    df.loc[df['Fund_name'].str.lower() == df['Investment Channel'], 'Investment Channel'] = 'Direct'
    df['Folio No'] = df['Folio'].str.extract(r'Folio No: (\d*) ')
    df['ISIN'] = df['Fund_name'].str.extract(r'ISIN[ :]+(\w+)[( ]')
    df['Advisor'] = df['Fund_name'].str.extract(r'Advisor[ :]+(\w+)[( )]')
    df.loc[df['Advisor'].str.lower() == 'registrar', 'Advisor'] = np.NAN
    df['AMC'] = df.Fund_name.str.lower().replace(amc_mapper, regex=True)

    df.drop(['Folio', 'Fund_name'], axis=1, inplace=True)
    df = df[['Name', 'Date', 'Amount', 'Units', 'Price', 'Unit_balance', 'Investment Type', 'Fund Type',
             'Investment Channel', 'Folio No', 'ISIN', 'Advisor', 'AMC', 'Remarks']]

    return df


cams = r"C:/Users/.pdf"
cams_pwd = "pass"
cams_txt = r"C:/Users/.txt"
cams_csv = r"C:/Users/.csv"

file_processing(cams, cams_pwd, cams_txt)
print("extracted text")
extract_text(cams_txt, cams_csv)
