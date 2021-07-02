# GLOBAL IMPORTS: 

import requests
import pandas
import os
import datetime
import numpy as np 

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import mimetypes



# GLOBAL VARS: 

NSE_URL1 = 'https://www1.nseindia.com/products/content/equities/equities/eq_security.htm'

HEADER_REQ1 = {
"Host" : "www1.nseindia.com",
"Connection" : "keep-alive",
"sec-ch-ua":   "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\"",
"sec-ch-ua-mobile": "?0",
"Upgrade-Insecure-Requests" : "1",
"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
"Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Sec-Fetch-Site" : "none",
"Sec-Fetch-Mode" : "navigate",
"Sec-Fetch-User" : "?1",
"Sec-Fetch-Dest" : "document",
"Accept-Encoding" : "gzip, deflate, br",
"Accept-Language" : "en-US,en;q=0.9"
}

DEBUG = True 
INCLUDE_NORMAL_ACTIVITY = True



def iqr_Anomaly_Upper(datalist,sample):
    if (datalist is None):
        return -1
    # convert to Numpy array. Easy to apply qunatile funtion 
    datalist = np.array(datalist)
    
    Q3, Q1 = np.percentile(datalist, [75 ,25])
    IQR = Q3 - Q1
    
    IQR_ANOMALY_UPPER_THRESHOLD = Q3 + 1.5 * IQR

    if(sample>=IQR_ANOMALY_UPPER_THRESHOLD):
        return 1  # Sample shows high volume activity 
    else:
        return 0  # Sample is normal volume activity 

# Format of the result row: 
# Stockname, Trading Verdit, Delivery Verdict
# ICICIBANK,ABNOMALLY HIGH TRADING,ABNOMALLY HIGH DELIVERY
# DMART,NORMAL TRADING,NOMAL DELIVERY
# DMART,HIGH TRADING,HIGH DELIVERY
# Thresholds = {NORMAL, HIGH, ABNOMALLY HIGH}
def iqrAnomalyCheck(stockname,date,df):

    if not isinstance(df, pandas.DataFrame):
        return str(stockname+","+date+",INVALID DATA,INVALID DATA\n")
    if (df is None):
        return str(stockname+","+date+",NULL DATA,NULL DATA\n")
    if (len(df.index)<7): 
        return str(stockname+","+date+",TOO LESS DATA,TOO LESS DATA\n")
    #Algorithm for checking {NORMAL, HIGH, ABNOMALLY HIGH}
    
    Trade_Volume_Latest_Sample = df["Total Traded Quantity"].to_numpy()[-1]
    Trade_Delivery_Latest_Sample = df["DeliverableQty"].to_numpy()[-1]
    
    Trade_Volume = df["Total Traded Quantity"].to_numpy()[:-1]
    Trade_Delivery = df["DeliverableQty"].to_numpy()[:-1]
    
    Q3_TV, Q1_TV = np.percentile(Trade_Volume, [75 ,25])
    TV_IQR_ANOMALY_UPPER_THRESHOLD = Q3_TV + 1.5 * (Q3_TV - Q1_TV) # UPPER HARD THRESHOLD for Trade Volume
    TV_N95_UPPER_THRESHOLD =  np.percentile(Trade_Volume, 95) # UPPER SOFT THRESHOLD for Trade Volume
    
    
    Q3_TD, Q1_TD = np.percentile(Trade_Delivery, [75 ,25])
    TD_IQR_ANOMALY_UPPER_THRESHOLD = Q3_TD + 1.5 * (Q3_TD - Q1_TD) # UPPER HARD THRESHOLD for Trade Volume
    TD_N95_UPPER_THRESHOLD =  np.percentile(Trade_Delivery, 95) # UPPER SOFT THRESHOLD for Trade Volume
    
    # Check Thresholds for Trading: 
    Trade_Volume_Outcome = "NOMAL TRADING"
    Trade_Delivery_Outcome = "NOMAL DELIVERY"
    
    if (Trade_Volume_Latest_Sample>=TV_IQR_ANOMALY_UPPER_THRESHOLD): 
        Trade_Volume_Outcome = "ABNOMALLY HIGH TRADING"
    elif (Trade_Volume_Latest_Sample>=TD_N95_UPPER_THRESHOLD):
        Trade_Volume_Outcome = "HIGH TRADING"
    else:
        Trade_Volume_Outcome = "NORMAL TRADING"
        
    # Check Thresholds for Delivery: 
    if (Trade_Delivery_Latest_Sample>=TD_IQR_ANOMALY_UPPER_THRESHOLD): 
        Trade_Delivery_Outcome = "ABNOMALLY HIGH DELIVERY"
    elif (Trade_Delivery_Latest_Sample>=TD_N95_UPPER_THRESHOLD):
        Trade_Delivery_Outcome = "HIGH DELIVERY"
    else:
        Trade_Delivery_Outcome = "NORMAL DELIVERY"

    return str(stockname+","+date+","+Trade_Volume_Outcome+","+Trade_Delivery_Outcome+"\n")
    
    
    



def fetchDataFromNSE(_symbol_list,_lastndays=21): 
    
    
    date_past = str(datetime.date.today() - pandas.offsets.DateOffset(days=_lastndays)).split(" ")[0]
    date_today  = str(datetime.date.today() - pandas.offsets.DateOffset(days=0)).split(" ")[0]
    
    date_past_obj = datetime.datetime.strptime(date_past, '%Y-%m-%d')
    date_today_obj = datetime.datetime.strptime(date_today, '%Y-%m-%d')
    
    
    date_past = date_past_obj.strftime('%d-%m-%Y')
    date_today = date_today_obj.strftime('%d-%m-%Y')

    print("date from ", date_past, type(date_past))
    print("date today ",date_today, type(date_today))
    
    
    #_symbol = "DMART"
    _segmentLink = "3"
    _symbolCount = "1"
    _series = "EQ"
    _dateRange = "+"
    _fromDate = date_past
    _toDate = date_today
    _dataType = "PRICEVOLUMEDELIVERABLE"
    
 
    
    # For debugging
    #print(f"===Start of Params===")
    ##print(f"got symbol:{_symbol}")
    #print(f"got segmentLink:{_segmentLink}")
    #print(f"got symbolCount:{_symbolCount}")
    #print(f"got series:{_series}")
    #print(f"got dateRange:{_dateRange}")
    #print(f"got fromDate:{_fromDate}")
    #print(f"got toDate:{_toDate}")
    #print(f"got dataType:{_dataType}")
    #print(f"===End of Params===")
    
       
 
    sess = requests.Session()
    rs = sess.get(NSE_URL1, headers=HEADER_REQ1)
    
    arr_cookies = [{'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path, 'expires': c.expires} for c in sess.cookies]
    parsed_cookies = arr_cookies[0].get('name') + "=" + arr_cookies[0].get('value')
    
    custom_headers = {
            "Host" : "www1.nseindia.com",
            "Connection" : "keep-alive",
            "sec-ch-ua":   "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\"",
            "sec-ch-ua-mobile": "?0",
            "Upgrade-Insecure-Requests" : "1",
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Accept" : "*/*",
            "X-Requested-With" : "XMLHttpRequest",
            "Sec-Fetch-Site" : "same-origin",
            "Sec-Fetch-Mode" : "cors",
            "Sec-Fetch-Dest" : "empty" ,
            "Referer" : "https://www1.nseindia.com/products/content/equities/equities/eq_security.htm",
            "Accept-Encoding" : "gzip, deflate, br",
            "Accept-Language" : "en-US,en;q=0.9",
            "Cookie" : parsed_cookies}
    
 
    final_result_set = ""
    result_row = ""
    
    for _symbol in _symbol_list:
        
        print("-------------------------------------------------")
        print("Checking: Stock: "+_symbol)
 
        rs = requests.get("https://www1.nseindia.com//marketinfo/sym_map/symbolCount.jsp?symbol="+_symbol,headers=custom_headers)
        _symbolCount = str(rs.text).strip()
        
        #print("After calling symbolCount.jsp: symbolCount="+_symbolCount)
        
        custom_query_params = {
           'symbol': _symbol,
           'segmentLink': _segmentLink,
           'symbolCount': _symbolCount,
           'series': _series,
           'dateRange': _dateRange,
           'fromDate': _fromDate,
           'toDate': _toDate,
           'dataType': _dataType}  
 
        rs = requests.get("https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp", params=custom_query_params,headers=custom_headers)
        parsed_tables = pandas.read_html(rs.text)
        
        #valid_data_flag = True
        #if not ("<span><nobr>Data for" in rs.text): 
        #    valid_data_flag = False 
        #with open("C:\\CG505\\CG505_Programming\\nse-volume-api\\temp\\debug.txt", "a+") as f:
        #    f.write(rs.text+"\n\n\n--------------------")
        

        df = pandas.DataFrame(parsed_tables[0])
        
        #print("Colunm size: "+str(len(df.columns)) ) 
        
        
        time_stamp_file = _symbol +"_"+str(datetime.datetime.now()).replace(" ","-").replace(":","-").replace(".","-") + ".csv"
        
        df.to_csv("temp/"+time_stamp_file)
        
        
        cols_2 = ['Date', 'Total Traded Quantity', 'DeliverableQty']
        

        #print("#Columns in stock "+_symbol+" : "+str(len(df.columns)))
        
        if (len(df.columns)!=15): # Work on data if and only the data has 15 cols => Proper data
            #print("#Columns in stock "+_symbol+" is not equal to 15")
            result_row = _symbol+",NSE DATA ERROR,NSE DATA ERROR,NSE DATA ERROR\n"
            print("*** Result: "+_symbol+" : "+result_row)
        
        else:
            df2 = df[['Date', 'Total Traded Quantity', 'DeliverableQty']].copy()
            Latest_Date = df2["Date"].to_numpy()[-1]
            result_row = iqrAnomalyCheck(_symbol,Latest_Date,df2)
            print("*** Result: "+_symbol+" : "+result_row)
            
 

        final_result_set = final_result_set + result_row
 
    return final_result_set



def send_email(report_filename):    
    
    #The mail addresses and password
    sender_address = '<your-gmail-with-secure-feature-off>@gmail.com'
    sender_password = ''
    receiver_address = '<receivers email>@gmail.com'
    
    fileDir = os.path.dirname(os.path.realpath('__file__'))
    with open(os.path.join(fileDir, "secrets/password.txt"), "r") as f:
        sender_password = f.read()
          
 
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Stock Delivery and Trading Alert'   #The subject line
    
    mail_content = 'Daily Stock Delivery and Trading Alert' 
    
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    
    attachment = open(report_filename, "rb")
    
    # instance of MIMEBase and named as p
    #p = MIMEBase('application', 'octet-stream')
    p = MIMEBase('text', 'plain')
 
 
    # To change the payload into encoded form
    p.set_payload(attachment.read())
      
    # encode into base64
    encoders.encode_base64(p)
       
    p.add_header('Content-Disposition', "attachment; filename=StockDeliveryTrading.csv" )
      
    # attach the instance 'p' to instance 'msg'
    message.attach(p)
        

    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    
    session.starttls() #enable security
    
    session.login(sender_address, sender_password) #login with mail_id and password
    
    text = message.as_string()
    
    session.sendmail(sender_address, receiver_address, text)
    
    session.quit()
 
    
    print('Mail Sent')




def mainFunction():

    fileDir = os.path.dirname(os.path.realpath('__file__'))
    stock_list = []
    with open(os.path.join(fileDir, 'stock-list/stocklist.txt')) as f:
        stock_list = f.read().splitlines()
        
    final_report = fetchDataFromNSE(stock_list, 28)
    
    report_filename = "reports/"+str(datetime.datetime.now()).replace(" ","-").replace(":","-").replace(".","-") + ".csv"
    
    fileDir = os.path.dirname(os.path.realpath('__file__'))
     
    with open(os.path.join(fileDir, report_filename), "w+") as f:
         f.write("STOCK,DATE,TRADING,DELIVERY\n")
         f.write(final_report)
 
    print("\n-------------FINAL REPORT---------------\n")
    print(final_report)
    
    send_email(os.path.join(fileDir, report_filename))
    
# Call main 
mainFunction()