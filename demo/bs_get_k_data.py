import pandas as pd
import baostock as bs

# This method can request history stock data from BaoStock by stock code
# Parameters: Code: 'stock code' in format sz.xxxxxx or sh.xxxxxx, if empty default is sz.000001
#             Start_date: 'YYYY-MM-DD', if empty default is 2015-01-01
#             End_date: 'YYYY-MM-DD', if empty default is the most recent trading day
#             Frequency: data interval, 'd' = day, 'w' = week, 'm' = month, '5' = 5 minutes, '15' = 15 minutes, '30' = 30 minutes, '60' = 60 minutes
#             Adjust: '3' no adjust flag, '2' split adjust

def bs_k_data_request(Code = 'sz.000001', Start_date = '2015-01-01', End_date, Frequency = 'd', Adjust = 3 ):
    # login to BaoStock
    lg = bs.login()

    # Set the fields required, can be set as "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
    fields = "date,open,high,low,close,volume"
    
    # query_history_k_data_plus('Stock Code', "fields", 'start_date', 'end_date', 'frequency', 'adjustflag'): BaoStock API to get history K data
    df_bs = bs.query_history_k_data_plus(Code, fields, start_date = Start_date, end_date = End_date, frequency = Frequency, adjustflag = Adjust)

    data_list = []
    
    # Keep append to data_list while no error occurs and has next
    while (df_bs.error_code == '0') & df_bs.next():
        data_list.append(df_bs.get_row_data())
    
    # Set pandas dataframe, data as data_list, column as field
    result = pd.DataFrame(data_list, column = df_bs.fields)

    # Set data type for each column
    result.close = result.close.astype('float64')
    result.open = result.open.astype('float64')
    result.low = result.low.astype('float64')
    result.high = result.high.astype('float64')
    result.volume = result.volume.astype('float64')
    result.volume = result.volume/100
    result.date = pd.DatetimeIndex(result.date)
    result.set_index("date", drop = True, inplace = True)
    result.index = result.index.set_names('Date')
    
    # Format data
    recon_data = {'High': result.high, 'Low': result.low, 'Open': result.open, 'Close': result.close, 'Volume': result.volume}
    df_recon = pd.DataFrame(recon_data)
    
    # System logout
    bs.logout()
    
    # Return type is pd.DataFrame
    return df_recon 


