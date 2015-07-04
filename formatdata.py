__author__ = 'rohan'
from pandas import Series
import pandas as pd
import numpy as np
import time

pd.set_option('precision', 9)
#pd.set_option('chop_threshold', 0)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 50000)


def make_dataframe(json_object, dataframe_parameters = {}):
    datetime = str(time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))
    dfAsk = pd.DataFrame.from_dict(json_object[dataframe_parameters['ask_array']], dtype = np.float64)
    dfAsk['Total'] = Series(dfAsk[dataframe_parameters['price_column']].astype(np.float) * dfAsk[dataframe_parameters['quantity_column']].astype(np.float), index = dfAsk.index)
    dfAsk['Exchange'] = Series(dataframe_parameters['name'], index = dfAsk.index)
    dfAsk['DateTime'] = Series(datetime, index = dfAsk.index)
    dfAsk.rename(columns = {dataframe_parameters['price_column']:'Price', dataframe_parameters['quantity_column']:'Size'}, inplace = True)
    if dataframe_parameters['name'] == 'Kraken' and dataframe_parameters['pair'] == 'XXBTXLTC':
        dfAsk['Price'] = Series(1/dfAsk['Price'].astype(np.float), index = dfAsk.index)
    columns = ['Exchange', 'DateTime','Price', 'Size', 'Total']
    dfAsk = dfAsk[columns]

    dfBid = pd.DataFrame.from_dict(json_object[dataframe_parameters['bids_array']], dtype = np.float64)
    if dataframe_parameters['name'] == 'Cryptsy':
        dataframe_parameters['price_column'] = 'buyprice'
    dfBid['Total'] = Series(dfBid[dataframe_parameters['price_column']].astype(np.float) * dfBid[dataframe_parameters['quantity_column']].astype(np.float), index = dfBid.index)
    dfBid['Exchange'] = Series(dataframe_parameters['name'], index = dfBid.index)
    dfBid['DateTime'] = Series(datetime, index = dfBid.index)
    dfBid.rename(columns = {dataframe_parameters['price_column']:'Price', dataframe_parameters['quantity_column']:'Size'}, inplace = True)
    if dataframe_parameters['name'] == 'Kraken' and dataframe_parameters['pair'] == 'XXBTXLTC':
        dfBid['Price'] = Series(1/dfBid['Price'].astype(np.float), index = dfBid.index)
    columns = ['Exchange', 'DateTime','Price', 'Size', 'Total']
    dfBid = dfBid[columns]

    return dfAsk, dfBid