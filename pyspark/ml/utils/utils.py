from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, ArrayType, BooleanType, StringType, StructType, StructField
from statsmodels.tsa.statespace.sarimax import SARIMAX
from pmdarima.arima import auto_arima
import pandas as pd
import sys
import collections

spark = SparkSession.builder.appName("battery-wear").enableHiveSupport().getOrCreate()

class PysparkUtils:
    def __init__(self, val1, val2):
        self.val1 = val1
        self.val2 = val2

    def extract_list(array_struct):

        returnVal = [i[1] for i in array_struct if i[1] is not None]
        return returnVal

    @staticmethod
    def t_test(list_a, list_b):
        from scipy.stats import ttest_ind

        t_stat, p = ttest_ind(list_a, list_b, alternative='less')

        return (str(t_stat), str(p))


    @staticmethod
    def mode(list):
        counter = collections.Counter(list)
        returnVal = counter.most_common(1)
        return returnVal[0][0]

    # Checking continuous sequence of values greater than threshold limit
    @staticmethod
    def seq_check(list, threshold=(50, 5)):
        cnt = 0
        returnVal = False
        for i in list:
            if cnt == threshold:
                break
            elif i < 70:
                cnt = cnt + 1
            elif (i >= threshold[0]) & (cnt < threshold[1]):
                cnt = 0

        if cnt == 5:
            returnVal = True
        else:
            returnVal = False
        return returnVal
    ### function for creating arima model for pyspark
    def arima_modeling(list, dt_list):
        returnVal = []
        ord = ''
        aic = 0


        data = pd.DataFrame({'list': list, 'dt': dt_list})
        data.set_index('dt', inplace=True)
        data.index.freq = 'D'

        data_model_params = auto_arima(data, start_p=0, d=1, start_q=0,
                                              max_p=5, max_d=5, max_q=5, start_P=0,
                                              D=1, start_Q=0, max_P=5, max_D=5,
                                              max_Q=5, m=6, seasonal=False,
                                              error_action='warn', trace=False,
                                              supress_warnings=True, stepwise=True,
                                              random_state=20, n_fits=50)

        m = data_model_params.get_params()
        ord = m.pop('order')
        if (ord == (0, 0, 0)):
            ord = (0, 1, 0)

        aic = data_model_params.aic()
        data_model = SARIMAX(data, order=ord, seasonal_order=(0, 0, 0, 0))
        results = data_model.fit()
        prediction = pd.DataFrame(
            results.predict(start=len(data), end=len(data) + 30).rename('predicted_fcc'))
        # prediction['output'] = prediction['predicted_fcc'].round(2).apply(str) + "|" + prediction['dt'].apply(str)
        prediction['output'] = prediction['predicted_fcc'].round(2)
        returnVal = prediction['output'].to_list()

        return str(ord), float(aic), returnVal

udf_extract_list = udf(PysparkUtils.extract_list, ArrayType(FloatType()))

udf_t_test = udf(PysparkUtils.t_test, StructType(
    [StructField('t_stat', StringType()), StructField('p_value', StringType())
     ]))

udf_mode = udf(PysparkUtils.mode, FloatType())
udf_seq_check = udf(PysparkUtils.seq_check, BooleanType())


udf_arima_modeling = udf(PysparkUtils.arima_modeling, StructType(
    [StructField('model_params', StringType()), StructField('aic', FloatType()),
     StructField('predictions', ArrayType(FloatType()))]))

