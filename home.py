from dataloading import create_dataframe
import logs.logfn as log

data_path='data/Rider-Info.csv'

try:
    food_del = create_dataframe(data_path, True)
    food_del.show()
    print('dataframe created successfully')
    log.log_info('dataframe created successfully')

except Exception as e:
    print('error while creating table', e)
    log.log_error(e)

from analytics.file1 import peak_hrs
try:
    result = peak_hrs(food_del)
    result.show(n=24)
    result.write.csv('result/analytics1',header=True,mode='overwrite')
    log.log_info('analytics1 file is executed and output is generated')
except Exception as e:
    log.log_error(e)

from analytics.file2 import evening_riders
try:
    result = evening_riders(food_del)
    result.show()
    result.write.csv('result/analytics2', header=True, mode='overwrite')
    log.log_info('analytics2 file is executed and output is generated')
except Exception as e:
    log.log_error(e)










