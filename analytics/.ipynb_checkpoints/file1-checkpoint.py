from pyspark.sql.functions import hour, count, concat_ws, col, lit, date_format

def peak_hrs(tablename):

    df = tablename.withColumn('hours', hour(tablename['order_time']))
    # df.select('order_time','hours').show()

    out = df.groupby('hours').agg(count('order_id').alias('no_of_orders'))
    # out.show()

    out = out.orderBy(out.no_of_orders.desc())
    # out.show()

    out = out.withColumn('formatted_hr', concat_ws(':', col('hours'), lit('00')))
    # out.show()
    df_12hr = out.withColumn('time_in_12hr', date_format(col('formatted_hr'), 'hh:mm a'))
    # return df_12hr

    df=tablename.withColumn