import datetime

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def run_cohort_analysis(file: str, start_time: datetime, num_weeks: int):
    spark: SparkSession = SparkSession.builder.appName('cohort_analysis').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    weekly_cohorts = spark.read \
        .option('inferSchema', 'true') \
        .option('sep', '\t') \
        .option('header', 'true') \
        .csv(file)

    weekly_cohorts = weekly_cohorts.select('timestamp', 'custID')

    weekly_cohorts = weekly_cohorts.where(F.col('timestamp') >= start_time)

    weekly_cohorts = weekly_cohorts.withColumn('first_week',
                                               F.date_sub(F.next_day('timestamp', 'Monday'), 7))

    partition_func = Window.partitionBy(['custID']).orderBy(F.col('first_week').asc())

    weekly_cohorts = weekly_cohorts.withColumn('first_purchase_date',
                                               F.min('timestamp').over(partition_func))

    weekly_cohorts = weekly_cohorts.withColumn('returning_weekly_purchase',
                                               F.floor(F.datediff('timestamp', 'first_purchase_date') / 7 + 1))
    weekly_cohorts = weekly_cohorts.where(F.col('returning_weekly_purchase') <= num_weeks)

    # weekly_cohorts.show(weekly_cohorts.count(), truncate=False)

    weekly_cohorts = weekly_cohorts \
        .groupby(['first_week', 'returning_weekly_purchase']) \
        .agg(F.count('custID').alias('num_customers'))

    # weekly_cohorts.show(weekly_cohorts.count(), truncate=False)

    weekly_cohorts = weekly_cohorts \
        .groupby(['first_week']) \
        .pivot('returning_weekly_purchase') \
        .agg(F.first('num_customers')) \
        .orderBy(['first_week'])

    weekly_cohorts.show(weekly_cohorts.count(), truncate=False)

    # weekly_cohorts.coalesce(1).write.mode('overwrite')\
    #     .options(header='True', delimiter='\t')\
    #     .csv("/Users/spatra/PycharmProjects/pipeline-dbr-utils/main/cohort_analysis/cohort_output")

    ## SQL way - WIP
    # weekly_cohorts.registerTempTable('cohort')
    # weekly_cohorts = spark.sql("""
    #    select cohort.timestamp, count(distinct cohort.custID) as active_users,
    #    count(distinct future_cohort.custID) as retained_users,
    #    count(distinct future_cohort.custID)/count(distinct cohort.custID) as retention
    #    from cohort
    #    left join cohort as future_cohort
    #        on cohort.custID = future_cohort.custID
    #        and cohort.timestamp = future_cohort.timestamp - interval '1 week'
    #    group by cohort.timestamp
    #    """)
    # weekly_cohorts.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    run_cohort_analysis('/Users/spatra/PycharmProjects/pipeline-dbr-utils/main/cohort_analysis/dataset.tsv',
                        "2018-04-07 7:07:17", 2)
