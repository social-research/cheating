from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def get_avg_kill_ratio(kills, deaths):
    """This function calculates the average kill ratio for each player.
       Args:
           kills: Dataframe that contains kill records of players
           deaths: Dataframe that contains death records of players
       Returns:
           avg_kill_ratio: Dataframe that contains the values of overall average kill ratio for each player
    """
    kills_by_date = spark.sql("""SELECT src AS id, m_date, COUNT(*) AS num_of_kills 
                                 FROM kills GROUP BY src, m_date""")
    kills_by_date_df = kills_by_date.toPandas()

    deaths_by_date = spark.sql("""SELECT dst AS id, m_date, COUNT(*) AS num_of_deaths 
                                  FROM deaths GROUP BY dst, m_date""")
    deaths_by_date_df = deaths_by_date.toPandas()

    dates_from_kills = kills_by_date_df[['id', 'm_date']]
    dates_from_deaths = deaths_by_date_df[['id', 'm_date']]
    dates = pd.concat([dates_from_kills, dates_from_deaths])
    dates = dates.drop_duplicates(subset=['id', 'm_date'])

    temp = pd.merge(dates, kills_by_date_df, how='outer', on=['id', 'm_date'])
    temp = temp.fillna(0)
    merged_table = pd.merge(temp, deaths_by_date_df, how='outer', on=['id', 'm_date'])
    merged_table = merged_table.fillna(0)

    merged_table['kill_ratio'] = merged_table['num_of_kills'] / (merged_table['num_of_kills'] + merged_table['num_of_deaths'])
    avg_kill_ratio = merged_table[['id', 'kill_ratio']].groupby(['id'], as_index=False).mean()
    avg_kill_ratio.columns = ['id', 'avg_kill_ratio']

    return avg_kill_ratio


def get_avg_time_diff_between_kills(kills):
    """This function calculates the average time difference between consecutive kills for each player.
       Args:
           kills: Dataframe that contains kill records of players
       Returns:
           avg_kill_intervals_df: Dataframe that contains values of overall average time difference 
                                  between kills for each player
    """
    add_prev_kill_times = spark.sql("""SELECT mid, src, UNIX_TIMESTAMP(time) AS time, 
                                       UNIX_TIMESTAMP(LAG(time, 1) OVER (PARTITION BY mid, src ORDER BY time)) 
                                       AS prev_time FROM kills ORDER BY mid, src""")
    add_prev_kill_times.registerTempTable("add_prev_kill_times")

    add_time_diffs = spark.sql("""SELECT mid, src, time, (time - prev_time) AS time_diff 
                                  FROM add_prev_kill_times ORDER BY src, mid, time""")
    add_time_diffs.registerTempTable("add_time_diffs")

    avg_kill_intervals = spark.sql("""SELECT src AS id, AVG(time_diff) AS delta  
                                      FROM add_time_diffs WHERE time_diff IS NOT NULL 
                                      GROUP BY src""")
    avg_kill_intervals_df = avg_kill_intervals.toPandas()

    return avg_kill_intervals_df

