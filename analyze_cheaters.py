from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def get_avg_kill_ratio(kills, deaths):
    """Calculates the average kill ratio of each player.

    Calculates the kill ratio of each player on a daily basis and get the average of the measurements. 
    The average kill ratio per day is the number of kills divided by the sum of kills and deaths.

    Args:
        kills: A Spark DataFrame that has killings done by players.
        deaths: A Spark DataFrame that has deaths of players.

    Returns:
        avg_kill_ratio: A Pandas DataFrame with the values of average kill ratio.

        If a player from the 'kills' argument did not kill anyone,
        the value of average kill ratio of that player is zero.
    """
    kills_per_day = spark.sql("""SELECT src AS id, m_date, COUNT(*) AS num_of_kills 
                                 FROM kills GROUP BY src, m_date""")
    kills_per_day_df = kills_per_day.toPandas()

    deaths_per_day = spark.sql("""SELECT dst AS id, m_date, COUNT(*) AS num_of_deaths 
                                  FROM deaths GROUP BY dst, m_date""")
    deaths_per_day_df = deaths_per_day.toPandas()

    dates_from_kills = kills_per_day_df[['id', 'm_date']]
    dates_from_deaths = deaths_per_day_df[['id', 'm_date']]
    dates = pd.concat([dates_from_kills, dates_from_deaths])
    dates = dates.drop_duplicates(subset=['id', 'm_date'])

    temp = pd.merge(dates, kills_per_day_df, how='outer', on=['id', 'm_date'])
    temp = temp.fillna(0)
    merged_table = pd.merge(temp, deaths_per_day_df, how='outer', on=['id', 'm_date'])
    merged_table = merged_table.fillna(0)
    
    merged_table['kill_ratio'] = merged_table['num_of_kills'] / (merged_table['num_of_kills'] + merged_table['num_of_deaths'])
    avg_kill_ratio = merged_table[['id', 'kill_ratio']].groupby(['id'], as_index=False).mean()
    
    avg_kill_ratio.columns = ['id', 'avg_kill_ratio']
    avg_kill_ratio['avg_kill_ratio'] = avg_kill_ratio['avg_kill_ratio'].round(4)

    return avg_kill_ratio


def get_avg_time_diff_between_kills(kills):
    """Gets time differences between two consecutive killings and the average value for each player.

    Args:
        kills: A Spark DataFrame that has killings done by players.

    Returns:
        avg_kill_interval: A Pandas DataFrame with the values of average time difference 
            between kills.

        If a player from the 'kills' argument killed less than two players,
        that row will not be found in the returned table.
    """
    add_prev_kill_times = spark.sql("""SELECT mid, src, UNIX_TIMESTAMP(time) AS time, 
                                       UNIX_TIMESTAMP(LAG(time, 1) OVER (PARTITION BY mid, src ORDER BY time)) AS prev_time 
                                       FROM kills ORDER BY mid, src""")
    add_prev_kill_times.registerTempTable("add_prev_kill_times")

    add_time_diffs = spark.sql("""SELECT mid, src, time, (time - prev_time) AS time_diff 
                                  FROM add_prev_kill_times ORDER BY src, mid, time""")
    add_time_diffs.registerTempTable("add_time_diffs")

    avg_time_diffs = spark.sql("""SELECT src AS id, AVG(time_diff) AS delta  
                                  FROM add_time_diffs WHERE time_diff IS NOT NULL 
                                  GROUP BY src""")
    avg_kill_interval = avg_time_diffs.toPandas()
    avg_kill_interval['delta'] = avg_kill_interval['delta'].round(4)

    return avg_kill_interval

