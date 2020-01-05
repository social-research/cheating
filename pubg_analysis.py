from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


### Functions that control files in the file system and creating datasets.


def clean_edges(table_name):
    """This function removes matches in special mode where players can revive multiple times 
       from telemetry data in the given table.
       Args:
           table_name: Name of a table that contains killings
       Returns:
           cleaned_logs: Kill records without matches in special mode
    """
    path_to_file = "s3://social-research-cheating/raw_text_files/" + table_name + "_edges.txt"
    
    # Define the structure of telemetry data.
    edgeSchema = StructType([StructField("mid", StringType(), True),
                             StructField("aid", StringType(), True),
                             StructField("src", StringType(), True),
                             StructField("dst", StringType(), True),
                             StructField("time", TimestampType(), True),
                             StructField("m_date", StringType(), True)])
    
    # Read edges from my S3 bucket and create a local table.
    edges = spark.read.options(header='false', delimiter='\t').schema(edgeSchema).csv(path_to_file)
    edges.registerTempTable("edges")
    
    # Remove matches in special mode where players revive multiple times.
    # Players should be killed only once as they are given only one life per match.
    # Compare the total number of victims and the number of unique victims to detect matches in special mode.
    spark.sql("""SELECT mid, COUNT(*) AS num_row, COUNT(DISTINCT dst) AS uniq_dst FROM edges 
                 GROUP BY mid""").createOrReplaceTempView("num_dst")

    # For each match, assign the value of zero if the match is in default mode 
    # and otherwise assign the value of one.
    spark.sql("""SELECT mid, num_row, uniq_dst, 
                 CASE WHEN num_row == uniq_dst THEN 0 ELSE 1 END AS spec_mod 
                 FROM num_dst""").createOrReplaceTempView("mod_tab")
    
    # Get match IDs in default mode.
    spark.sql("SELECT mid, num_row FROM mod_tab WHERE spec_mod = 0").createOrReplaceTempView("defalut_mods")
    
    # Get killings done during matches in default mode.
    cleaned_logs = spark.sql("""SELECT e.mid, src, dst, time, m_date 
                                FROM edges e JOIN defalut_mods d ON e.mid = d.mid""")
    
    return cleaned_logs


def combine_telemetry_data(day, num_of_files, PATH_TO_DATA):
    """This function combines all telemetry files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store kill records created on the given date
           PATH_TO_DATA: Path to raw data
    """
    if day == 1:
        # Create the first parquet file.
        cleaned_tab = clean_edges("td_day_1_1")
        cleaned_tab.write.parquet(PATH_TO_DATA)
        
        for i in range(2, num_of_files+1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(PATH_TO_DATA)
    else:
        for i in range(1, num_of_files+1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(PATH_TO_DATA)


def get_participants(table_name):
    """This function creates a tidy table that contains the team IDs of players 
       who took part in teamplay matches.
       Args:
           table_name: Name of a table that contains raw team membership data
       Returns:
           participants: Dataframe that contains the team IDs of players for all teamplay matches.
    """
    path_to_file = "s3://social-research-cheating/team-data/" + table_name + "_edges.txt"
    
    # Define the structure of team membership data.
    edgeSchema = StructType([StructField("mid", StringType(), True),
                             StructField("src", StringType(), True),
                             StructField("dst", StringType(), True),
                             StructField("tid", StringType(), True),
                             StructField("time", TimestampType(), True),
                             StructField("mod", StringType(), True),
                             StructField("rank", IntegerType(), True),
                             StructField("m_date", StringType(), True)])
    
    # Read edges from the S3 bucket and create a local table.
    data = spark.read.options(header='false', delimiter='\t').schema(edgeSchema).csv(path_to_file)
    data.registerTempTable("data")
    participants = spark.sql("""SELECT * FROM (SELECT mid, src AS id, tid FROM data 
                                UNION SELECT mid, dst, tid FROM data)""")
    
    return participants


def combine_team_data(day, num_of_files, PATH_TO_DATA):
    """This function combines all team membership data files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store team membership information 
                         created on the given date
           PATH_TO_DATA: Path to the data that contains team membership information
    """
    if day == 1:
        # Create the first parquet file.
        team_data = get_participants("md_day_1_1")
        team_data.write.parquet(PATH_TO_DATA)
        for i in range(2, num_of_files+1):
            new_team_data = get_participants("md_day_" + str(day) + "_" + str(i))
            new_team_data.write.mode("append").parquet(PATH_TO_DATA)
    else:
        for i in range(1, num_of_files+1):
            new_team_data = get_participants("md_day_" + str(day) + "_" + str(i))
            new_team_data.write.mode("append").parquet(PATH_TO_DATA)
            

def get_obs_data(PATH_TO_DATA, players):
    """This function creates a dataset (including self-loops) that contains the killings of matches 
       where cheaters killed at least one player and at least one player who later adopts cheating.
       Args:
           PATH_TO_DATA: Path to a raw dataset in the S3 bucket
           players: Table (dataframe) that contains player data
    """
    spark.read.parquet(PATH_TO_DATA).createOrReplaceTempView("raw_data")
    
    # Add more information about killers and victims.
    spark.sql("""SELECT mid, src, start_date AS src_sd, ban_date AS src_bd, cheating_flag AS src_flag, 
                 CASE WHEN m_date <= ban_date AND m_date >= start_date THEN 1 ELSE 0 END AS src_curr_flag, 
                 dst, time, m_date 
                 FROM raw_data r LEFT JOIN players p ON r.src = p.id""").createOrReplaceTempView("add_src_flags")
    
    spark.sql("""SELECT mid, src, src_sd, src_bd, src_flag, src_curr_flag,
                 dst, start_date AS dst_sd, ban_date AS dst_bd, cheating_flag AS dst_flag, 
                 CASE WHEN m_date <= ban_date AND m_date >= start_date THEN 1 ELSE 0 END AS dst_curr_flag,
                 time, m_date 
                 FROM add_src_flags a LEFT JOIN players p ON a.dst = p.id""").createOrReplaceTempView("edges")

    # Find matches where cheaters killed at least one player and at least one potential cheater exists.
    # For each match, the value of c_cnt should be zero if there is no one killed by cheating.
    # The value of pot_cheaters should be zero if there is no one (either killer or victim) who later adopts cheating.
    sum_tab = spark.sql("""SELECT mid, 
                           SUM(CASE WHEN src_curr_flag = 1 THEN 1 ELSE 0 END) AS c_cnt, 
                           (SUM(CASE WHEN src_curr_flag = 0 AND src_flag = 1 THEN 1 ELSE 0 END) + 
                            SUM(CASE WHEN dst_curr_flag = 0 AND dst_flag = 1 THEN 1 ELSE 0 END)) AS pot_cheaters 
                           FROM edges GROUP BY mid""")
    sum_tab.registerTempTable("sum_tab")
    # sum_tab.write.parquet("s3://social-research-cheating/general-stats/obs_sum_tab.parquet")
    
    spark.sql("SELECT mid FROM sum_tab WHERE c_cnt > 0 AND pot_cheaters > 0").createOrReplaceTempView("legit_mids")
    
    # Extract the records of matches where at least one cheater took part in.
    legit_logs = spark.sql("""SELECT e.mid, src, src_sd, src_bd, src_curr_flag, src_flag, 
                              dst, dst_sd, dst_bd, dst_curr_flag, dst_flag, time, m_date 
                              FROM edges e JOIN legit_mids l ON e.mid = l.mid""")
    legit_logs.write.parquet("s3://social-research-cheating/edges/obs_data.parquet")
    

### Functions that analyze cheaters and compare them with non-cheaters


def get_avg_kill_ratio(kill_logs, death_logs):
    """This function calculates the average kill ratio for each player.
       Args:
           kill_logs: Dataframe that contains kill records of players
           death_logs: Dataframe that contains death records of players
       Returns:
           avg_kill_ratio: Dataframe that contains the values of overall average kill ratio for each player
    """
    # Calculate the number of kills of each player by date.
    kills_by_date = spark.sql("""SELECT src AS id, m_date, COUNT(*) AS kills FROM kill_logs 
                                 GROUP BY src, m_date""")
    kills_by_date_df = kills_by_date.toPandas()

    # Calculate the number of deaths of each player by date.
    deaths_by_date = spark.sql("""SELECT dst AS id, m_date, COUNT(*) AS deaths FROM death_logs 
                                  GROUP BY dst, m_date""")
    deaths_by_date_df = deaths_by_date.toPandas()

    # Create a dataframe that contains both kills and deaths of each player by date.
    dates_from_kills = kills_by_date_df[['id', 'm_date']]
    dates_from_deaths = deaths_by_date_df[['id', 'm_date']]
    temp = pd.concat([dates_from_kills, dates_from_deaths])
    temp = temp.drop_duplicates(subset=['id', 'm_date'])

    # Merge the two dataframes into one.
    kill_info = pd.merge(temp, kills_by_date_df, how='outer', on=['id', 'm_date'])
    kill_info = kill_info.fillna(0)
    merged_df = pd.merge(kill_info, deaths_by_date_df, how='outer', on=['id', 'm_date'])
    merged_df = merged_df.fillna(0)

    # Calculate the overall average kill ratio of each player.
    merged_df['kill_ratio'] = merged_df['kills']/(merged_df['kills']+merged_df['deaths'])
    avg_kill_ratio = merged_df[['id', 'kill_ratio']].groupby(['id'], as_index=False).mean()
    avg_kill_ratio.columns = ['id', 'avg_kill_ratio']

    return avg_kill_ratio


def get_avg_time_diff_between_kills(kill_logs):
    """This function calculates the average time difference between consecutive kills for each player.
       Args:
           kill_logs: Dataframe that contains kill records of players
       Returns:
           avg_kill_intervals_df: Dataframe that contains values of overall average time difference 
                                  between kills for each player
    """
    time_diff_logs = spark.sql("""SELECT mid, src, UNIX_TIMESTAMP(time) AS time, 
                                  UNIX_TIMESTAMP(LAG(time, 1) OVER (PARTITION BY mid, src ORDER BY time)) 
                                  AS prev_time FROM kill_logs ORDER BY mid, src""")
    time_diff_logs.registerTempTable("time_diff_logs")

    tdiff = spark.sql("""SELECT mid, src, time, (time - prev_time) AS tsdiff FROM time_diff_logs 
                         ORDER BY src, mid, time""")
    tdiff.registerTempTable("tdiff")

    avg_kill_intervals = spark.sql("""SELECT src AS id, AVG(tsdiff) AS delta  FROM tdiff WHERE tsdiff IS NOT NULL 
                                      GROUP BY src""")
    avg_kill_intervals_df = avg_kill_intervals.toPandas()

    return avg_kill_intervals_df


### Functions that analyze the victimisation-based mechanism


def add_level_of_harm(logs, perc):
    """This function checks whether a killing is critical or not 
       in accordance with the given level of harm.
       Args:
           logs: Dataframe that contains killings
           perc: Number between 0 and 100 which represents the percentage
       Returns:
           res: Dataframe that contains the cases in accordance with the motif
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("SELECT mid, COUNT(*) AS num_rows FROM logs GROUP BY mid ORDER BY mid")
    num_of_rows.registerTempTable("num_of_rows")

    fin_c_match_logs = spark.sql("""SELECT c.*, num_rows 
                                    FROM logs c JOIN num_of_rows n ON c.mid = n.mid""")
    fin_c_match_logs.registerTempTable("fin_c_match_logs")

    # Get the ranking of each victim for each match.
    rank_tab = spark.sql("""SELECT *, RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS ranking 
                            FROM fin_c_match_logs ORDER BY mid, time DESC""")
    rank_tab.registerTempTable("rank_tab")

    # Get victims who were killed after getting into the top 30 percent.
    # The value of damage is one if the victim got killed after getting into the top 30 percent, and otherwise zero.
    res = spark.sql("SELECT *, " + "CASE WHEN ((ranking + 1) / num_rows) > " + str(perc/100) + 
                    " THEN 0 ELSE 1 END AS damage FROM rank_tab")
    
    return res


def get_vic_summary_tab(legit_cases):
    """This function returns a summary table for the victimisation-based mechanism.
       Args:
           legit_cases: Dataframe that contains the cases in accordance with the motif
       Returns:
           add_dates: Dataframe that contains the total number of victimisation experiences 
                      and the number of unique killers.
    """
    # Get the summary data by date for each cheater.
    victims_tab = spark.sql("""SELECT dst AS id, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(dst_sd, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                               CAST(DATEDIFF(dst_sd, m_date) AS INT) AS period, src AS killer, 
                               COUNT(*) AS exp, SUM(damage) AS sev_exp FROM legit_cases
                               GROUP BY dst, dst_sd, m_date, src""")
    victims_tab.registerTempTable("victims_tab")

    # Get the date when the player was first killed by cheating.
    first_m_dates = spark.sql("""SELECT * 
                                 FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                 AS rownumber FROM victims_tab) WHERE rownumber IN (1)""")
    first_m_dates.registerTempTable("first_m_dates")
    
    # Get the table that contains the total number of victimisation experiences and the number of unique cheaters.
    vic_info = spark.sql("""SELECT id, start_date, SUM(exp) AS total_exp, SUM(sev_exp) AS total_sev_exp, 
                            COUNT(DISTINCT killer) AS uniq_killers 
                            FROM victims_tab GROUP BY id, start_date""")
    vic_info.registerTempTable("vic_info")

    add_dates = spark.sql("""SELECT v.id, v.start_date, f.m_date, f.period, 
                             v.total_exp, v.total_sev_exp, v.uniq_killers 
                             FROM vic_info v LEFT JOIN first_m_dates f ON v.id = f.id""")
    
    return add_dates 


### Functions for creating mapping tables


def with_column_index(sdf):
    """This function adds an index column to the given dataframe.
       Args:
           sdf: Dataframe without an index
       Returns:
           Dataframe that contains row numbers as an index.
    """
    new_schema = StructType(sdf.schema.fields + [StructField("ColumnIndex", LongType(), False),])
    
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)


def permute_node_labels(raw_td, nodes, team_ids):
    """This function permutes the node labels of the given network randomly and returns a mapping table.
       Args:
           raw_td: Original dataframe that contains kills
           nodes: Dataframe that contains player information
           team_ids: Dataframe that contains pairs of player and team ID
       Returns:
           mapping: Dataframe that can be used to replace the original node labels 
                    with the new node labels. 
    """
    spark.sql("""SELECT mid, m_date, dst AS id, dst_curr_flag AS flag FROM td 
                 UNION 
                 SELECT mid, m_date, src, src_curr_flag FROM td ORDER BY mid""").createOrReplaceTempView("temp_1")
    spark.sql("""SELECT mid, m_date, dst AS id, dst_curr_flag AS flag FROM td 
                 UNION 
                 SELECT mid, m_date, src, src_curr_flag FROM td ORDER BY mid""").createOrReplaceTempView("temp_2")

    # Add team information of players for each teamplay match.
    temp_1_with_tids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                    CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                    FROM temp_1 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                    ORDER BY mid, tid, flag""")
    temp_1_with_tids.registerTempTable("temp_1_with_tids")
    
    temp_2_with_tids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                    CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                    FROM temp_2 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                    ORDER BY mid, tid, flag""")
    temp_2_with_tids.registerTempTable("temp_2_with_tids")

    df1_ci = with_column_index(temp_1_with_tids)

    # Create a table for mapping.
    rand_tab = spark.sql("""SELECT mid AS match_id, id AS rand, flag AS rand_flag, tid AS rand_tid 
                            FROM temp_2_with_tids ORDER BY mid, tid, flag, rand()""")
    rand_tab.registerTempTable("rand_tab")
    
    df2_ci = with_column_index(rand_tab)

    join_on_index = df1_ci.join(df2_ci, df1_ci.ColumnIndex == df2_ci.ColumnIndex, 'inner').drop("ColumnIndex")
    join_on_index.registerTempTable("join_on_index")

    mapping = spark.sql("""SELECT mid AS match_id, id AS original, flag AS orig_flag, tid AS orig_tid, 
                           rand AS randomised, rand_flag, rand_tid 
                           FROM join_on_index ORDER BY mid""")
    
    return mapping
    

### Functions that analyze the observation-based mechanism


def get_observers(records):
    """This function returns a summary table for the observation-based mechanism.
       Args:
           records: Dataframe that contains killings
       Returns:
           add_dates: Dataframe that contains the total number of observations
                      and the number of unique cheaters.
    """
    # Get a list of killings done by cheaters.
    kills_done_by_cheaters = spark.sql("""SELECT mid, src AS killer, time, damage AS dam, aid FROM records 
                                          WHERE src_curr_flag = 1""")
    kills_done_by_cheaters.registerTempTable("kills_done_by_cheaters")
    
    victims = spark.sql("""SELECT mid, dst AS vic, time, aid FROM records 
                           WHERE dst_flag == 1 AND dst_curr_flag == 0""")
    victims.registerTempTable("victims")
    
    # Get a table of players (both killers and victims) who observed killings done by cheaters when they were alive.
    sub_observers = spark.sql("""SELECT s.mid, s.src AS id, s.src_sd AS start_date, s.src_bd, 
                                 k.time, s.m_date, k.dam, k.killer, k.aid 
                                 FROM records s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid < k.aid 
                                 WHERE src_flag == 1 AND src != killer AND src_curr_flag == 0""")
    sub_observers.registerTempTable("sub_observers")
    
    # Get a table of players (both killers and victims) who observed killings done by cheaters when they were alive.
    observers_tab = spark.sql("""SELECT s.* FROM sub_observers s JOIN victims v ON s.mid = v.mid AND s.id = v.vic AND s.aid < v.aid
                                 UNION
                                 SELECT s.mid, s.src AS id, s.src_sd, s.src_bd, k.time, s.m_date, k.dam, k.killer, k.aid
                                 FROM records s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                                 WHERE src_flag == 1 AND src != killer AND src_curr_flag == 0
                                 UNION
                                 SELECT s.mid, s.dst AS id, s.dst_sd, s.dst_bd, k.time, s.m_date, k.dam, k.killer, k.aid 
                                 FROM records s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                                 WHERE dst_flag == 1 AND dst != killer AND dst_curr_flag == 0""")
    
    observers_tab.registerTempTable("observers_tab")

    obs_summary = spark.sql("""SELECT mid, id, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(start_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                               CAST(DATEDIFF(start_date, m_date) AS INT) AS period, 
                               killer, COUNT(*) AS obs, SUM(dam) AS sev_dam 
                               FROM observers_tab 
                               GROUP BY mid, id, start_date, m_date, killer""")
    
    return obs_summary

