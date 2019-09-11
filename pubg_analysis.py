from functools import reduce
from pyspark.sql.functions import col, lit, when
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.types import StructType, StructField, LongType


# Functions for controlling files in the file system.

def clean_edges(table_name):
    """This function removes invalid records from kill records stored in the given table.
       Args:
           table_name: Name of a table that contains kill records
       Returns:
           cleaned_logs: Kill records without invalid records and matches in special mode
    """
    # Get edges from a table and remove invalid records with missing src or dst.
    tab = "SELECT * FROM " + table_name + " WHERE src != 'null' AND dst != 'null'"
    edges = spark.sql(tab)
    edges.registerTempTable("edges")

    # Add cheating flags of killers and those of victims.
    # First, add cheating flags of killers.
    add_src_flags = spark.sql("""SELECT mid, src, ban_date AS src_bd, cheating_flag AS src_flag, dst, time, m_date 
                                 FROM edges e JOIN players p ON e.src = p.id""")
    add_src_flags.registerTempTable("add_src_flags")

    # Add cheating flags of victims.
    edges = spark.sql("""SELECT mid, src, src_bd, src_flag, dst, ban_date AS dst_bd, cheating_flag AS dst_flag, time, m_date 
                         FROM add_src_flags a JOIN players p ON a.dst = p.id ORDER BY src_flag""")
    edges.registerTempTable("edges")

    # Find matches where at least one cheater took part in (without considering the start date of cheating).
    count_cheaters = spark.sql("SELECT mid, (SUM(src_flag) + SUM(dst_flag)) AS c_cnt FROM edges GROUP BY mid")
    count_cheaters.registerTempTable("count_cheaters")

    # For each match, the value of c_cnt should be zero if there is no cheater.
    c_matches = spark.sql("SELECT * FROM count_cheaters WHERE c_cnt > 0")
    c_matches.registerTempTable("c_matches")
    c_logs = spark.sql("""SELECT e.mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date 
                          FROM edges e JOIN c_matches c ON e.mid = c.mid""")
    c_logs.registerTempTable("c_logs")

    # Remove matches in special mode where players revive multiple times.
    # Players should be killed only once if they are given only one life per match.
    # Compare the total number of victims and that of unique victims to detect matches in special mode.
    num_dst = spark.sql("SELECT mid, COUNT(*) AS num_row, COUNT(DISTINCT dst) AS uniq_dst FROM c_logs GROUP BY mid ORDER BY mid")
    num_dst.registerTempTable("num_dst")

    # For each match, assign the value of zero if the match is in default mode and otherwise assign the value of one.
    mod_tab = spark.sql("SELECT mid, num_row, uniq_dst, CASE WHEN num_row == uniq_dst THEN 0 ELSE 1 END AS spec_mod FROM num_dst")
    mod_tab.registerTempTable("mod_tab")

    # Count the number of each mode.
    mod_cnt = spark.sql("SELECT spec_mod, COUNT(*) FROM mod_tab GROUP BY spec_mod")
    defalut_mods = spark.sql("SELECT mid, num_row FROM mod_tab WHERE spec_mod == 0")
    defalut_mods.registerTempTable("defalut_mods")

    cleaned_logs = spark.sql("""SELECT c.mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date 
                                FROM c_logs c JOIN defalut_mods d ON c.mid = d.mid""")
    cleaned_logs.registerTempTable("cleaned_logs")

    return cleaned_logs


def combine_telemetry_dt_into_one(day, num_files):
    """This function combines all telemetry files into one parquet file.
       Args:
           day: The date when matches were played
           num_files: The total number of files that store kill records created on the given date
    """
    f_path = "/tmp/td_data.parquet"

    if day == 1:
        # Create the first file.
        clogs = clean_edges("td_day_1_1")
        clogs.write.parquet(f_path)
        for i in range(2, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            union_df = parquet_df.unionAll(new_edges)
            union_df.write.parquet(f_path).mode("overwrite")
    else:
        for i in range(1, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            union_df = parquet_df.unionAll(new_edges)
            union_df.write.parquet(f_path).mode("overwrite")


def combine_team_info_into_one(day, num_files):
    """This function combines all team membership files into one parquet file.
       Args:
           day: The date when matches were played
           num_files: The total number of files that store team membership information of matches played on the given date
    """
    f_path = "/tmp/team_info.parquet"

    if day == 1:
        # Create the first file.
        team_info = spark.sql("SELECT * FROM md_day_1_1")
        team_info.write.parquet(f_path)
        for i in range(2, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            new_edges = spark.sql("SELECT * FROM md_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(f_path)
    else:
        for i in range(1, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            new_edges = spark.sql("SELECT * FROM md_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(f_path)


def remove_files_from_tables_dir(day, num_files):
    """This function deletes all files corresponding to the given date.
       Args:
           day: The date when matches were played
           num_files: The total number of files that store matches played on the given date
    """
    for i in range(1, num_files+1):
        dbutils.fs.rm("/FileStore/tables/td_day_" + str(day) + "_" + str(i) + "_edges.txt", True)


def create_data_for_cheater_analysis(day, num_files, fname):
    """This function creates a dataset that contains records of all players between March 1 and March 3.
       Args:
           day: The date when matches were played
           num_files: The total number of files that store kill records created on the given date
           fname: Filename of a file that stores the result.
    """
    f_path = "/tmp/" + fname + ".parquet"

    if day == 1:
        # Create the first file.
        full_td_info = spark.sql("""SELECT mid, src, dst, time, m_date FROM td_day_1_1 
                                    WHERE src != 'null' AND dst != 'null' AND src != dst""")
        full_td_info.write.parquet(f_path)
        for i in range(2, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            # Get edges from a table and remove invalid records such as records with missing src or dst and self-loops.
            new_edges = spark.sql("SELECT mid, src, dst, time, m_date FROM td_day_" + str(day) + "_" + str(i) +
                                  " WHERE src != 'null' AND dst != 'null' AND src != dst")
            new_edges.write.mode("append").parquet(f_path)
    else:
        for i in range(1, num_files+1):
            parquet_df = spark.read.parquet(f_path)
            new_edges = spark.sql("SELECT mid, src, dst, time, m_date FROM td_day_" + str(day) + "_" + str(i) +
                                  " WHERE src != 'null' AND dst != 'null' AND src != dst")
            new_edges.write.mode("append").parquet(f_path)


def remove_matches_in_special_mod(input_fname, output_fname):
    """This function removes matches in special mode where players can revive multiple times.
       Args:
           input_fname: Filename of an input file that contains kill records of matches in both default and special modes
           output_fname: Filename of an output file that contains cleaned logs without matches in special mode
    """
    edges = spark.read.parquet("/tmp/" + input_fname + ".parquet")
    edges.registerTempTable("edges")

    # Players should be killed only once if they are given only one life per match.
    # Compare the total number of victims and that of unique victims to detect matches in special mode.
    num_dst = spark.sql("SELECT mid, COUNT(*) AS num_row, COUNT(DISTINCT dst) AS uniq_dst FROM edges GROUP BY mid ORDER BY mid")                
    num_dst.registerTempTable("num_dst")

    # For each match, assign the value of zero if the match is in defalut mode and otherwise assign the value of one.
    mod_tab = spark.sql("SELECT mid, num_row, uniq_dst, CASE WHEN num_row == uniq_dst THEN 0 ELSE 1 END AS spec_mod FROM num_dst")       
    mod_tab.registerTempTable("mod_tab")

    # Count the number of each mode.
    # mod_cnt = spark.sql("SELECT spec_mod, COUNT(*) FROM mod_tab GROUP BY spec_mod")
    # display(mod_cnt)

    # Get match IDs in default mode.
    default_mods = spark.sql("SELECT mid, num_row FROM mod_tab WHERE spec_mod == 0")
    default_mods.registerTempTable("default_mods")

    cleaned_logs = spark.sql("SELECT e.mid, src, dst, time, m_date FROM edges e JOIN default_mods d ON e.mid = d.mid")
    cleaned_logs.write.parquet("/tmp/" + output_fname + ".parquet")


# Functions for analysing cheaters and comparing them with non-cheaters

def cal_avg_kill_ratio(kill_logs, death_logs):
    """This function calculates the average kill ratio for each player.
       Args:
           kill_logs: Dataframe that contains kill records of players
           death_logs: Dataframe that contains death records of players
       Returns:
           kill_ratio_tab: Dataframe that contains values of average kill ratio by date for each player
           avg_kill_ratio: Dataframe that contains values of overall average kill ratio for each player
    """
    # Calculate the number of kills of each player by date.
    kills_by_date = spark.sql("SELECT src AS id, m_date, COUNT(*) AS kills FROM kill_logs GROUP BY src, m_date")
    kills_by_date_df = kills_by_date.toPandas()

    # Calculate the number of deaths of each player by date.
    deaths_by_date = spark.sql("SELECT dst AS id, m_date, COUNT(*) AS deaths FROM death_logs GROUP BY dst, m_date")
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

    # Calculate the average kill ratio of each player by date.
    merged_df['kill_ratio'] = merged_df['kills']/(merged_df['kills']+merged_df['deaths'])
    kill_ratio_tab = merged_df[['id', 'm_date', 'kill_ratio']].sort_values(by=['id', 'm_date'])

    # Calculate the overall average kill ratio of each player.
    kill_ratio_by_date_df = merged_df[['id', 'kill_ratio']]
    avg_kill_ratio = kill_ratio_by_date_df.groupby(['id'], as_index=False).mean()
    avg_kill_ratio.columns = ['id', 'avg_kill_ratio']

    return kill_ratio_tab, avg_kill_ratio


def cal_avg_time_diff_between_kills_of_cheaters(kill_logs):
    """This function calculates the average time difference between consecutive kills for each cheater.
       Args:
           kill_logs: Dataframe that contains kill records of cheaters
       Returns:
           avg_kill_interval_by_date: Dataframe that contains values of average time difference between kills by date 
                                      for each cheater
           ov_avg_kill_intervals: Dataframe that contains values of overall average time difference between kills 
                                  for each cheater
    """
    # Calculate the time difference between kills for each match
    kill_logs = kill_logs.toPandas().sort_values(['src', 'mid', 'time'])
    kill_logs['time'] = pd.to_datetime(kill_logs['time'])
    kill_logs['delta'] = kill_logs.groupby(['mid', 'src'])['time'].diff()
    kill_logs['delta'] = kill_logs['delta'] / np.timedelta64(1, 's')

    # Calculate the average time difference between kills of cheaters by date.
    time_diff_df = kill_logs[['src', 'm_date', 'delta']]
    avg_kill_interval_by_date = time_diff_df.groupby(['src', 'm_date'], as_index=False).mean()

    # Calculate the overall average kill interval of each cheater.
    ov_avg_kill_intervals = avg_kill_interval_by_date.groupby(['src'], as_index=False).mean()
    ov_avg_kill_intervals = ov_avg_kill_intervals.dropna()

    return avg_kill_interval_by_date, ov_avg_kill_intervals


def cal_avg_time_diff_between_kills_of_non_cheaters(kill_logs):
    """This function calculates the average time difference between consecutive kills for each non-cheater.
       Args:
           kill_logs: Dataframe that contains kill records of non-cheaters
       Returns:
           nc_avg_kill_intervals_df: Dataframe that contains values of overall average time difference between kills 
                                     for each non-cheater
    """
    time_diff_logs = spark.sql("""SELECT mid, src, dst, UNIX_TIMESTAMP(time) AS time, 
                                  UNIX_TIMESTAMP(LAG(time, 1) OVER (PARTITION BY mid, src ORDER BY time)) AS prev_time 
                                  FROM kill_logs ORDER BY mid, src""")
    time_diff_logs.registerTempTable("time_diff_logs")

    tdiff = spark.sql("SELECT mid, src, dst, time, (time - prev_time) AS tsdiff FROM time_diff_logs ORDER BY src, mid, time")
    tdiff.registerTempTable("tdiff")

    nc_avg_kill_intervals = spark.sql("SELECT src, AVG(tsdiff) AS delta  FROM tdiff WHERE tsdiff IS NOT NULL GROUP BY src")
    nc_avg_kill_intervals_df = nc_avg_kill_intervals.toPandas()

    return nc_avg_kill_intervals_df


# Functions for estimating the start date of cheating for each cheater

def estimate_start_date_of_cheating(avg_kill_ratio_by_date, avg_kill_interval_by_date):
    """This function estimates the start date of cheating by using the given performance information.
       Args:
           avg_kill_ratio_by_date: Table that contains the average kill ratio by date
           avg_kill_interval_by_date: Table that contains the average time difference between kills by date
       Returns:
           estimation_df: The estimated start date of cheating for each cheater
    """
    # Create a dataframe that store the performance of cheaters.
    avg_kill_interval_by_date.columns = ['id', 'm_date', 'delta']
    combined_tab = pd.merge(avg_kill_ratio_by_date, avg_kill_interval_by_date, how='left', on=['id', 'm_date'])
    combined_tab['kill_ratio'] = combined_tab['kill_ratio'].round(2)
    combined_tab['delta'] = combined_tab['delta'].round(2)
    combined_tab['flag'] = 0

    # Change the value of flag into one if the record meets one of the following conditions.
    combined_tab.loc[(combined_tab['kill_ratio'] >= 0.8) & (combined_tab['delta'] <= 140), 'flag'] = 1
    flagged_records =  combined_tab[combined_tab['flag'] == 1]
    start_dates_df = flagged_records.groupby(['id']).first().reset_index()
    start_dates = spark.createDataFrame(start_dates_df)
    start_dates.registerTempTable("start_dates")
    estimation = spark.sql("SELECT c.id, s.m_date AS start_date, ban_date FROM cheaters c LEFT JOIN start_dates s ON c.id = s.id")

    # Calculate the period of cheating.
    estimation_df = estimation.toPandas()
    estimation_df['ban_date'] = pd.to_datetime(estimation_df['ban_date'])
    estimation_df['start_date'] = pd.to_datetime(estimation_df['start_date'])
    estimation_df['period'] = (estimation_df['ban_date'] - estimation_df['start_date']).astype('timedelta64[D]') + 1

    return estimation_df


def get_cheaters_with_full_info(estim_tab):
    """This function extracts only cheaters with start date information.
       Args:
           estim_tab: Table that contains the estimated start dates of cheating and missing values for cheaters
       Returns:
           complete_rows: The estimated start date of cheating for only cheaters who have full performance information
    """
    complete_rows = estim_tab.loc[estim_tab.period.notnull()]
    complete_rows['period'] = complete_rows['period'].astype('int')

    return complete_rows


def fill_missing_values(estim_tab):
    """This function creates a table that contains both start date and ban date of each cheater.
       For cheaters with at least one missing performance information,this function applies the modal value of two days.
       Args:
           estim_tab: Table that contains the estimated start dates of cheating and missing values for cheaters
       Returns:
           estim_tab: The estimated start date of cheating for all cheaters after filling missing values
    """
    estim_tab['period'] = estim_tab['period'].fillna(2)
    estim_tab['start_date'] = estim_tab['start_date'].fillna(estim_tab['ban_date'] - pd.to_timedelta(estim_tab['period']-1, unit='d'))         
    estim_tab['start_date'] = estim_tab['start_date'].astype('str')
    estim_tab.loc[(estim_tab['start_date'] < '2019-03-01'), 'start_date'] = '2019-03-01'
    estim_tab['start_date'] = pd.to_datetime(estim_tab['start_date'])
    estim_tab['period'] = (estim_tab['ban_date'] - estim_tab['start_date']).astype('timedelta64[D]') + 1

    estim_tab['start_date'] = estim_tab['start_date'].astype('str')
    estim_tab['ban_date'] = estim_tab['ban_date'].astype('str')

    return estim_tab


# Functions relating to victimisation-based mechanism

def count_basic_motifs(telemetry_data, nodes):
    """This function counts the number of team-killings.
       Args:
           telemetry_data: Dataframe that contains kills and player information
           nodes: Dataframe that contains airs of player ID and team ID for each match
       Returns:
           Dataframe that contains motifs
           The count of motifs
    """
    # Find cases which describe the given motif.
    paths = spark.sql("""SELECT * FROM records 
                         WHERE src_flag == 1 AND dst_flag == 1 AND m_date >= src_sd AND m_date < dst_sd AND dst_sd != 'NA'""")
    paths.registerTempTable("paths")
    # display(paths)

    paths_df = paths.toPandas()
    paths_df['m_date'] = pd.to_datetime(paths_df['m_date'])
    paths_df['dst_sd'] = pd.to_datetime(paths_df['dst_sd'])
    paths_df['period'] = (paths_df['dst_sd'] - paths_df['m_date']).astype('timedelta64[D]')
    paths_spark_df = spark.createDataFrame(paths_df)
    paths_spark_df.registerTempTable("paths_spark_df")
    # display(paths_spark_df)

    # Count the number of transitions from non-cheater to cheater that happened within 7 days.
    pairs = spark.sql("SELECT src, dst FROM paths_spark_df WHERE period <= 7 GROUP BY src, dst")
    # display(pairs)
    
    return paths_spark_df, pairs.count()


def count_motifs_with_severe_harm(records, perc):
    """This function counts the number of team-killings.
       Args:
           records: Dataframe that contains kills and player information
           perc: Percentage value between 0 and 100
       Returns:
           Dataframe that contains motifs
           The count of motifs
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("SELECT mid, COUNT(*) AS num_rows FROM records GROUP BY mid ORDER BY mid")
    num_of_rows.registerTempTable("num_of_rows")

    fin_c_match_logs = spark.sql("""SELECT c.mid, src, src_flag, dst, dst_flag, time, num_rows 
                                    FROM records c JOIN num_of_rows n ON c.mid = n.mid""")
    fin_c_match_logs.registerTempTable("fin_c_match_logs")

    # Get the ranking of each victim for each match.
    rank_tab = spark.sql("""SELECT mid, src, src_flag, dst, dst_flag, time, num_rows, 
                            RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS ranking 
                            FROM fin_c_match_logs ORDER BY mid, time DESC""")
    rank_tab.registerTempTable("rank_tab")
    # display(rank_tab)

    # Get victims who were killed after getting into the top 30 percent.
    # The value of damage is one if the victim got killed after getting into the top 30 percent, and otherwise zero.
    top_percent = spark.sql("SELECT mid, src, dst, time, num_rows, ranking, CASE WHEN ((ranking + 1) / num_rows) > " + 
                            str(perc / 100) + " THEN 0 ELSE 1 END AS damage FROM rank_tab")
    top_percent.registerTempTable("top_percent")

    # Add the information about damage into the paths above.
    severity_paths = spark.sql("""SELECT p.mid, p.src, p.dst, p.time, p.period, damage 
                                  FROM paths_spark_df p JOIN top_percent t ON p.mid = t.mid AND p.dst = t.dst""")
    severity_paths.registerTempTable("severity_paths")
    # display(severity_paths)

    # Among paths found above, find the paths where non-cheaters were harmed severly by cheating.
    pairs = spark.sql("SELECT src, dst FROM severity_paths WHERE damage == 1 AND period <= 7 GROUP BY src, dst")
    # display(pairs)
    
    return severity_paths, pairs.count()


def with_column_index(sdf):
    """This function adds an index column to the given dataframe.
       Args:
           sdf: Dataframe without an index
       Returns:
           Dataframe that contains row numbers as an index.
    """
    new_schema = StructType(sdf.schema.fields + [StructField("ColumnIndex", LongType(), False),])
    
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)


def permute_node_labels(raw_td, nodes):
    """This function permutes the node labels of the given network and returns the randomised network.
       Args:
           raw_td: Original dataframe that contains kills
           nodes: Dataframe that contains player information
       Returns:
           randomised_logs: Dataframe that contains kills with permuted node labels.
    """
    temp_1 = spark.sql("""SELECT mid, m_date, dst AS id FROM td GROUP BY mid, m_date, dst 
                          UNION 
                          SELECT mid, m_date, src FROM td GROUP BY mid, m_date, src ORDER BY mid""")
    temp_2 = spark.sql("""SELECT mid, m_date, src AS id FROM td GROUP BY mid, m_date, src 
                          UNION 
                          SELECT mid, m_date, dst FROM td GROUP BY mid, m_date, dst ORDER BY mid""")
    temp_1.registerTempTable("temp_1")
    temp_2.registerTempTable("temp_2")

    # Add the cheating flags of cheaters.
    new_temp_1 = spark.sql("""SELECT mid, m_date, t.id, 
                              CASE WHEN m_date <= ban_date AND m_date >= start_date AND cheating_flag = 1 THEN 1 ELSE 0 END AS flag 
                              FROM temp_1 t JOIN nodes n ON t.id = n.id ORDER BY mid, flag""")
    new_temp_2 = spark.sql("""SELECT mid, m_date, t.id, 
                              CASE WHEN m_date <= ban_date AND m_date >= start_date AND cheating_flag = 1 THEN 1 ELSE 0 END AS flag 
                              FROM temp_2 t JOIN nodes n ON t.id = n.id""")
    new_temp_1.registerTempTable("new_temp_1")
    new_temp_2.registerTempTable("new_temp_2")
    df1_ci = with_column_index(new_temp_1)

    # Create a table for mapping.
    rand_tab = spark.sql("SELECT mid AS match_id, id AS rand, flag AS rand_flag FROM new_temp_2 ORDER BY mid, flag, rand()")
    rand_tab.registerTempTable("rand_tab")
    df2_ci = with_column_index(rand_tab)

    join_on_index = df1_ci.join(df2_ci, df1_ci.ColumnIndex == df2_ci.ColumnIndex, 'inner').drop("ColumnIndex")
    join_on_index.registerTempTable("join_on_index")

    mapping = spark.sql("""SELECT mid AS match_id, id AS original, flag AS orig_flag, rand AS randomised, rand_flag 
                           FROM join_on_index ORDER BY mid""")
    mapping.registerTempTable("mapping")

    # Get randomised gameplay logs.
    temp_rand_logs = spark.sql("""SELECT mid, src, randomised AS new_src, dst, time, m_date 
                                  FROM td t JOIN mapping m ON t.src = m.original AND t.mid = m.match_id""")
    temp_rand_logs.registerTempTable("temp_rand_logs")
    randomised_logs = spark.sql("""SELECT mid, new_src AS src, randomised AS dst, time, m_date 
                                   FROM temp_rand_logs t JOIN mapping m ON t.dst = m.original AND t.mid = m.match_id""")
    
    return randomised_logs


def check_team_killing(kill_logs, participants):
    """This function counts the number of team-killings.
       Args:
           kill_logs: Dataframe that contains kills and player information
           participants: Dataframe that contains airs of player ID and team ID for each match
       Returns:
           The count of team-killings
    """
    # Add the team information of killers.
    update_tid_of_killers = spark.sql("""SELECT k.mid, src, tid AS src_tid, dst, time, m_date 
                                         FROM kill_logs k JOIN participants p ON k.mid = p.mid AND k.src = p.id""")
    update_tid_of_killers.registerTempTable("update_tid_of_killers")

    # Add the team information of victims.
    dt_with_tids = spark.sql("""SELECT u.mid, src, src_tid, dst, tid AS dst_tid, time, m_date 
                                FROM update_tid_of_killers u JOIN participants p ON u.mid = p.mid AND u.dst = p.id""")
    dt_with_tids.registerTempTable("dt_with_tids")
    team_killings = spark.sql("SELECT * FROM dt_with_tids WHERE src_tid = dst_tid")
    
    return team_killings.count()


def plot_dist_of_test_stats(num_of_motifs, test_stats_lst):
    """This function plots the distribution of the given test statistics.
       Args:
           num_of_motifs: The count of motifs on the empirical network
           test_stats_lst: List of test statistics from randomised networks
    """
    test_df = pd.DataFrame(test_stats_lst)
    test_df.columns = ['z-scores']
    zscore_df = (test_df - test_df.mean())/test_df.std(ddof=0)

    rand_mean = test_df.mean()
    rand_std = test_df.std(ddof=0)
    observed_stat = (num_of_motifs - rand_mean) / rand_std
    observed_stat = round(float(np.float64(observed_stat)), 4)

    bins = np.arange(0, zscore_df['z-scores'].max() + 1.5) - 0.5
    fig = zscore_df.hist(column='z-scores', histtype='step', bins=20)
    plt.xlabel("Test statistics")
    plt.ylabel("Frequency")
    plt.axvline(observed_stat, color='r', linewidth=2)
    # plt.xlim(xmin=-4)
    # plt.xlim(xmax=4)
    # plt.ylim(ymax=20)
    plt.title("")
    plt.tight_layout()
    image = plt.show()
    display(image)

