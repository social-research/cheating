from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def add_damage(td, percent):
    """This function checks whether a killing is critical or not 
       in accordance with the given level of harm. The value of damage is one
       if the victim got killed after getting into the top 30 percent, and otherwise zero.
       Args:
           td: DataFrame that contains killings
           percent: Number between 0 and 100 which represents the percentage
       Returns:
           damage: DataFrame that contains the cases in accordance with the motif
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("""SELECT mid, COUNT(*) AS num_of_rows FROM td 
                               GROUP BY mid ORDER BY mid""")
    num_of_rows.registerTempTable("num_of_rows")

    add_num_of_rows = spark.sql("""SELECT t.*, num_of_rows 
                                   FROM td t JOIN num_of_rows n ON t.mid = n.mid""")
    add_num_of_rows.registerTempTable("add_num_of_rows")

    ranks = spark.sql("""SELECT *, RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS rank 
                         FROM add_num_of_rows ORDER BY mid, time DESC""")
    ranks.registerTempTable("ranks")

    damage = spark.sql("SELECT *, " + "CASE WHEN ((rank + 1) / num_of_rows) > " + str(percent/100) +
                       " THEN 0 ELSE 1 END AS damage FROM ranks")
    
    return damage


def get_vic_summary_tab(transitions):
    """This function returns a summary table for the victimisation-based mechanism.
       Args:
           transitions: DataFrame that contains the cases in accordance with the motif
       Returns:
           add_dates: DataFrame that contains the total number of victimisation experiences
    """
    stats_of_victims = spark.sql("""SELECT dst AS id, 
                                    TO_DATE(CAST(UNIX_TIMESTAMP(dst_sd, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                                    TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                                    CAST(DATEDIFF(dst_sd, m_date) AS INT) AS period, src AS killer, 
                                    COUNT(*) AS num_of_exp, 
                                    SUM(damage) AS num_of_severe_damage 
                                    FROM transitions
                                    GROUP BY dst, dst_sd, m_date, src""")
    stats_of_victims.registerTempTable("stats_of_victims")

    # Get the date when the player was first killed by cheating.
    first_m_dates = spark.sql("""SELECT * 
                                 FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                 AS row_number FROM stats_of_victims) WHERE row_number IN (1)""")
    first_m_dates.registerTempTable("first_m_dates")

    summary_table = spark.sql("""SELECT id, start_date, SUM(num_of_exp) AS total_exp, 
                                 SUM(num_of_severe_damage) AS total_severe_damage
                                 FROM stats_of_victims 
                                 GROUP BY id, start_date""")
    summary_table.registerTempTable("summary_table")

    add_dates = spark.sql("""SELECT s.id, s.start_date, f.m_date, f.period, 
                             s.total_exp, s.total_severe_damage
                             FROM summary_table s LEFT JOIN first_m_dates f ON s.id = f.id""")
    
    return add_dates 


def get_observers(obs_data):
    """This function returns a summary table for the observation-based mechanism.
       Args:
           obs_data: DataFrame that contains killings
       Returns:
           add_dates: DataFrame that contains the total number of observations
    """
    kills_done_by_cheaters = spark.sql("""SELECT mid, src AS killer, time, aid FROM obs_data 
                                          WHERE src_curr_flag = 1""")
    kills_done_by_cheaters.registerTempTable("kills_done_by_cheaters")
    
    victims = spark.sql("""SELECT mid, dst AS vic, time, aid FROM obs_data 
                           WHERE dst_flag == 1 AND dst_curr_flag == 0""")
    victims.registerTempTable("victims")
    
    # Get a table of players (both killers and victims) who observed killings done by cheaters when they were alive.
    sub_observers = spark.sql("""SELECT s.mid, s.src AS id, s.src_sd AS start_date, s.src_bd, 
                                 k.time, s.m_date, k.killer, k.aid 
                                 FROM obs_data s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid < k.aid 
                                 WHERE src_flag == 1 AND src != killer AND src_curr_flag == 0""")
    sub_observers.registerTempTable("sub_observers")

    observers = spark.sql("""SELECT s.* FROM sub_observers s JOIN victims v 
                             ON s.mid = v.mid AND s.id = v.vic AND s.aid < v.aid
                             UNION
                             SELECT s.mid, s.src AS id, s.src_sd, s.src_bd, k.time, s.m_date, k.killer, k.aid
                             FROM obs_data s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                             WHERE src_flag == 1 AND src != killer AND src_curr_flag == 0
                             UNION
                             SELECT s.mid, s.dst AS id, s.dst_sd, s.dst_bd, k.time, s.m_date, k.killer, k.aid 
                             FROM obs_data s JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                             WHERE dst_flag == 1 AND dst != killer AND dst_curr_flag == 0""")

    observers.registerTempTable("observers")

    summary_table = spark.sql("""SELECT mid, id, 
                                 TO_DATE(CAST(UNIX_TIMESTAMP(start_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                                 TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                                 CAST(DATEDIFF(start_date, m_date) AS INT) AS period, killer, 
                                 COUNT(*) AS num_of_obs
                                 FROM observers 
                                 GROUP BY mid, id, start_date, m_date, killer""")
    
    return summary_table


def get_obs_summary_tab(observers, num_of_obs):
    summary_table = spark.sql("SELECT id, start_date, SUM(CASE WHEN num_of_obs >= " + str(num_of_obs) +
                              " THEN 1 ELSE 0 END) AS total_obs FROM observers GROUP BY id, start_date")
    summary_table.registerTempTable("summary_table")

    first_m_dates = spark.sql("""SELECT * 
                                 FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                 AS row_number FROM observers) WHERE row_number IN (1)""")
    first_m_dates.registerTempTable("first_m_dates")

    add_dates = spark.sql("""SELECT s.id, s.start_date, f.m_date, f.period, s.total_obs
                             FROM summary_table s LEFT JOIN first_m_dates f ON s.id = f.id""")

    return add_dates

