from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def add_damage(rd, percent):
    """Adds the amount of potential damage caused by cheating to the victim for each killing.
       Exploits the fact that players who survive longer are ranked higher.
    
    Args:
        rd: A Spark DataFrame of killings.  
        percent: An integer between 0 and 100 representing the degree of harm. For example,
            we assume that players are severely harmed if they were killed by cheating 
            after getting into the top 30% if the value of 'percent' is 30. 

    Returns:
        damage: A Spark DataFrame with an additional column in which 
            each row (victim) takes on the value of 1 if the player got into the top 
            and the value 0 otherwise.
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("""SELECT mid, COUNT(*) AS num_of_rows FROM rd 
                               GROUP BY mid ORDER BY mid""")
    num_of_rows.registerTempTable("num_of_rows")

    add_num_of_rows = spark.sql("""SELECT r.*, num_of_rows 
                                   FROM rd r JOIN num_of_rows n ON r.mid = n.mid""")
    add_num_of_rows.registerTempTable("add_num_of_rows")
    
    ranks = spark.sql("""SELECT *, RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS rank 
                         FROM add_num_of_rows ORDER BY mid, time DESC""")
    ranks.registerTempTable("ranks")

    damage = spark.sql("SELECT *, " + "CASE WHEN ((rank + 1) / num_of_rows) > " + str(percent/100) +
                       " THEN 0 ELSE 1 END AS damage FROM ranks")
    
    return damage


def get_vic_summary_tab(experiences):
    """Gets the number of experiences that occurred before cheating adoption for each cheater. 

    Args:
        experiences: A Spark DataFrame of experiences (= being killed by cheating) 
            that players (victims) had before cheating adoption.

    Returns:
        add_dates: A Spark DataFrame that shows the total number of experiences and 
            the number of experiences with severe harm before cheating adoption for each cheater.
    """
    victim_stats = spark.sql("""SELECT dst AS id, src AS killer,
                                TO_DATE(CAST(UNIX_TIMESTAMP(dst_sd, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                                TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                                CAST(DATEDIFF(dst_sd, m_date) AS INT) AS period,  
                                COUNT(*) AS num_of_exp, 
                                SUM(damage) AS num_of_severe_damage 
                                FROM experiences GROUP BY dst, dst_sd, m_date, src""")
    victim_stats.registerTempTable("victim_stats")

    # Get the date when the player was first killed by cheating (= the first contact with cheating).
    first_contacts = spark.sql("""SELECT * 
                                  FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                  AS row_number FROM victim_stats) WHERE row_number IN (1)""")
    first_contacts.registerTempTable("first_contacts")

    summary_table = spark.sql("""SELECT id, start_date, SUM(num_of_exp) AS total_exp, 
                                 SUM(num_of_severe_damage) AS total_severe_damage
                                 FROM victim_stats GROUP BY id, start_date""")
    summary_table.registerTempTable("summary_table")

    add_dates = spark.sql("""SELECT s.id, s.start_date, f.m_date, f.period, 
                             s.total_exp, s.total_severe_damage
                             FROM summary_table s LEFT JOIN first_contacts f ON s.id = f.id""")
    
    return add_dates 


def get_observers(obs_data):
    """Gets a list of observers who observed cheating and the number of observations 
       for each killer within each match.

    Args:
        obs_data: A Spark DataFrame of killings.

    Returns:
        summary_table: A Spark DataFrame that lists the players who observed cheating, 
            killers whom they observed, and the number of killings done by each cheater 
            they observed for each match. 
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
    """Counts the number of observations that occurred before adopting cheating for each cheater.

    Args:
        observers: A Spark DataFrame that lists the pairs of cheating killer and observer and 
            shows the number of observations for each pair.  
        num_of_obs: A number representing the definition of observation. For example, 
            we assume two killings done by the same cheater as one observation 
            if the value of 'num_of_obs' is 2.

    Returns:
        add_dates: A Spark DataFrame that shows the total number of observations and 
            before adopting cheating for each cheater.
    """
    summary_table = spark.sql("SELECT id, start_date, SUM(CASE WHEN num_of_obs >= " + str(num_of_obs) +
                              " THEN 1 ELSE 0 END) AS total_obs FROM observers GROUP BY id, start_date")
    summary_table.registerTempTable("summary_table")

    first_contacts = spark.sql("""SELECT * 
                                  FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                  AS row_number FROM observers) WHERE row_number IN (1)""")
    first_contacts.registerTempTable("first_contacts")

    add_dates = spark.sql("""SELECT s.id, s.start_date, f.m_date, f.period, s.total_obs
                             FROM summary_table s LEFT JOIN first_contacts f ON s.id = f.id""")

    return add_dates

