# THE AVERAGE KILL RATIO OF CHEATERS VS. NON-CHEATERS
#===============================================================================
cheater_kill_ratio <- read.csv("data/cheater_analysis/c_avg_kill_ratio.csv", 
                               header = TRUE, stringsAsFactors = FALSE)
cheater_kill_ratio$flag <- 1

non_cheater_kill_ratio <- read.csv("data/cheater_analysis/nc_avg_kill_ratio.csv", 
                                   header = TRUE, stringsAsFactors = FALSE)
non_cheater_kill_ratio$flag <- 0

merged_data <- rbind(cheater_kill_ratio, non_cheater_kill_ratio)
merged_data$flag <- as.factor(merged_data$flag)


var.test(avg_kill_ratio ~ flag, data = merged_data)

# F test to compare two variances
# 
# data: avg_kill_ratio by flag
# F = 1.276, num df = 854152, denom df = 650, p-value = 2.647e-05
# alternative hypothesis: true ratio of variances is not equal to 1
# 95 percent confidence interval:
# 1.141006 1.418377
# sample estimates:
# ratio of variances 
# 1.275974


t.test(avg_kill_ratio ~ flag, data = merged_data, 
       alternative = c("two.sided"), var.equal = FALSE, conf.level = 0.95)

# Welch Two Sample t-test
# 
# data: avg_kill_ratio by flag
# t = -48.643, df = 651.26, p-value < 2.2e-16
# alternative hypothesis: true difference in means is not equal to 0
# 95 percent confidence interval:
# -0.3761868 -0.3469935
# sample estimates:
# mean in group 0 mean in group 1 
# 0.4045074       0.7660975


# THE AVERAGE TIME DIFFERENCE BETWEEN KILLS OF CHEATERS VS. NON-CHEATERS
#===============================================================================
cheater_kill_interval <- read.csv("data/cheater_analysis/c_avg_kill_interval.csv", 
                                  header = TRUE, stringsAsFactors = FALSE)
cheater_kill_interval$flag <- 1

non_cheater_kill_interval <- read.csv("data/cheater_analysis/nc_avg_kill_interval.csv", 
                                      header = TRUE, stringsAsFactors = FALSE)
non_cheater_kill_interval$flag <- 0

merged_data <- rbind(cheater_kill_interval, non_cheater_kill_interval)
merged_data$flag <- as.factor(merged_data$flag)


var.test(delta ~ flag, data = merged_data)

# F test to compare two variances
# 
# data: delta by flag
# F = 3.7352, num df = 623677, denom df = 628, p-value < 2.2e-16
# alternative hypothesis: true ratio of variances is not equal to 1
# 95 percent confidence interval:
# 3.333446 4.159496
# sample estimates:
# ratio of variances 
# 3.735207


t.test(delta ~ flag, data = merged_data, 
       alternative = c("two.sided"), var.equal = FALSE, conf.level = 0.95)

# Welch Two Sample t-test
# 
# data: delta by flag
# t = 18.235, df = 632.74, p-value < 2.2e-16
# alternative hypothesis: true difference in means is not equal to 0
# 95 percent confidence interval:
# 48.57681 60.30168
# sample estimates:
# mean in group 0 mean in group 1 
# 194.1090       139.6698

