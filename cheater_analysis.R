### First, compare the average kill ratio of cheaters vs. non-cheaters.
# Read data in csv files.
c_avg_kill_ratio <- read.csv("data/cheater_analysis/c_avg_kill_ratio.csv", 
                             header = TRUE, stringsAsFactors = FALSE)
c_avg_kill_ratio$flag <- 1
nc_avg_kill_ratio <- read.csv("data/cheater_analysis/nc_avg_kill_ratio.csv", 
                              header = TRUE, stringsAsFactors = FALSE)
nc_avg_kill_ratio$flag <- 0

merged_kill_ratio <- rbind(c_avg_kill_ratio, nc_avg_kill_ratio)
merged_kill_ratio$flag <- as.factor(merged_kill_ratio$flag)

# Independent Two Sample Variance Test: var.test()
# H0: The variance of average kill ratio by cheating flag is equal 
# (sigma1^2/sigma2^2 = 1).
# H1: The variance of average kill ratio by cheating flag is not equal 
# (sigma1^2/sigma2^2 != 1).

var.test(avg_kill_ratio ~ flag, data = merged_kill_ratio)

# F test to compare two variances
# 
# data:  avg_kill_ratio by flag
# F = 1.276, num df = 854152, denom df = 650, p-value = 2.647e-05
# alternative hypothesis: true ratio of variances is not equal to 1
# 95 percent confidence interval:
#   1.141006 1.418377
# sample estimates:
#   ratio of variances 
#             1.275974
# Thus, the variance of average kill ratio by cheating flag is not equal.

# Independent Two Sample t-test: t-test()
# H0: The average kill ratio of cheaters and that of non-cheaters are equal.
# H1: The average kill ratio of cheaters and that of non-cheaters are not equal.
t.test(avg_kill_ratio ~ flag, data = merged_kill_ratio, 
       alternative = c("two.sided"), # c("two.sided", "less", "greater")
       var.equal = FALSE, 
       conf.level = 0.95)

# Welch Two Sample t-test
# 
# data:  avg_kill_ratio by flag
# t = -48.643, df = 651.26, p-value < 2.2e-16
# alternative hypothesis: true difference in means is not equal to 0
# 95 percent confidence interval:
#   -0.3761868 -0.3469935
# sample estimates:
#   mean in group 0 mean in group 1 
# 0.4045074       0.7660975


### Next, compare the average kill interval of cheaters vs. non-cheaters.
# Read data in csv files.
c_avg_kill_interval <- read.csv("data/cheater_analysis/c_avg_kill_interval.csv", 
                                header = TRUE, stringsAsFactors = FALSE)
c_avg_kill_interval$flag <- 1
nc_avg_kill_interval <- read.csv("data/cheater_analysis/nc_avg_kill_interval.csv", 
                                 header = TRUE, stringsAsFactors = FALSE)
nc_avg_kill_interval$flag <- 0

merged_kill_interval <- rbind(c_avg_kill_interval, nc_avg_kill_interval)
merged_kill_interval$flag <- as.factor(merged_kill_interval$flag)

var.test(delta ~ flag, data = merged_kill_interval)

# F test to compare two variances
# 
# data:  delta by flag
# F = 3.7352, num df = 623677, denom df = 628, p-value < 2.2e-16
# alternative hypothesis: true ratio of variances is not equal to 1
# 95 percent confidence interval:
#   3.333446 4.159496
# sample estimates:
#   ratio of variances 
# 3.735207

t.test(delta ~ flag, data = merged_kill_interval, 
       alternative = c("two.sided"), # c("two.sided", "less", "greater")
       var.equal = FALSE, 
       conf.level = 0.95)

# Welch Two Sample t-test
# 
# data:  delta by flag
# t = 18.235, df = 632.74, p-value < 2.2e-16
# alternative hypothesis: true difference in means is not equal to 0
# 95 percent confidence interval:
#   48.57681 60.30168
# sample estimates:
#   mean in group 0 mean in group 1 
# 194.1090        139.6698

