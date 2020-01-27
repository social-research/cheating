library(pscl)


cal_dpois <- function(row_num, df){
  # Calculate the poisson fitted probability.
  vals <- as.numeric(df[row_num, 4:(ncol(df)-2)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$poisson <- round(dpois(stats$vals, df$rand_mean[row_num]), 3)
  colnames(stats) <- c("val", "freq", "poisson")
  return(stats)
}


# First, examine the distribution of numbers from 100 simulations 
# with simple def. of observation and experience and 
# test whether the assumption of a Poisson distribution is appropriate.

data <- read.csv('data/chi_sq/RO_RE_dat.csv', stringsAsFactors = FALSE)
data$rand_mean <- rowMeans(data[, 4:ncol(data)])
data$pois_pval <- NA

for (i in c(1: nrow(data))) {
  test_tab <- cal_dpois(i, data)
  obs_sum <- sum(test_tab$freq)
  lamb <- sum(test_tab$val * test_tab$freq) / obs_sum
  hyp_prob <- round(dpois(test_tab$val, lamb), 3)
  # round(hyp_prob, 3)
  # sum(hyp_prob)
  
  # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
  chisq_gof_test <- chisq.test(test_tab$freq, p=hyp_prob, rescale.p = TRUE)
  # chisq_gof_test$observed
  # chisq_gof_test$expected
  data$pois_pval[i] <- round(chisq_gof_test$p.value, 3)
  # chisq_gof_test
} 

write.csv(data,"data/chi_sq/RO_RE_pois_test.csv", row.names = FALSE)


# Examine the distribution of numbers from 100 simulations 
# with strict def. of observation and experience and 
# test whether the assumption of a Poisson distribution is appropriate.

data <- read.csv('data/chi_sq/SO_SE_dat.csv', stringsAsFactors = FALSE)
data$rand_mean <- rowMeans(data[, 4:ncol(data)])
data$pois_pval <- NA

for (i in c(1: nrow(data))) {
  test_tab <- cal_dpois(i, data)
  obs_sum <- sum(test_tab$freq)
  lamb <- sum(test_tab$val * test_tab$freq) / obs_sum
  hyp_prob <- round(dpois(test_tab$val, lamb), 3)
  # round(hyp_prob, 3)
  # sum(hyp_prob)
  
  # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
  chisq_gof_test <- chisq.test(test_tab$freq, p=hyp_prob, rescale.p = TRUE)
  # chisq_gof_test$observed
  # chisq_gof_test$expected
  data$pois_pval[i] <- round(chisq_gof_test$p.value, 3)
  # chisq_gof_test
} 

write.csv(data,"data/chi_sq/SO_SE_pois_test.csv", row.names = FALSE)


# Examine the distribution of numbers from 100 simulations 
# with simple def. of observation and experience and 
# test whether the assumption of a Normal distribution is appropriate.


cal_dnorm <- function(row_num, df){
  # Calculate the poisson fitted probability.
  vals <- as.numeric(df[row_num, 4:(ncol(df)-3)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$density <- round(dnorm(stats$vals, df$rand_mean[row_num], 
                               df$rand_std[row_num]), 3)
  colnames(stats) <- c("val", "freq", "norm_density")
  return(stats)
}


data <- read.csv('data/chi_sq/RO_RE_norm_dat.csv', stringsAsFactors = FALSE)
data$norm_pval <- NA


for (i in c(1: nrow(data))) {
  test_tab <- cal_dnorm(i, data)
  
  # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
  chisq_gof_test <- chisq.test(test_tab$freq, p=test_tab$norm_density, 
                               rescale.p = TRUE)
  # chisq_gof_test$observed
  # chisq_gof_test$expected
  data$norm_pval[i] <- round(chisq_gof_test$p.value, 3)
  # chisq_gof_test
} 

write.csv(data,"data/chi_sq/RO_RE_norm_test.csv", row.names = FALSE)


# Examine the distribution of numbers from 100 simulations 
# with strict def. of observation and experience and 
# test whether the assumption of a Normal distribution is appropriate.


data <- read.csv('data/chi_sq/SO_SE_norm_dat.csv', stringsAsFactors = FALSE)
data$norm_pval <- NA

for (i in c(1: nrow(data))) {
  test_tab <- cal_dnorm(i, data)
  
  # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
  chisq_gof_test <- chisq.test(test_tab$freq, p=test_tab$norm_density, 
                               rescale.p = TRUE)
  # chisq_gof_test$observed
  # chisq_gof_test$expected
  data$norm_pval[i] <- round(chisq_gof_test$p.value, 3)
  # chisq_gof_test
} 

write.csv(data,"data/chi_sq/SO_SE_norm_test.csv", row.names = FALSE)

