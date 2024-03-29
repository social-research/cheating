# PACKAGES
#===============================================================================
library(pscl)


# FUNCTIONS
#===============================================================================
cal_dpois <- function(row_num, df){
  vals <- as.numeric(df[row_num, 4:(ncol(df)-3)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$poisson <- round(dpois(stats$vals, df$rand_mean[row_num]), 3)
  colnames(stats) <- c("val", "freq", "poisson")
  return(stats)
}


run_pois_chi_sq_tests <- function(data, output_filename) {
  data$pois_pval <- NA
  
  for (i in c(1: nrow(data))) {
    test_tab <- cal_dpois(i, data)
    obs_sum <- sum(test_tab$freq)
    lamb <- sum(test_tab$val * test_tab$freq) / obs_sum
    hyp_prob <- round(dpois(test_tab$val, lamb), 3)
    chisq_gof_test <- chisq.test(test_tab$freq, p=hyp_prob, rescale.p = TRUE)
    data$pois_pval[i] <- round(chisq_gof_test$p.value, 3)
    print(chisq_gof_test)
  } 
  # write.csv(data, output_filename, row.names = FALSE)
}


cal_dnorm <- function(row_num, df){
  vals <- as.numeric(df[row_num, 4:(ncol(df)-3)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$density <- round(dnorm(stats$vals, df$rand_mean[row_num], df$rand_std[row_num]), 3)
  colnames(stats) <- c("val", "freq", "norm_density")
  return(stats)
}


run_norm_chi_sq_tests <- function(data, output_filename) {
  data$norm_pval <- NA
  
  for (i in c(1: nrow(data))) {
    test_tab <- cal_dnorm(i, data)
    chisq_gof_test <- chisq.test(test_tab$freq, p=test_tab$norm_density, rescale.p = TRUE)
    data$norm_pval[i] <- round(chisq_gof_test$p.value, 3)
    print(chisq_gof_test)
  } 
  # write.csv(data, output_filename, row.names = FALSE)
}


# MAIN
#===============================================================================
# Examine the distribution of motifs from 1000 simulations 
# to test whether the assumption of a Poisson/Normal distribution is appropriate.

data <- read.csv("data/chi_sq/RO_RE_chi_sq_data.csv", stringsAsFactors = FALSE)
run_pois_chi_sq_tests(data, "data/chi_sq/RO_RE_chi_sq_poiss.csv")
run_norm_chi_sq_tests(data, "data/chi_sq/RO_RE_chi_sq_norm.csv")

data <- read.csv("data/chi_sq/SO_SE_chi_sq_data.csv", stringsAsFactors = FALSE)
run_pois_chi_sq_tests(data, "data/chi_sq/SO_SE_chi_sq_poiss.csv")
run_norm_chi_sq_tests(data, "data/chi_sq/SO_SE_chi_sq_norm.csv")


# Examine the distribution of basic (1, 0), (0, 1), (1, 1) motifs from 1000 simulations 
# to test whether the assumption of a Poisson/Normal distribution is appropriate.
data <- read.csv("data/chi_sq/simple_motifs.csv", stringsAsFactors = FALSE)
run_pois_chi_sq_tests(data, "")
run_norm_chi_sq_tests(data, "")

data <- read.csv("data/chi_sq/strict_motifs.csv", stringsAsFactors = FALSE)
run_pois_chi_sq_tests(data, "")
run_norm_chi_sq_tests(data, "")

