library(pscl)


# Examine the distribution of numbers from 100 simulations 
# to test whether the assumption of a Poisson distribution is appropriate.


cal_dpois <- function(row_num, df){
  # Calculate the poisson fitted probability.
  vals <- as.numeric(df[row_num, 3:(ncol(df)-2)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$poisson <- round(dpois(stats$vals, df$rand_mean[row_num]), 3)
  colnames(stats) <- c("val", "freq", "poisson")
  return(stats)
}


get_chisq_p_val <- function(data){
  for (i in c(1: nrow(data))) {
    test_tab <- cal_dpois(i, data)
    obs_sum <- sum(test_tab$freq)
    lamb <- sum(test_tab$val * test_tab$freq) / obs_sum
    hyp_prob <- round(dpois(test_tab$val, lamb), 3)
    
    # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
    chisq_gof_test <- chisq.test(test_tab$freq, p=hyp_prob, rescale.p = TRUE)
    # chisq_gof_test$observed
    # chisq_gof_test$expected
    data$pois_pval[i] <- round(chisq_gof_test$p.value, 3)
    print(chisq_gof_test)
  } 
}


dat <- read.csv('data/chi_sq/simple_motifs.csv', stringsAsFactors = FALSE)
dat$rand_mean <- rowMeans(dat[, 3:ncol(dat)])
dat$pois_pval <- NA
get_chisq_p_val(dat)


dat <- read.csv('data/chi_sq/strict_motifs.csv', stringsAsFactors = FALSE)
dat$rand_mean <- rowMeans(dat[, 3:ncol(dat)])
dat$pois_pval <- NA
get_chisq_p_val(dat)


# Plot of Poisson Distribution: Observed vs. Expected
# Observed frequency
plot(as.numeric(test_tab$val), chisq_gof_test$observed, 
     main="Poisson Distribution: Observed vs. Expected", 
     type='b', 
     pch=0,
     col='blue',
     xlab="Number of strict (1, 1) motifs",
     ylab="Frequency",
     ylim=c(0,9))

# Dual Y-axes plot (secondary Y-axis)
par(new=T)

# Expected frequency
plot(as.numeric(test_tab$val), chisq_gof_test$expected, 
     type='b', 
     pch=1,
     col='red', 
     xlab="", 
     ylab="",
     ylim=c(0,9))

legend(x=305, y=9, 
       c("Observed", "Expected"), 
       pch=c(0,1), 
       col=c('blue', 'red'))


# Examine the distribution of numbers from 100 simulations 
# to test whether the assumption of a Normal distribution is appropriate.


cal_dnorm <- function(row_num, df){
  vals <- as.numeric(df[row_num, 3:(ncol(df)-3)])
  stats <- as.data.frame(table(vals), stringsAsFactors = FALSE)
  stats$vals <- as.numeric(stats$vals)
  stats$density <- round(dnorm(stats$vals, df$rand_mean[row_num], 
                               df$rand_std[row_num]), 3)
  colnames(stats) <- c("val", "freq", "norm_density")
  return(stats)
}


get_chisq_norm_p_val <- function(data) {
  for (i in c(1: nrow(data))) {
    test_tab <- cal_dnorm(i, data)
    print(sum(test_tab$norm_density))
    # Rescale the hypothesised probabilities to make sum(hyp_prob) = 1.
    chisq_gof_test <- chisq.test(test_tab$freq, p=test_tab$norm_density, 
                                 rescale.p = TRUE)
    # chisq_gof_test$observed
    # chisq_gof_test$expected
    data$norm_pval[i] <- round(chisq_gof_test$p.value, 3)
    print(chisq_gof_test)
  } 
}


dat <- read.csv('data/chi_sq/simple_motifs_norm_dat.csv', stringsAsFactors = FALSE)
dat$norm_pval <- NA
get_chisq_norm_p_val(dat)


dat <- read.csv('data/chi_sq/strict_motifs_norm_dat.csv', stringsAsFactors = FALSE)
dat$norm_pval <- NA
get_chisq_norm_p_val(dat)

