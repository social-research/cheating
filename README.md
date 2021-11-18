Cheating in Online Gaming Spreads through Observation and Victimization
--------------

This repository contains replication materials for the article:

- J.E. Kim and M. Tsvetkova. (2021). Cheating in online gaming spreads through observation and victimization. Forthcoming in *Network Science*.


The `data` folder contains the data files required to reproduce the figures in the paper (See the `figs` folder) by using `paper-descriptive-stats-visualization.ipynb` and `paper-heatmap-visualization.ipynb`.

- `01-storing-data.ipynb` reads the raw data from text files and stores them as Spark DataFrames in Amazon S3.

- `02-cheater-performance-analysis.ipynb` compares cheaters and non-cheaters in terms of performance by using two performance measures to prove that cheaters are better performers and use the results as a baseline for cheating detection.

- `03-estimation-of-cheating-adoption-time.ipynb` estimates the time of cheating adoption for each cheater using performance information.

- `04-victimization-based-mechanism.ipynb` creates randomized networks by permuting the node labels of each match and counts the total victimization experiences in the data and simulations.

- `05-observation-based-mechanism.ipynb` counts the total observations done by potential cheaters (= players who did not cheat at the time of the match but later adopted cheating) in the data and simulations.

- `06-merging-two-mechanisms.ipynb` merges the results of motif analysis and stores them in csv files.

- `07-descriptive-stats.ipynb` gets some descriptive statistics of the data.

Note that running the jupyter notebooks above requires a Spark environment and we stored large data sets in Amazon S3 due to GitHub's file size restriction. The rest two notebooks below can be run without a Spark cluster and we provide the data files required to replicate the figures of our analysis.

- `paper-descriptive-stats-visualization.ipynb` creates the plots of descriptive statistics presented in the paper.

- `paper-heatmap-visualization.ipynb` creates the heatmaps of motif analysis and chi-squared tests.
