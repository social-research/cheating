Cheating in Online Gaming Spreads through Observation and Victimization
--------------

This repository contains the replication materials for the article:

- J.E. Kim and M. Tsvetkova. (2021). Cheating in online gaming spreads through observation and victimization. Forthcoming in *Network Science*.


The `data` folder contains the data files required to reproduce the figures in the paper (See the `figs` folder) by using `paper-descriptive-stats-visualization.ipynb` and `paper-heatmap-visualization.ipynb`.

- `cheater-stats.ipynb` compares cheaters and non-cheaters in terms of performance by using two performance measures to prove that cheaters are better performers and estimates the time of cheating adoption for each cheater using performance information.

- `descriptive-stats.ipynb` provides the descriptive statistics included in the paper.

- `analyze_cheaters.py` contains the functions used in `cheater-stats.ipynb`

- `vic_net_viz.R` and `obs_net_viz.R` generate Figure 2 in the paper.

Note that running the jupyter notebooks above requires a Spark environment and we stored large data sets in Amazon S3 due to GitHub's file size restriction. The rest two notebooks below can be run without a Spark cluster and we provide the data files required to replicate the figures of our analysis.

- `paper-descriptive-stats-visualization.ipynb` creates the plots of descriptive statistics presented in the paper.

- `paper-heatmap-visualization.ipynb` creates the heatmaps of motif analysis and chi-squared tests.
