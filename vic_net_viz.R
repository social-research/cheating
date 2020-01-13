library(igraph)
library(dplyr)
library(inlmisc)


# Read data in csv files.
edge_list <- read.csv("data/viz/edge_list.csv", header = TRUE, stringsAsFactors = FALSE)
node_list <- read.csv("data/viz/node_list.csv", header = TRUE, stringsAsFactors = FALSE)
frlay <- read.csv("data/viz/frlay.csv", header = TRUE, stringsAsFactors = FALSE)

# Convert the data frame into numerics.
frlay <- sapply(frlay, function(x) as.numeric(as.character(x)))

# Sort the node list by cheating flag and team ID.
node_list = node_list[order(node_list$tid, node_list$flag), ]

rownames(node_list) <- 1:nrow(node_list) # Update the index of the dataframe.
node_list$new_id <- seq.int(nrow(node_list))

permuted_list = as.data.frame(node_list %>% sample_frac(1) %>% group_by(tid) 
                              %>% group_by(flag) %>% sample_frac(1))
permuted_list = permuted_list[order(permuted_list$tid), ]

# Update the index of the dataframe.
rownames(permuted_list) <- 1:nrow(permuted_list)
node_list$rand_id = permuted_list$new_id

edge_list = merge(edge_list, node_list, 
                  by.x="src", by.y="id")[, c("new_id", "dst", "time", "color")]
colnames(edge_list) <- c("src", "dst", "time", "color")

edge_list = merge(edge_list, node_list, 
                  by.x="dst", by.y="id")[, c("src", "new_id", "time", "color")]
colnames(edge_list) <- c("src", "dst", "time", "color")

# Sort the kills (edges) by time.
edge_list = edge_list[order(edge_list$time), ]

# Update the index of the dataframe.
rownames(edge_list) <- 1:nrow(edge_list)

# Plot the network.
td_net <- graph.edgelist(as.matrix(edge_list[, c(1, 2)]))
V(td_net)$name <- node_list$new_id
V(td_net)$rand_name <- node_list$rand_id
V(td_net)$flag <- node_list$flag
V(td_net)$tid <- node_list$tid
E(td_net)$weight <- 1:nrow(edge_list)

colors <- inlmisc::GetColors(max(node_list$tid), scheme = "smooth rainbow")
frame_col <- c("black", "red")

num_of_edges <- nrow(edge_list)
edge_colors <- GetColors(num_of_edges, scheme = "smooth rainbow", 
                         stops = c(0.7, 0.9))

# frlay <- layout.fruchterman.reingold(td_net)

# Create two plots and save them as an image file.
png("figs/net_viz/vic_net.png", 
     width=20, height=10, 
     units='in', res=300)

par(mfrow = c(1, 2), mar = c(0, 1, 2, 1))

plot(td_net,
     loops = TRUE,
     vertex.size = 7,
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     edge.width = (E(td_net)$weight/100) * 5, 
     edge.arrow.size = 0.4 + (E(td_net)$weight/5),
     edge.color = adjustcolor(frame_col[edge_list$color], alpha.f = 0.8), 
     vertex.label = V(td_net)$name,
     vertex.label.color = ifelse(V(td_net)$name >= 78, "white", "black"),
     layout = frlay, 
     vertex.label.cex = 1)
title("Original", cex.main = 2)

plot(td_net,
     loops = TRUE,
     vertex.size = 7,
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     edge.width = (E(td_net)$weight/100) * 5, 
     edge.arrow.size = 0.4 + (E(td_net)$weight/5),
     edge.color = adjustcolor(frame_col[edge_list$color], alpha.f = 0.8), 
     vertex.label = V(td_net)$rand_name,
     vertex.label.color = ifelse(V(td_net)$rand_name >= 78, "white", "black"),
     layout = frlay, 
     vertex.label.cex = 1)
title("Permutation", cex.main = 2)

dev.off()
