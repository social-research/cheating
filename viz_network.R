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
                  by.x="src", by.y="id")[, c("new_id", "dst", "time")]
colnames(edge_list) <- c("src", "dst", "time")

edge_list = merge(edge_list, node_list, 
                  by.x="dst", by.y="id")[, c("src", "new_id", "time")]
colnames(edge_list) <- c("src", "dst", "time")

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
lab_colors <- c("black", "red")
node_shape <- c("circle", "square")
node_size <- c(8, 7)

num_of_edges <- nrow(edge_list)
edge_colors <- GetColors(num_of_edges, scheme = "smooth rainbow", 
                         stops = c(0.7, 0.9))

# frlay <- layout.fruchterman.reingold(td_net)

# Create two plots and save them as an image file.
png("figs/net_viz/net_viz_3.png", 
     width=20, height=10, 
     units='in', res=300)

par(mfrow = c(1, 2), mar = c(0, 1, 2, 1))

plot(td_net, 
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     vertex.size = node_size[node_list$flag + 1], 
     edge.width = (E(td_net)$weight/100) * 5, 
     edge.arrow.size = 0.4 + (E(td_net)$weight/5),
     edge.color = adjustcolor(edge_colors),
     vertex.label = V(td_net)$name,
     vertex.label.color = lab_colors[node_list$flag + 1],
     vertex.shape = node_shape[node_list$flag + 1],
     layout = frlay, 
     vertex.label.cex = 1)
title("Original", cex.main=1)

plot(td_net, 
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     vertex.size = node_size[node_list$flag + 1],
     edge.width = (E(td_net)$weight/100) * 5, 
     edge.arrow.size = 0.4 + (E(td_net)$weight/5),
     edge.color = adjustcolor(edge_colors),
     vertex.label = V(td_net)$rand_name,
     vertex.label.color = lab_colors[node_list$flag + 1],
     vertex.shape = node_shape[node_list$flag + 1],
     layout = frlay, 
     vertex.label.cex = 1)
title("Permutation", cex.main=1)

dev.off()
