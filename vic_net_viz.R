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

# Make safeloops default color
edge_list$color[edge_list$src==edge_list$dst] <- 1

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
frame_col <- c("black", "deeppink2")
vertex_frame <- c(NA, "deeppink2")
node_shape <- c("circle", "square")
node_size <- c(9, 8)

num_of_edges <- nrow(edge_list)
edge_colors <- GetColors(num_of_edges, scheme = "smooth rainbow", 
                         stops = c(0.7, 0.9))

# frlay <- layout.fruchterman.reingold(td_net)

# Reduce the size of self-loops.
# trace("plot.igraph", edit=TRUE)
# Add cp <- matrix(c(x0, y0, x0 + 0.18, y0 + 0.07, x0 + 0.18, y0 - 0.07, x0, y0), ncol = 2, byrow = TRUE)

# Create two plots and save them as an image file.
png("figs/net_viz/vic_net_rev.png", 
     width=17.8, height=9, 
     units='cm', res=400)

par(mfrow = c(1, 2), mar = c(0, 0.2, 0.7, 0.2))

plot.igraph(td_net,
     loops = TRUE,
     vertex.shape = node_shape[node_list$flag + 1],
     vertex.size = node_size[node_list$flag + 1],
     vertex.frame.color = vertex_frame[node_list$flag + 1],
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     edge.width = 0.05 + (E(td_net)$weight/100) * 1.6, 
     edge.arrow.size = 0.18,
     edge.color = adjustcolor(frame_col[edge_list$color], alpha.f = 0.8), 
     vertex.label = V(td_net)$name,
     vertex.label.color = ifelse(V(td_net)$name >= 78, "white", "black"),
     layout = frlay, 
     vertex.label.cex = 0.5)
title("A) Original network", cex.main = 0.75, font.main = 1)

plot.igraph(td_net,
     loops = TRUE,
     vertex.shape = node_shape[node_list$flag + 1],
     vertex.size = node_size[node_list$flag + 1],
     vertex.color = adjustcolor(colors[node_list$tid], alpha.f = 0.7), 
     vertex.frame.color = vertex_frame[node_list$flag + 1],
     edge.width = 0.05 + (E(td_net)$weight/100) * 1.6, 
     edge.arrow.size = 0.18,
     edge.color = adjustcolor(frame_col[edge_list$color], alpha.f = 0.8), 
     vertex.label = V(td_net)$rand_name,
     vertex.label.color = ifelse(V(td_net)$rand_name >= 78, "white", "black"),
     layout = frlay, 
     vertex.label.cex = 0.5)
title("B) Randomized network", cex.main = 0.75, font.main = 1)

dev.off()
