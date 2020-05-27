library(igraph)
library(dplyr)
library(inlmisc)


# Read data in csv files.
full_edge_list <- read.csv("data/viz/full_edge_list.csv", header = TRUE, 
                           stringsAsFactors = FALSE)
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

full_edge_list = merge(full_edge_list, node_list, 
                  by.x="src", by.y="id")[, c("new_id", "dst", "time", 
                                             "type", "color", "weight")]
colnames(full_edge_list) <- c("src", "dst", "time", "type", "color", "weight")

full_edge_list = merge(full_edge_list, node_list, 
                  by.x="dst", by.y="id")[, c("src", "new_id", "time", 
                                             "type", "color", "weight")]
colnames(full_edge_list) <- c("src", "dst", "time", "type", "color", "weight")
# Make safeloops default color
full_edge_list$color[full_edge_list$src==full_edge_list$dst] <- 1

# Sort the kills (edges) by time.
full_edge_list = full_edge_list[order(full_edge_list$time), ]

# Update the index of the dataframe.
rownames(full_edge_list) <- 1:nrow(full_edge_list)

# Plot the network.
td_net <- graph.edgelist(as.matrix(full_edge_list[, c(1, 2)]))
V(td_net)$name <- node_list$new_id
V(td_net)$rand_name <- node_list$rand_id
V(td_net)$flag <- node_list$flag
V(td_net)$tid <- node_list$tid
E(td_net)$weight <- full_edge_list$weight

frame_col <- c("black", "deeppink2")
edge_col <- c("black", "deeppink2", "yellowgreen")
vertex_frame <- c(NA, "deeppink2")
vertex_shape <- c("circle", "circle")
node_size <- c(9, 8)

# frlay <- layout.fruchterman.reingold(td_net)

# Create two plots and save them as an image file.
pdf("figs/net_viz/obs_net.pdf", 
    width=3.42, height=3.4)

par(mfrow = c(1, 1), mar = c(0, 0.1, 0.1, 0.1))
#trace("plot.igraph",edit=TRUE) # manually reduce loop size
# find in loop <- function
# cp <- matrix(c(x0, y0, x0 + 0.1, y0 + 0.05, x0 + 0.1, 
#                 y0 - 0.05, x0, y0), ncol = 2, byrow = TRUE)
plot.igraph(td_net,
     loops = TRUE,
     vertex.shape = vertex_shape[node_list$flag + 1],
     vertex.size = node_size[node_list$flag + 1],
     vertex.frame.color = vertex_frame[node_list$flag + 1],
     vertex.color = "lightgray",
     edge.width = 0.05 + (E(td_net)$weight/100) * 1.6,  
     edge.arrow.size = 0.18,
     edge.color = adjustcolor(edge_col[full_edge_list$color], 
                              alpha.f = 0.8), 
     vertex.label = V(td_net)$name,
     vertex.label.color = "black",
     layout = frlay, 
     vertex.label.cex = 0.5)
#title("A) Killing network", cex.main = 0.75, font.main = 1)


dev.off()
