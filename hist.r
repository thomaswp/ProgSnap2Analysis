library(ggplot2)
library(scales)


data <- data.frame()
for (dir in c("CC", "CWO", "BlockPy", "ITAP", "PCRS")) {
  for (file in c("EQ", "RED", "WatWin")) {
    path <- paste0("out/", dir, "/", file, ".csv")
    if (!file.exists(path)) {
      print(paste("Missing:", path))
      next
    }
    metric <- read.csv(path)
    print(head(metric))
    names(metric)[2] <- "Value"
    metric$SubjectID <- as.character(metric$SubjectID)
    metric$Metric <- if (file == "WatWin") "Watwin" else file
    metric$Dataset <- dir
    if (nrow(data) == 0)
      data <- metric
    else
      data <- rbind(data, metric)
  }
}

# 5x3.5
ggplot(data, aes(Value)) + geom_density(fill="gray") + 
  scale_x_continuous(breaks = c(0.5)) + 
  scale_y_continuous(name="Density") +
  theme_bw() + #ggtitle("Metric Distributions") +
  facet_grid(Metric ~ Dataset)

ggplot(data, aes(Value)) + geom_histogram(bins=6, aes(y=..density..)) + 
  scale_x_continuous(breaks = c(0, 0.5, 1)) + 
  scale_y_continuous(name="Density", labels=percent_format()) +
  theme_bw() + #ggtitle("Metric Distributions") +
  facet_grid(Metric ~ Dataset)

n <- 150
test <- data.frame(Value=runif(n*5*3,0,1), 
                   Metric=rep(c("EQ", "RED", "Watwin"), n), 
                   Dataset=c(rep("CC", n), rep("CWO", n), rep("BlockPy", n), rep("PCRS", n), rep("ITAP", n)))

