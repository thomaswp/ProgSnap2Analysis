library(ggplot2)

n <- 150
test <- data.frame(Value=runif(n*5*3,0,1), 
                   Metric=rep(c("EQ", "RED", "Watwin"), n), 
                   Dataset=c(rep("CC", n), rep("CWO", n), rep("BlockPy", n), rep("PCRS", n), rep("ITAP", n)))

ggplot(test, aes(Value)) + geom_histogram(bins=10) + 
  scale_x_continuous(breaks = c(0, 0.5, 1)) + scale_y_continuous(name="Count") +
  theme_bw() + #ggtitle("Metric Distributions") +
  facet_grid(Metric ~ Dataset)
