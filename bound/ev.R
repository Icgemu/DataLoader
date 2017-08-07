ev <- read.csv("F://idea/DataLoader/geo-mapped.csv",header = T,sep=",",encoding = "UTF-8",fileEncoding = "UTF-8")
ev$date_str <- as.factor(ev$date_str)
ev$date_str <- as.Date(ev$date_str,"%Y%m%d")

mySelected <- subset(ev,id %in% c("141","339","277","315","317","76","381"))

library("ggplot2")

ggplot(mySelected,aes(x=date_str ,y=avgTemp ,colour=name,shape = name)) +geom_smooth(n=100,span=0.2) + geom_point(size = 1)
ggplot(mySelected,aes(x=date_str ,y=maxTmp ,colour=name,shape = name)) + geom_smooth(n=100,span=0.2)+ geom_point(size = 2)
ggplot(mySelected,aes(x=date_str ,y=medianTemp ,colour=name,shape = name)) +geom_smooth(n=100,span=0.2) + geom_point(size = 1)
ggplot(mySelected,aes(x=date_str ,y=medianTemp ,colour=name,shape = name)) +geom_line() + geom_point(size = 1)
