library(readr)
library(caret)
library(randomForest)

set.seed(1)

cat("reading the train and test data\n")
train <- read_csv("../input/train.csv")
test  <- read_csv("../input/test.csv")

feature.names <- names(train)[2:ncol(train)-1]

cat("assuming text variables are categorical & replacing them with numeric ids\n")
for (f in feature.names) {
  if (class(train[[f]])=="character") {
    levels <- unique(c(train[[f]], test[[f]]))
    train[[f]] <- as.integer(factor(train[[f]], levels=levels))
    test[[f]]  <- as.integer(factor(test[[f]],  levels=levels))
  }
}

cat("replacing missing values with -1\n")
train[is.na(train)] <- -1
test[is.na(test)]   <- -1

cat("sampling train to get around memory limitations\n")
train <- train[sample(nrow(train), 40000),]
gc()

cat("training a Random Forest classifier\n")
rf <- randomForest(train[,feature.names], factor(train$target), ntree=40, sampsize=5000, nodesize=2)

cat("Create Importance Dataframe\n")
submission <- data.frame(Field=rownames(rf$importance),rf$importance)
submission <- submission[order(rf$importance,decreasing=TRUE),]
head(submission,30)
cat("saving the submission file\n")
write_csv(submission, "random_forest_importance_submission.csv")
