

val lr = new         LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
val model1 = lr.fit(training)


