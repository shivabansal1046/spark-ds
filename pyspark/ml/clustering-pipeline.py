
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

### spark ml need vectorized features as input

assembler = VectorAssembler(
    inputCols=<list of feature columns>,
    outputCol="features")

## feature scaling

scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

## kmean

kmeans = KMeans().setSeed(1)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
paramGrid = ParamGridBuilder() \
    .addGrid(kmeans.k, [5]) \
    .build()

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=ClusteringEvaluator(),
                          numFolds=5)  # use 3+ folds in practice
cvModel = crossval.fit(<input df>)

print(cvModel.avgMetrics)
predictions = cvModel.transform(<input df>)

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

### udf to map sparse vector values to original index values
def apps_lookup(arr):
    lookup_arr = [...]
    return_val = []
    for key,val in enumerate(arr):
        if val > 0:
            return_val.append(lookup_arr[key])
    return return_val
vector_udf = udf(apps_lookup)

predictions.withColumn("orignal_values", vector_udf(F.col("features"))).show()


