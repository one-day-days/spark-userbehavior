# spark-userbehavior

本项目主要对淘宝用户行为数据进行分析与预测，使用编程语言有Python，Scala；数据挖掘工具为Spark。



##  **数据来源**

本课题使用到的数据为阿里开源数据，登录官网下载即可，共有两个数据集：

**用户行为数据集字段**

| 字段名称      | 字段说明           | 提取                           |
| ------------- | ------------------ | ------------------------------ |
| user_id       | 用户标识           | 抽样&字段脱敏                  |
| item_id       | 商品标识           | 字段脱敏                       |
| behavior_type | 用户对商品行为类型 | 包括浏览、收藏、加购物车、购买 |
| user_geohash  | 用户位置空间标识   | 由经纬度通过保密的算法生成     |
| item_category | 商品分类标识       | 字段脱敏                       |
| time          | 行为时间           | 精确到小时                     |

**商品集字段**

| 字段          | 字段说明                     | 提取说明                    |
| ------------- | ---------------------------- | --------------------------- |
| item_id       | 商品标识                     | 抽样&字段脱敏               |
| item_geohash  | 商品位置的空间标识，可以为空 | 由经纬度·通过保密的算法生成 |
| item_category | 商品分类标识                 | 字段脱敏                    |



## **数据储存**

数据下载后，选择Orc文件格式以及Snappy压缩格式存储，提高文件的使用效率。为后面的数据分析、特征提取做好基础。



## **数据分析**

数据分析是为了对数据进行更深入的了解，从而为特征提取和模型训练提供数据支持。本项目使用Spark SQL和Pandas对数据进行分析，对数据的基本特征、分布、相关性等进行探索性分析，以便在后续的特征提取中选择更有效的特征。



## **数据可视化**

数据可视化是为了更直观地展现数据分析的结果。本项目使用Matplotlib对数据分析得到的数据进行可视化。



## **特征提取**

特征提取是机器学习项目中非常重要的一步。在这一步中，需要从原始数据中提取出对预测目标有影响的特征，构建出特征数据集。本项目使用Spark SQL进行特征构建，并使用特征工程技术从特征集中选择出更优秀的特征，以提高模型预测性能。



## **数据挖掘**

数据挖掘是通过机器学习算法对特征数据集进行训练和预测的过程。本项目使用Spark MLib结合提取后的特征，采用逻辑回归、随机森林、梯度提升树三个模型进行用户购买行为的预测。

```scala
//1.创建逻辑回归模型
val lr = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol("featuresVector")
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
//训练模型
val lrModel = lr.fit(trainingData)
//使用测试集评估模型性能
val predictions = lrModel.transform(testData)

//2.创建随机森林模型
val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features_vector")
    .setNumTrees(10)  
//训练模型
val model = rf.fit(trainingData)
//对测试集进行预测
val predictions = model.transform(testData)

//3.构建GBDT模型
val gbdt = new GBTClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxIter(10)
    .setFeatureSubsetStrategy("auto")
    .setMinInfoGain(0.2)
    .setLossType("leastSquares")
//构建pipeline
val pipeline = new Pipeline()
	.setStages(Array(assembler, gbdt))
//训练模型
val model = pipeline.fit(trainingData)
//对测试集进行预测
val predictions = model.transform(testData)
```



## **模型评估**

模型评估是为了衡量模型的性能。本项目使用Spark MLib中的工具对逻辑回归、随机森林、梯度提升树三个模型进行性能评估。

| **模型评分** |            |           |           |
| ------------ | ---------- | --------- | --------- |
| 模型名称     | 精确率     | 召回率    | F1        |
| 逻辑回归     | 0.6126342  | 0.6511899 | 0.6313239 |
| 随机森林     | 0.667463   | 0.666736  | 0.6670998 |
| 梯度提升树   | 0.58687084 | 0.8112228 | 0.6810459 |