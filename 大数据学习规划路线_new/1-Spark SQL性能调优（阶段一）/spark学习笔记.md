# 1>>哔站【40分钟速通】分布式计算框架Spark

## sparkRDD与编程模型

### sparkRDD（弹性分布式数据集）

* RDD是只读的
* 弹性：某节点失败后，并不需要从头再重新计算一遍（因为之前计算的RDD都在内存中），所以恢复数据计算只需要返回到上一节点获取其RDD继续计算即可（**注意**：一对一的RDD恢复起来容易；一旦中间经过shuffle，即多对一，恢复数据过程比较麻烦）

![image-20260621161726444](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621161726444.png)



![image-20260621170333216](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621170333216.png)



### Spark Shuffle

* **Spark Shuffle**‌ 是 Spark 里负责把数据在不同节点之间重新分布的过程，简单说就是“洗牌”，把分散的数据按规则聚拢，方便后续计算 。（**按key重新分配数据到不同分区**）



![image-20260621171055869](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171055869.png)

‌‌‌

![image-20260621171400083](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171400083.png)



![image-20260621171542158](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171542158.png)



![image-20260621171641264](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171641264.png)



![image-20260621171805467](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171805467.png)



![image-20260621171929811](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621171929811.png)



![image-20260621172120650](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621172120650.png)



![image-20260621161549129](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621161549129.png)

* Transformation只记录转换关系
* Action触发RDD计算，会输出计算结果
* <**first**：输出第一条数据到控制台上> <**count**：输出RDD的行数到控制台上> <**collect**：输出RDD的整个结果集到控制台上> <**foreach**：遍历-输出数据结果到控制台上> <**saveAsTextFile**：将计算结果输出存储到textfile上>



### 宽依赖与窄依赖

* 窄依赖（RDD之间一对一的关系）
* 宽依赖（RDD之间一对多的关系）

![image-20260621161440925](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621161440925.png)



## spark程序运行架构

![image-20260621175628055](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621175628055.png)

Spark程序的运行架构是一个典型的**主从架构（Master-Slave）**，其核心思想是“**计算向数据移动**”，通过将计算任务分发到多台机器上并行执行，来处理海量数据。一个Spark应用运行时，主要包含以下几个核心组件和概念。

### 🏗️ 核心组件

一个Spark应用由两类关键进程构成：**一个Driver（驱动）和多个Executor（执行器）**。

（对应到**YARN**：**Application Master** 和 **Container**）

*   **Driver (驱动器)**：这是整个Spark应用的核心，可以看作是“大脑”。它负责执行用户程序中的`main()`方法，并创建`SparkContext`上下文。它的主要职责包括：
    *   **任务调度**：将用户程序转化为具体的任务，并在Executor之间进行分配和调度。
    *   **资源申请**：向集群管理器（Cluster Manager）申请运行Executor所需的资源。
    *   **状态监控**：跟踪所有任务的执行情况，并通过Web UI展示运行信息。

*   **Executor (执行器)**：这是真正干活的“工人”，是一个运行在工作节点（Worker Node）上的JVM进程。每个Spark应用都拥有自己独立的一组Executor。它的核心功能有两点：
    *   **执行任务**：负责运行Driver分配给它的具体任务（Task），并将结果返回给Driver。
    *   **数据缓存**：通过内部的**BlockManager**模块，将RDD等数据缓存在内存或磁盘中，显著提升迭代计算和交互式查询的速度。

*   **Cluster Manager (集群管理器)**：负责管理和分配集群资源的外部服务。Spark支持多种集群管理器，如**Standalone**（Spark自带的简单管理器）、**Hadoop YARN**（生产中常用）和**Apache Mesos**。

### 🧩 核心概念与层次关系

一个Spark应用从代码到执行，会经历清晰的层次划分，其关系大致如下：

1.  **Application (应用)**：用户编写的Spark程序，它包含一个Driver和多个Executor。
2.  **Job (作业)**：当应用程序中的代码遇到一个**Action操作**（如`collect()`，`count()`）时，会触发一次计算，生成一个Job。
3.  **Stage (阶段)**：一个Job为了高效执行，会根据RDD之间的依赖关系，特别是**宽依赖**（需要进行Shuffle操作，如`groupByKey`），切分成多个Stage。Stage是任务调度的基本单位，Stage之间是串行执行的。
4.  **Task (任务)**：一个Stage最终会被分解成一组并行执行的任务，每个Task处理一个数据分区（Partition），是Executor上执行的最小工作单元。

一个Application可以包含多个Job，一个Job包含多个Stage，一个Stage包含多个Task。

### ⚙️ 运行基本流程

以Spark on YARN的Cluster模式（生产环境常用）为例，运行流程如下：

1.  **提交与启动**：用户通过`spark-submit`提交应用。集群管理器（YARN）启动**ApplicationMaster**，该AM会启动**Driver**。
2.  **注册与申请资源**：Driver初始化`SparkContext`，并向集群管理器注册并申请Executor资源。
3.  **启动Executor**：集群管理器分配资源后，在多个工作节点上启动**Executor**进程，Executor启动后会向Driver反向注册，建立心跳连接。
4.  **构建DAG与调度**：Driver将用户程序逻辑解析为RDD的**DAG（有向无环图）**，然后提交给内部的**DAG调度器**。DAG调度器将DAG拆分成多个Stage，并提交给底层的**任务调度器**。
5.  **分发与执行任务**：任务调度器将每个Stage的任务集合分发给已注册的Executor执行。Executor上以多线程方式运行任务，充分利用CPU资源。
6.  **反馈与完成**：任务执行结果反馈给Driver，直至所有任务完成。应用运行结束后，Executor释放资源。

### ✨ 架构特点

*   **进程隔离与多线程**：每个应用拥有独立的Executor进程，彼此隔离，但进程内部采用多线程执行任务，比MapReduce的进程模型启动开销更小，更高效。
*   **与资源管理器解耦**：Spark并不关心底层资源管理器是谁，只要能申请到Executor并保持通信即可，这使得它可以灵活部署在Standalone、YARN、Mesos等多种环境上。



## spark作业的提交模式



![image-20260621181506730](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621181506730.png)



* 一个Spark应用由两类关键进程构成：**一个Driver（驱动）和多个Executor（执行器）**。

（对应到**YARN**：**Application Master** 和 **Container**）

* 下图的左图对应**YARN-Cluster**模式，右图对应**YARN-Client**模式（为了便于客户端调试，将Driver放到客户端client里面）

![image-20260621181609756](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621181609756.png)















# 2>>Spark性能调优实战

## 01-性能调优的必要性

### 开发案例 1：数据抽取

案例：给定数据条目，从中抽取特定字段。这样的数据处理需求在平时
的 ETL 作业中相当普遍。想要实现这个需求，我们需要定义一个函数 extractFields：
它的输入参数是 Seq[Row]类型，也即数据条目序列；输出结果的返回类型是
Seq[(String, Int)]，也就是（String, Int）对儿的序列；函数的计算逻辑是从数据条目
中抽取索引为 2 的字符串和索引为 4 的整型。

**scala**

```scala
//实现方案1 —— 反例
val extractFields: Seq[Row] => Seq[(String, Int)] = {
    (rows: Seq[Row]) => {
        var fields = Seq[(String, Int)]()
        rows.map(row => {
        	fields = fields :+ (row.getString(2), row.getInt(4))
        })
        fields
    }
}


//实现方案2 —— 正例
val extractFields: Seq[Row] => Seq[(String, Int)] = {
	(rows: Seq[Row]) =>
		rows.map(row => (row.getString(2), row.getInt(4))).toSeq
}
```

**python**

```python
# 实现方案1 —— 反例【使用可变变量和循环，效率低下】
# 注意：在 PySpark 中，通常是在 RDD 的 map 算子中处理单行数据，
# 而不是像 Scala 示例那样处理 Seq[Row]。
# 但为了忠实于原文逻辑，这里模拟对一组数据的处理函数。

def extract_fields_bad(rows):
    fields = []
    for row in rows:
        # 假设 row 是一个可以通过索引访问的对象，如 list 或 Row
        fields.append((row[2], row[4]))
    return fields


# 实现方案2 —— 正例【使用函数式编程 (map)，简洁高效】
def extract_fields_good(rows):
    # 利用 map 和 lambda 表达式
    return list(map(lambda row: (row[2], row[4]), rows))
# 或者在 RDD 操作中直接使用：
# rdd.map(lambda row: (row[2], row[4]))

```



### 开发案例 2：数据过滤与数据聚合

```scala
/**
(startDate, endDate)
e.g. ("2021-01-01", "2021-01-31")
*/
val pairDF: DataFrame = _
/**
(dim1, dim2, dim3, eventDate, value)
e.g. ("X", "Y", "Z", "2021-01-15", 12)
*/
val factDF: DataFrame = _
// Storage root path
val rootPath: String = _
```

在这个案例中，我们有两份数据，分别是 pairDF 和 factDF，数据类型都是DataFrame。第一份数据 pairDF 的 Schema 包含两个字段，分别是开始日期和结束日期。第二份数据的字段较多，不过最主要的字段就两个，一个是 Event date 事件日期，另一个是业务关心的统计量，取名为 Value。其他维度如 dim1、dim2、dim3 主要用于数据分组，具体含义并不重要。从数据量来看，pairDF 的数据量很小，大概几百条记录，factDF 数据量很大，有上千万行。
对于这两份数据来说，具体的业务需求可以拆成 3 步：

1. 对于 pairDF 中的每一组时间对，从 factDF 中过滤出 Event date 落在其间的数据条目；
2. 从 dim1、dim2、dim3 和 Event date 4 个维度对 factDF 分组，再对业务统计量Value 进行汇总；
3. 将最终的统计结果落盘到 Amazon S3。

**scala**

```scala
//实现方案1 —— 反例
def createInstance(factDF: DataFrame, startDate: String, endDate: String): DataFrame = {
    val instanceDF = factDF
    .filter(col("eventDate") > lit(startDate) && col("eventDate") <= lit(endDate))
    .groupBy("dim1", "dim2", "dim3", "event_date")
    .agg(sum("value") as "sum_value")
    instanceDF
}
pairDF.collect.foreach{
    case (startDate: String, endDate: String) =>
    val instance = createInstance(factDF, startDate, endDate)
    val outPath = s"${rootPath}/endDate=${endDate}/startDate=${startDate}"
    instance.write.parquet(outPath)
}


//实现方案2 —— 正例
val instances = factDF
.join(pairDF, factDF("eventDate") > pairDF("startDate") && factDF("eventDate") <= pairDF("endDate"))
.groupBy("dim1", "dim2", "dim3", "eventDate", "startDate", "endDate")
.agg(sum("value") as "sum_value")
instances.write.partitionBy("endDate", "startDate").parquet(rootPath)
```

**python**

```python
# 实现方案1 —— 反例【在 Driver 端收集小表，然后循环遍历，导致大表被反复扫描】
from pyspark.sql.functions import col, lit, sum as _sum

def create_instance_bad(fact_df, start_date, end_date):
    instance_df = fact_df \
        .filter((col("eventDate") > lit(start_date)) & (col("eventDate") <= lit(end_date))) \
        .groupBy("dim1", "dim2", "dim3", "event_date") \
        .agg(_sum("value").alias("sum_value"))
    return instance_df

# 假设 pair_df 是小表，fact_df 是大表
# 这种写法会导致 fact_df 被扫描 pair_df.count() 次
for row in pair_df.collect():
    start_date = row['startDate']
    end_date = row['endDate']
    
    instance = create_instance_bad(fact_df, start_date, end_date)
    out_path = f"{root_path}/endDate={end_date}/startDate={start_date}"
    instance.write.parquet(out_path)
    


# 实现方案2 —— 正例【使用 Join 代替循环，一次性扫描大表】
from pyspark.sql.functions import col, sum as _sum

# 使用不等式 Join
# 注意：PySpark 中 Join 条件需要用括号包起来，或者使用字符串表达式
instances = fact_df.join(
    pair_df,
    (fact_df.eventDate > pair_df.startDate) & (fact_df.eventDate <= pair_df.endDate),
    "inner"
) \
.groupBy("dim1", "dim2", "dim3", "eventDate", "startDate", "endDate") \
.agg(_sum("value").alias("sum_value"))

instances.write.partitionBy("endDate", "startDate").parquet(root_path)
```



![image-20260621215428646](C:\Users\l\AppData\Roaming\Typora\typora-user-images\image-20260621215428646.png)



## 02-性能调优的本质



























