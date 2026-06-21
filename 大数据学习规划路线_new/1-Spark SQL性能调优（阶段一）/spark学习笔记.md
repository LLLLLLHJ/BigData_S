# 哔站【40分钟速通】分布式计算框架Spark

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









