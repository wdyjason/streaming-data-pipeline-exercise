# streaming-data-pipeline-exercise

## 热身任务

本地运行 WordCount 练习，输入数据，观察结果。

## 练习

Kafka中已经包含了创建的商品点击量的数据，现计算每个商品的累积点击量，以及实时热门商品（按照实时点击量取TOP N）。并写入数据库中，以便进行展示。最终需要将应用部署到远程的Flink集群中，查看运行状态。


### 第一部分：数据无 Schema 信息

**任务1**

输入：Kafka消息无key。每条消息为 `itemId:count`，比如 1001:1。
输出：将每个商品的累积点击量输出到控制台。

**任务2**

输入：Kafka消息有key，其中key为itemId，每条消息内容为 count。
输出：将每个商品的累积点击量输出到控制台。

**任务3**

输入：Kafka消息有key，其中key为itemId，每条消息内容为 count。
输出：将每个商品的累积点击量输出到postgresql数据库中，包括ItemId，count（总点击量）以及处理时间。

**任务4**

输入：Kafka消息有key，其中key为itemId，每条消息内容为 count。
输出：每10秒计算一次最近30秒内的点击量，取前数量最多的前5个，输出到postgresql数据库中，包括ItemId，count（总点击量）以及30秒窗口的开始时间和结束时间。注意，数据库表只包含最多前5个商品商品的点击量。将应用提交到远程 Flink 集群中运行，观察管理界面。

**任务5**

输入：Kafka消息有key，其中key为itemId，每条消息内容为 count。
输出：每10秒计算一次最近30秒内的点击量，取前数量最多的前5个，输出到postgresql数据库中，包括ItemId，count（总点击量）以及30秒窗口的开始时间和结束时间。注意，数据库表每个时间窗口的计算结果。


### 第2部分：使用Schema Registry 校验数据编码

TODO

### 第3部分：使用 EventTime 和 Watermark

TODO
