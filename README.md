# streaming-data-pipeline-exercise

## 目标

1. 了解 Kafka 的基本用法，理解 Key 的作用
2. 通过使用 Flink 理解流式管道的应用场景
3. 通过使用 Schema Registry 理解 Schema 在数据管道中的作用和常见用法

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

**任务1**

给定schema，通过`kafka-avro-console-producer`写入数据到Kafka，在Flink应用中打印反序列化后的数据。

Schema 如下

```json
{
      "type": "record",
      "name": "click",
      "fields": [
          {
              "name": "itemId",
              "type": "string"
          },
          {
              "name": "count",
              "type": "long"
          },
          {
              "name": "eventTime",
              "type" : "long",
              "logicalType": "timestamp-millis"
          }
      ]
    }
```

**任务2**

原始数据中新增了字段 userId（类型为long），迁移schema到新的版本，思考对任务1的影响。

**任务3**

使用Gradle任务，根据Avro的schema文件，生成相应的类型定义。

### 第3部分：使用 EventTime 和 Watermark

TODO
