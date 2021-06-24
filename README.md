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

### 第3部分：使用 EventTime 和 Watermark


### 第4部分：使用 Kafka Connect 接入数据

给定全量的商品主数据，使用 Kafka Connect 和 CSV connect 导入到 Kafka之中。

### 第5部分：完整练习

根据商品信息和商品点击事件，计算实时点击量TOP N的商品。

输入：
* ClickEvent：通过Kafka客户端写入Kakfa，是实时的流数据，数据可能会迟到。包含itemId, userId, timestamp。本次练习可使用 avro-kafka-consumer-console 写入。
* ItemData: 商品主数据。定期的更新CSV文件（全量数据）。包含 itemId, description。

输出：itemId, description, count, startTime, endTime

其他：Kafka中的数据使用avro编码，通过schema registry进行校验；当程序重启时，能从上次断点继续执行（容错）
