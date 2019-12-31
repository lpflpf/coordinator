> 基于 zk 组件实现的去中心化的任务调度工具

## 目的

服务宕机最所难免，无状态服务，比如web 服务，rpc 服务等，可以通过dubbo，服务代理的方式解决服务不可用的问题。
**但是有状态的服务，如何能解决单点问题，是此工具解决的问题。**

## 设计思路：

依赖 zookeeper，实现服务之间的调度和通信。
选择一个为主节点，并负责做服务异常监听。当主节点宕机，其他节点选择一个为主节点。

#### Zookeeper 目录结构

  - ACK
    - node1
    - node2
    - ...
  - BROADCAST
  - LOCK\_COORDINATOR
  - REGISTER\_CENTER
    - node1 【临时节点】
    - node2 【临时节点】
    - ...
  - LOCK\_PATH\_INIT

## 服务启动

  - 创建服务所需 Zookeeper 节点
  - 启动协程 监听服务广播节点数据的变更
  - 启动协程 获取协调者的锁
  - 添加临时节点到注册中心

## 服务关闭

  - 关闭协程
  - 关闭相应 zk 连接

## 消息处理

  - 节点增加
  - 节点删除
  - ReBalance
    - 获取当前可使用的计算节点
    - 计算Rebalance 后的 Sharding 与 Rebalance 之前的数据是否一致。若一致则无需ReBalance。
    - 数据不一致，则广播暂停所有正在运行的消息。
    - 确认消息已经广播完成，广播新 Sharding 数据

## 设计思考

  - sharding 版本。在sharding 中加入版本，防止修改了shard后，执行节点仍然使用从zk中获取的分片执行。
  - ReBalance 需要先暂停后开启。防止任务接收消息不一致，导致任务被重复执行。
  - 在 ack 节点上，增加了广播的 version，防止有些节点访问慢，导致获取到的 response 不一致。
