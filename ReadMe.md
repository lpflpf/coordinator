> 基于 zk 组件实现的去中心化的任务调度工具

##目的

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
  - LOCK_COORDINATOR
  - REGISTER_CENTER
    - node1 【临时节点】
    - node2 【临时节点】
    - ...
  - LOCK_PATH_INIT

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
    - 判断
    - Select Coordinator
    - Coordinator get current Sharding info , get node info,
    - 选择Strategy，并为Node分配Sharding构成ReBalance
    - create Response/id , wait Response.
    - 更新ReBalance 到Broadcast，广播消息
    - Watch Response ok.

## 设计思考

  - sharding 版本。在sharding 中加入版本，防止修改了shard后，执行节点仍然使用从zk中获取的分片执行。
