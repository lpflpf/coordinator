> 基于 zk 组件实现的去中心化的任务调度工具

设计思路：

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
