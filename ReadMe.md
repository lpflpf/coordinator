> 一个基于Zookeeper的简易协调器组件，用于常驻内存的多节点任务的作业动态分配。

设计思路：

#### Zookeeper 目录结构
  - RegisterCenter
    - node1
    - node2
    - node3
  - Response
    - id 
      - node1
      - node2
      - node3
  - Broadcast

## 服务启动
  - 监听注册中心
  - 监听Broadcast节点，并处理广播消息
  - 在RegisterCenter 添加节点

## 服务关闭

## 消息处理
  - 节点增加
  - 节点删除
  - ReBalance
    - Select Coordinator
    - Coordinator get current Sharding info , get node info,
    - 选择Strategy，并为Node分配Sharding构成Rebalance
    - create Response/id , wait Response.
    - 更新Rebalance 到Broadcast，广播消息
    - Watch Response ok.

## Bad Case
  - node Online/Offline when ReBalance.
    - Waiting for last ReBalance end.