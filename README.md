## 6.824
### finish List（论文+CODE+DEBUG）
- [x] Lab1  用时20h，四天
- [x] Lab2A 用时28h，四天，500次测试无一FAIL
- [x] Lab2B(屎山版v0) 用时三天，忘了多少小时，500次测试无一FAIL
  - 补丁版v1: 用时2天，15+小时，
    - [x] 修复了leader发送rpc，未收到reply时，还会继续执行一致性检查回退的bug
    - [x] 修复了leader收到reply的term>curTerm时，不更新curTerm的bug
    - [x] 删除了选举限制的更新resetTimerTicker的bug，原因是当前candidate不能成为leader
    - [x] 修复了当选leader和更新commitIndex的人数问题，应满足大于(len(rf.peers) >> 1)，即仅有两个人时，不能选举成功
    - [x] 修复了leader仅会提交当前term的日志的bug
  - 补丁版v2:
    - [x] 修复了leader变成follower，仍然发心跳的bug
    - [x] 增加了心跳投票结束机制，当不回复心跳时，会自动结束协程，释放资源
    - [x] 增加了一致性检查优化算法
    - [x] 增加了选举过程结束后退出机制
    - [x] 修复出现选举限制后，即使args.Term > curTerm，leader和candidate也不会变成follower的bug
    - [x] 修复出现一致性冲突后，即使args.Term > curTerm，leader和candidate也不会变成follower的bug
