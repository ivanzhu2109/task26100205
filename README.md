# VeloCache — 单向去重传输库

VeloCache 在发送端与接收端之间传输字节内容：发送端把内容切成 8 KiB 块，
对接收端可能已缓存的块只发 32 字节短引用（SHA-256），从而节省 WAN 流量。
当接收端因容量压力淘汰了被引用的块时，接收端会自动回发补全请求，发送端
重发该块原文，传输无需上层重新提交即可完成。

## 特性

- 按 8 KiB 切块（末块可不足），只有逐字节相同的块才会复用。
- 接收端可复用缓存固定 1 MiB，按原文字节计量，LRU 淘汰，块内容存本地文件
  （`<接收目录>/cache/<哈希>.blk`），索引等会话状态在内存。
- 引用失效自动补全：接收端发 `need`，发送端用保留的原文回 `data`。
- 消息可乱序、重复、任意延迟（不丢失）：重复消息不刷新 LRU 使用次序，
  迟到消息不会重复交付结果，也不会让已完成传输重新等待。
- 发送端在收到完成确认后释放保留的原文；接收端结果按编号只交付一次，
  取走后仅保留防重复交付所需的元数据（已完成编号集合）。

## API

两端仅通过字节消息交互，由调用方在本机直接投递，互不读取对方状态。

```go
snd, _ := velocache.NewSender("/path/to/sender-dir")   // 发送端私有目录
rcv, _ := velocache.NewReceiver("/path/to/receiver-dir") // 接收端私有目录

// 1. 提交传输（编号唯一，单份最大 4 MiB，可同时存在 32 份）
snd.Submit(42, data)

// 2. 取出待发送消息，由调用方投递给对端
for {
    msg, ok := snd.NextMessage() // 接收端同样用 rcv.NextMessage()
    if !ok { break }
    deliverToPeer(msg) // 可乱序、可重复，但最终都要投递
}

// 3. 交付收到的消息
rcv.Deliver(msg) // 发送端 -> 接收端
snd.Deliver(msg) // 接收端 -> 发送端（补全请求 / 完成确认）

// 4. 取回完成结果（每个编号只交付一次）
for {
    id, data, ok := rcv.NextResult()
    if !ok { break }
    use(id, data)
}
```

## 消息格式（二进制，小开销）

| 类型 | 方向 | 布局 |
|------|------|------|
| `data` | 发送→接收 | `[1][id:8][idx:4][total:4][内容]` |
| `ref`  | 发送→接收 | `[2][id:8][idx:4][total:4][SHA-256:32]` |
| `need` | 接收→发送 | `[3][id:8][idx:4][SHA-256:32]` |
| `done` | 接收→发送 | `[4][id:8]` |

## 运行测试

```sh
go test ./...
```

测试覆盖：

- `TestRepeatTransferByteBudget`：预热后连续 100 次传输相同 1 MiB
  （128 个互异高熵块），双向实际消息总字节约 0.6%，远低于 5% 上限，
  且每次输出与原文逐字节核对。
- `TestEvictedReferenceIsRepaired`：引用生成后对应块被其他传输淘汰、
  引用随后到达，自动补全；期间其余传输正常完成，缓存始终不超 1 MiB。
- `TestDisorderDuplicatesAndLateMessages`：数据与控制消息乱序、重复，
  并在原文释放后再次投递全部旧消息；每个编号只交付一次，无悬挂传输。
- `TestBasicTransfer`：基本传输（含非整块末块）。

每个场景结束都断言：消息队列清空、无悬挂传输、结果逐字节一致。
