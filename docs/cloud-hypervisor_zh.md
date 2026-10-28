[English](cloud-hypervisor.md) | [简体中文](cloud-hypervisor_zh.md)

# cloud-hypervisor — VMM 与平台 patches

平台用 cloud-hypervisor(CH)作为 microVM 监视器。绝大多数路径跑 upstream
行为,外部托管内存、restore-safe vsock 和可靠 VM lifecycle 通过本仓维护的
0001–0007 补丁实现。0008–0011 补丁增加 §3.8 所述的 ARM SVE 快照兼容性回移。
本文档定义 patch 范围、构建方式及对应行为契约。

## 1. 概述

### 1.1 为什么需要 patch

平台对 VMM 有四条非 upstream 的诉求:

1. **外部托管 sandbox RAM**:host 进程(sandbox-ctl)持有 memfd inode,CH 通过
   继承 fd 把同一 inode mmap 到自己地址空间——而不是 CH 自己 `memfd_create`。
   这样 sandbox-ctl 拥有内存所有权,快照时直接通过 SEEK_DATA/HOLE 扫驻留页,
   绕过 CH→file→sandbox-ctl 的中转
2. **外部 uffd handler**:sandbox-ctl 接管 sandbox RAM 缺页,实现按需从快照
   或零页填充。CH 自己创建 uffd(必须绑到 CH mm),通过 SCM_RIGHTS 把 fd 传
   给 sandbox-ctl 的 handler
3. **restore-safe vsock**:CH restore 会重建空的 Unix backend,guest 却保留
   已连接 socket。快照时必须向 guest event queue 预发布 virtio-vsock transport
   reset,恢复时重发该事件的 IRQ,并在 guest 确认清理旧连接前阻止新 backend RX
   包进入
4. **可靠 VM lifecycle**:pause、snapshot、resume 和 ordered shutdown 在数值
   `cpu.max` 与 host contention 下不能丢失 vCPU kick,也不能因 CPU 或 virtio
   worker 的旧 acknowledgement 提前返回或永久阻塞

这些组合能力在所固定的 upstream CH 版本中未直接提供。第二条尤其本质——upstream uffd handler 模型
是 CH 调外部 socket,handler 提供数据,但平台需要 handler 接管 fault 投递
路由,upstream 的 listener 模型不够。此外，从未驻留的空洞 run 可能继承父层
非零内容，balloon 仍须发送逻辑释放通知，只能跳过无效的 inode punch（§3.4）。

### 1.2 patch 范围(总结)

以下行数对应 sandboxer 提交 `c29f9af875c595df68491b50e872007e9285ef48` 的补丁集合,
是该版本的近似范围说明,不是永久的大小或兼容性保证。

| 文件 | 改动行数 | 内容 |
|------|----------|------|
| `vmm/src/vm_config.rs` | ~19 | `MemoryZoneConfig` 新增 `fd` / `uffd_socket` 字段 |
| `vmm/src/config.rs` | ~9 | `MemoryConfig::parse` 增加两个 key 解析 |
| `vmm/src/memory_manager.rs` | ~338 | fd 注入 + user_managed skip + create_ram_region 内创建 uffd + va_report sendmsg(SCM_RIGHTS) + UFFDIO_REGISTER |
| `vmm/src/seccomp_filters.rs` | ~17 | allowlist `userfaultfd` syscall + `UFFDIO_API` / `UFFDIO_REGISTER` ioctl |
| `virtio-devices/src/balloon.rs` | ~38 | balloon release 对 file-backed region 的空洞 run 跳过 `PUNCH_HOLE`/`MADV_DONTNEED`(`SEEK_DATA` 探测)|
| `virtio-devices/src/seccomp_filters.rs` | ~8 | balloon 线程 seccomp 放行 `SYS_lseek`(skip-hole 探测所需)|
| `virtio-devices/src/vsock/unix/muxer.rs` | ~20 | 持久化 host local-port 分配游标 |
| `virtio-devices/src/vsock/device.rs` / `mod.rs` | ~330 | snapshot 时发布 transport reset,restore 重发 IRQ,guest 确认前 gate RX |
| `hypervisor/src/cpu.rs` / `kvm/mod.rs` | ~110 | `KVM_SET_SIGNAL_MASK` no-miss vCPU kick,userspace 保持 blocked mask |
| `vmm/src/cpu.rs` / `seccomp_filters.rs` | ~550 | lifecycle 安全点消费 kick、请求级 ACK/deadline、KVM ioctl allowlist |
| `virtio-devices/src/device.rs` / `epoll_helper.rs` / net / vhost-user | ~280 | pause event publish/wake + resume 双向 barrier,覆盖自定义 worker |

该历史版本的 7 个 patch 文件合计 1,665 insertions / 136 deletions。当前补丁系列仍基于 cloud-hypervisor `v51.1`。

### 1.3 维护策略

- 仓库相对 patch 路径:`native-deps/deps/ch-patches/*.patch`,按编号顺序应用
- 在 `sandboxer/native-deps` 中执行 `make ch-patches-apply`(全新 `make cloud-hypervisor` 构建也会执行);
  开发循环与幂等 sanity 语义见 [Native 构建 §3](../native-deps/README_zh.md#3-patch-开发循环)
- 每次计划升级 upstream 时复核补丁,依据实际上游变化解决冲突;不假定固定发布周期或固定 rebase 工作量
- 外部 RAM/UFFD patch 的设计选择(SCM_RIGHTS in-process +
  `create_ram_region` 里跑 ioctl)与 upstream 风格偏差明显,本项目维持自有
  patch;vCPU kick 与 worker barrier 若有 upstream 等价修复,升级时应优先替换
  `0007`,不长期维护重复实现

## 2. 启用条件与命令行

CH `--memory-zone` 增加两个 key:

| key | 类型 | 含义 |
|-----|------|------|
| `fd` | int | 由父进程通过 cmd.ExtraFiles 注入的文件描述符,用作 zone 的 backing |
| `uffd_socket` | path | 普通 UDS 路径,CH 在该 socket 上 sendmsg(va_report; SCM_RIGHTS=uffd_C) |

行为契约:

- **不写 fd**(也不写 uffd_socket):内存分配沿用 upstream 的 backing-file/memfd 路径,
  不启用这里的外部 uffd handler。这仅描述内存 backing,不表示 lifecycle、vsock 等其他补丁被禁用
- **仅写 fd**:CH 把 fd 当 backing,跳过 memfd_create;snapshot 时跳过 dump;
  restore 时不 fill。这条独立成立(无 uffd 仍可外部托管)
- **fd + uffd_socket 同时写**:在 fd 行为基础上叠加,CH 内 `userfaultfd()`
  → `UFFDIO_API` → `UFFDIO_REGISTER MISSING (plus negotiated WP) on chVA` → `connect uffd_socket`
  → `sendmsg(va_report; SCM_RIGHTS=uffd_fd)` → 等 ack。ack 之前 vCPU 不允许
  跑

平台 sandbox-ctl 调用形式(冷启动 + 恢复完全相同):

```
cloud-hypervisor \
  --memory-zone id=ram0,size=8G,shared=on,fd=3,uffd_socket=/run/sandbox/<sid>/uffd.sock \
  ...
# fd=3 ← memfd from sandbox-ctl via cmd.ExtraFiles[0]
```

解析器要求 zone `id`;平台使用 `ram0`。详细命令行(冷启动 / 恢复)见 [sandbox_zh.md](sandbox_zh.md) §5.2 与 §7。

## 3. patch 提交结构

### 3.1 0001 — externally allocated memfd-backed memory zone

主题:**memory: support externally allocated memfd-backed memory zone**

改动文件:`vm_config.rs` / `config.rs` / `memory_manager.rs`

要点:

- `MemoryZoneConfig` 新增 `pub fd: Option<i32>`,当前补丁使用 `#[serde(default)]`,
  并非 `serde(skip)`。序列化的 fd 数字不是可迁移的内核引用;新的 CH 进程仍需继承相应 fd。
- `MemoryConfig::parse` 解析 `fd=N`;实际 `File::from_raw_fd` 在 memory-manager
  构建路径中接管继承的描述符,不在 CLI parser 中执行。CH 拥有这些 `File` 及其 clone;
  父进程另持自己的描述符,生命周期相互独立。
- zone 提供 fd 时,memory manager 将该 backing 传给 `create_ram_region`,不另建 memfd;
  patch 0002 再把 zone 记为 `user_managed: true`。
- 一个 zone 可含多个 region,例如较大的 x86_64 zone 跨越 PCI MMIO hole。
  每个 region 通过 `try_clone()` 获得自己的 `File` ownership;各映射及其 file offset
  仍指向同一 backing inode,共享 host page-cache 数据。

`Transportable::send` 与 `fill_saved_regions` 都遍历 `snapshot_memory_ranges`
表,**这两处不需要单独 patch**——靠 commit 0002 在 `memory_range_table` 处过滤
即可同时影响快照和恢复路径。

### 3.2 0002 — snapshot skip user-managed memory zones

主题:**snapshot: skip user-managed memory zones**

改动文件:`memory_manager.rs`

要点:

- `memory_range_table` 在生成 `snapshot_memory_ranges` 时检测每个 zone 的
  `user_managed` 标志,user_managed=true 的 zone 在表中**不出现**
- 这样 `Transportable::send`(快照 dump 路径)看到的 ranges 不含 user-managed
  zone,自然 `Ok(())` 即返回——CH **不**写 8 GiB memory-ranges 文件
- 同理 restore 路径 `fill_saved_regions` 看到的 ranges 表不含此 zone,
  fill 操作变成空操作——guest 物理地址通过 mmap 直接映到 sandbox-ctl 的 memfd
  inode;配置了外部 uffd 时,内容由 sandbox-ctl handler 按需提供

upstream 已有"file-backed + MAP_SHARED + hardlink"的等价 skip 分支(用于
share-storage live migration 场景)。我们的 user_managed zone 满足"file-backed
+ MAP_SHARED",但 memfd 没有 hardlink(`st_nlink == 0`),所以不能复用现有
分支——添加平行分支识别 `user_managed` 标志。

### 3.3 0003 — external uffd handler via in-process create + SCM_RIGHTS

主题:**memory: external uffd handler via in-process create + SCM_RIGHTS handoff**

改动文件:`memory_manager.rs` / `seccomp_filters.rs`

要点:

- `create_ram_region` 在 mmap 完成后,如果 zone 配了 `uffd_socket`:

  ```
  uffd = userfaultfd(O_CLOEXEC | O_NONBLOCK)
  UFFDIO_API features = UFFD_FEATURE_MISSING_SHMEM
                      | UFFD_FEATURE_EVENT_REMOVE
                      | UFFD_FEATURE_EVENT_UNMAP
                      | UFFD_FEATURE_THREAD_ID
  UFFDIO_REGISTER(uffd, [chVA, +size], MISSING)
  conn = connect(uffd_socket)
  sendmsg(conn, iov=va_report{chVA, size}, cmsg=SCM_RIGHTS([uffd]))
  recvmsg(conn, expect ack)
  // 回到 create_ram_region 主流程,继续 vCPU 启动
  ```

- feature bits **必须严格按内核头位置**:

  ```
  UFFD_FEATURE_MISSING_SHMEM = 1 << 5
  UFFD_FEATURE_EVENT_REMOVE  = 1 << 3
  UFFD_FEATURE_EVENT_UNMAP   = 1 << 6
  UFFD_FEATURE_THREAD_ID     = 1 << 8
  ```

  位置写错会被内核解读成别的 feature。例如 `1 << 1` 是 `EVENT_FORK`,要求
  `CAP_SYS_PTRACE`,权限不足时可能导致 UFFDIO_API 返回 EPERM。

- seccomp 放行:`SYS_userfaultfd` syscall + `UFFDIO_API` / `UFFDIO_REGISTER`
  ioctl(filter 在 `seccomp_filters.rs`)

**为什么 uffd 必须 CH 内创建**:Linux 内核把 uffd 上下文绑到 `userfaultfd()`
调用进程的 mm。sandbox-ctl 创建的 uffd 上 `UFFDIO_REGISTER` 只能 register
sandbox-ctl mm 的 VMA,不能 register CH mm 的 chVA。所以 uffd 必须在 CH 内
创建,然后通过 SCM_RIGHTS 把 fd(而非内核内部对象)传给 sandbox-ctl 让它读
事件。fd 表是进程级的,但 uffd ctx 的事件路由按创建者 mm 的 VA 解析,
sandbox-ctl 收到 fd 后读出来的事件 va 就是 CH 视角的 chVA。

**惰性恢复中的后端首次访问。** 后端 mmap 有意不注册 UFFD。普通 shmem
访问可能直接创建零页，而未读取快照字节，因此读取或只写一个状态字节也必须先准备
内存。所有 vhost GPA/UVA 地址转换都在返回切片前调用带上下文的
`EnsureLoaded(ctx, memfdOffset, length)` 屏障，覆盖描述符、间接描述符表、
avail/used ring、请求头、两个方向的数据和状态。缺少回调、范围无效或填充失败都
使队列终止，不能发布 used-ring 完成记录。所有磁盘通过统一的服务器创建路径绑定
该回调，CLI 与 SDK 的冷启动和恢复均使用此路径。队列重置取消待完成加载，等待
worker 退出后才替换内存表。

屏障检查所有相交页，包括部分访问之外的原有字节。Loaded 范围批量检查状态，
保留实时内容而不读取快照。Absent 页由现有 fault worker 使用其私有 4 KiB
缓冲区读取权威来源。Released 页永不读取历史快照内容。整页安装使用所属 CH
注册的 `UFFDIO_COPY`，shmem 需要时也通过零缓冲区 COPY。`EEXIST` 保留已经
存在的页并唤醒等待者；它**不证明**此前无保护的后端访问曾恢复快照。必须校验
实际完成字节数。`EAGAIN` 在 worker 外可取消地重试，使事件读取和 CPU fault
队列能够推进；`ENOENT` 或无效注册使请求失败。真正的 CPU MISSING fault
遇到 Loaded 页仍走独立的已回收页处理路径。

`va_report` 的线格式不变。已计算的 memfd 偏移通过内部回调传递，并与实际 CH
VA 区间及持有的 UFFD fd 关联。只有成功接管后，新注册才可用于加载；接管失败
回滚暂存的地址映射，由回调调用方清理 fd。加载可取消地等待注册就绪，并在区域
边界拆分。UNMAP 使对应注册区间失效；REMOVE 释放页内容但保留注册。

安装互斥锁协调 CPU urgent、后端 mandatory、speculative tail 的 ioctl 与
非阻塞内核事件读取和 REMOVE/UNMAP 状态发布。来源读取后、实际安装前再次校验，
防止在观察到释放后安装过期快照缓冲区。来源读取、重试等待、注册等待和可能阻塞的
队列发送均不持有此锁。保留普通数据和 chunk 的有界 tail 批量填充。
锁顺序审查参考 [Linux 5.10 的 UFFD 事件与 COPY 路径](https://github.com/torvalds/linux/blob/v5.10/fs/userfaultfd.c)：
REMOVE 在等待用户态前释放 mmap 读锁，映射变化期间 COPY 可以返回 `EAGAIN`。
此锁**不是 DMA pin**：要求 guest 正常持有未完成描述符的缓冲区；屏障返回后任意
并发 balloon/discard 不在此保证范围内。

Close 同步关闭准入和取消操作，等待调用者及所有 worker 退出，并通过有界 epoll
轮询停止 reader，之后才关闭 UFFD fd。来源和后端映射必须存活至上述等待完成。
启动早期失败也会取消就绪与握手等待。此方案不增加第二套 UFFD，不使用
MINOR/CONTINUE，不预先填充整块 RAM，不提高内核最低版本，也不改变资源策略。

可重复验证包括 `pkg/vhost` 中的
`TestRealMissingShmemRestoredUsedRingPreservesUntouchedBytes`：两个共享 memfd
别名、真实的 CH 侧 MISSING_SHMEM 注册及实际 `publishUsed` 路径，在 CPU-first
和 backend-first 两种顺序下都必须保留周围字节，并把保存的索引从 7 推进至 8。
UFFD 和地址转换测试覆盖范围、取消、注册、释放及并发失败。现有
`snapshot.restore.sh` E2E 还在快照/恢复之间保留 O_DIRECT 文件描述符和 32 个
匿名缓冲区。在页内偏移 512 处读取 512 字节后，其余 3,584 字节必须保持原值。
该用例包含冷启动对照、DMA 前用户态未访问的十六个候选页，以及恢复后显式通过
CPU 预热的十六个对照页；`dio-before.json`/`dio-after.json` 保留完整性结果。
guest 的 pinning/prefetch 可能填充候选页，因此 E2E 本身不能证明 backend-first
顺序；真实 used-ring 测试提供确定的访问顺序。应遵守既有准备来源/产物契约，
结合现有 KVM 内存压力覆盖执行两类测试，并验证 Linux 5.10 最低版本。
这些是验证要求，不代表尚未执行的测试已经通过。

### 3.4 0004 — balloon 空洞探测（当前顺序由 0013 修正）

0004 引入 file-backed RAM 的 SEEK_DATA 探测及 balloon 线程 `SYS_lseek`
seccomp 放行。0013 保留探测，但不再跳过空洞区间的 chVA `MADV_DONTNEED`：
restore 的未物化 Hole 可能仍继承父层非零内容，逻辑释放必须先通知 handler。

当前 release 顺序为 **chVA MADV_DONTNEED → SEEK_DATA → 必要的 PUNCH_HOLE**。
即使整个区间已稀疏，也先执行 MADV。handler 持有 installMu 读取 REMOVE
并发布 R/Released；内核 ACK 可以早于用户态发布，但填充和 CaptureView
不能越过这个发布边界。探测只能避免 inode 的无效 punch，不能省掉逻辑释放事件。MADV 或 punch 失败走错误路径；SEEK_DATA 返回 ENXIO 表示没有 Data，其他 SEEK 错误则保守继续 PUNCH_HOLE。已累计 R/D
不回退。handler 在 installMu 下同时读取事件、记录 R/D 和发布 Released 状态，
从而让后台填充和 CaptureView 在事件读取与状态发布之间无法进入。

### 3.5 0005 — 持久化 vsock host local-port 游标

CH 的 Unix vsock backend 在 restore 时重新创建。若 host local-port 分配器回到
`0x40000000`,首个 host-init 连接可能复用 guest 快照里仍存在的四元组并收到 RST。
patch 把 `local_port_last` 纳入 `VsockState`,恢复 backend 后从下一端口继续分配。
该状态在嵌套快照中逐层保存,不是只覆盖一次 restore 的进程内游标。

### 3.6 0006 — snapshot 时预发布 vsock transport reset

仅避免端口复用仍不完整:CH 不序列化 backend connection map,而 guest 内核会把
连接 socket、credit 与关闭状态一并带入快照。restore 后两端因此处于不同 transport
epoch。restore 的 guest RAM 又由外部 userfaultfd 惰性恢复,设备 `activate` 阶段读取
event virtqueue 页可能只看到尚未 fault-in 的空 avail ring,不能在这里要求新 descriptor。
patch 执行以下协议:

1. VM 已暂停后,`Vsock::snapshot` 从**源 VM 的 live event virtqueue**取一个 guest
   提供的 writable descriptor,写入 `VIRTIO_VSOCK_EVENT_TRANSPORT_RESET`,推进 used
   ring 并置 `transport_reset_pending`。普通 `/vm.pause` 不发布 reset。
2. pending 状态随 `VsockState` 持久化。源 VM resume 与目标设备 restore activation
   都只重发该 used-ring event 的 IRQ;restore 不再读取或消费 event descriptor。
3. pending 期间 backend 可接收 host UDS,但所有 RX packet 都留在 backend,不能进入
   guest RX queue。
4. Linux guest 处理 reset:关闭 connected sockets、重新读取 CID,listener 保持
   bind/listen;随后补回 event descriptor 并 kick event queue。CH 把该 kick 作为
   acknowledgement,解除 RX gate 并立即排空 pending RX。

snapshot staging 会先排空 reset 之前已经到达的 eventfd kick。设备线程 resume 后若
同一批 epoll 仍带着该旧通知,非阻塞 read 返回 `EAGAIN` 并保持 gate,不会把旧 kick
误当成新 reset 的 acknowledgement。

因此首个 `restore` 控制连接可以在 VM resume 后立即发起,但其 REQUEST 必定在 guest
完成旧连接清理后才可见。这里没有 retry、sleep 或放宽 deadline。源 VM snapshot
阶段若 descriptor 缺失或非法会明确失败;目标 restore activation 即使没有任何新
available descriptor 也必须成功,因为 reset 已经存在于快照的 used ring 中。

### 3.7 0007 — VM pause/resume 与 ordered shutdown 可靠性

CH v51.1 用 no-op `SIGRTMIN` handler 中断 `KVM_RUN`:控制线程先发布 pause/kill
状态,再 `pthread_kill` 每个 vCPU,等待 vCPU loop 写入 acknowledgement。若 signal
在 vCPU 检查状态之后、进入下一次 `KVM_RUN` 之前到达 userspace,handler 返回后该
vCPU 仍可阻塞在 `KVM_RUN`;每 10 ms 重发只能降低概率,不能关闭竞态窗口。

patch 通过三组相互独立但同属 lifecycle barrier 的修复关闭该问题:

1. 创建线程在 spawn vCPU 前配置 `KVM_SET_SIGNAL_MASK` 并阻塞 kick signal,使子线程
   从第一条指令起继承 blocked mask;配置失败直接返回,不会让一个提前退出的 vCPU
   留在启动 barrier 之外。vCPU loop 在读取 lifecycle 状态前保持 signal blocked;
   `KVM_RUN` 通过已配置的 mask 原子 unblock,退出时恢复 blocked mask。signal 在状态
   检查与 `KVM_RUN` 之间到达会保持 pending 并中断同一次调用;KVM 返回后处理 PIO/MMIO
   等 userspace exit 时仍不消费该 signal。outer loop 观察到对应 lifecycle 请求并完成
   必要的 pending KVM I/O 后,才短暂执行 `SIG_UNBLOCK`/`SIG_BLOCK`,由空 handler 消费
   本次 kick 并立即恢复 blocked mask,避免后续 `KVM_RUN` 因旧 pending signal 持续
   立即返回。VMM seccomp 的公共 KVM ioctl 集显式放行 `KVM_SET_SIGNAL_MASK`。
2. pause、shutdown、NMI 和 vCPU removal 在发布请求前清除旧 ACK;vCPU 只有进入当前
   请求的 pause/NMI/kill 分支后才写 ACK,自然 reset、shutdown、run error 或 panic
   不能冒充请求确认。KVM no-miss 路径每个请求只发一次 kick;pause/NMI 分支在 ACK 前
   消费已 pending 的本次 kick,并在 park/unpark 后再次消费,覆盖 vCPU 先看到共享状态、
   控制线程后发送 kick 的顺序。NMI 使用 ACK set/clear 双向 barrier,控制线程在所有
   vCPU 清除本次 ACK 后才返回,
   因此前一请求的 drain 不会吞掉下一次 pause 的 kick。已经自然结束但尚未 join 的
   vCPU thread 由 `JoinHandle::is_finished()` 从 signal barrier 排除,其 ACK 仍保持
   false,避免正常 shutdown 清理等待一个不可能到达的 ACK。pause 失败会撤销请求、
   unpark 并等待已参与 vCPU 恢复;NMI 即使 signal 超时也会完成 ACK-clear cleanup,
   两条错误路径都使用独立的新 deadline。非 KVM backend 保留 10 ms retry。所有等待
   统一使用 `CLOCK_MONOTONIC` 语义的 1 s deadline。
3. runtime pause 先发布共享 pause event,再显式 unpark 所有普通与自定义 worker,
   关闭 worker 的 zero-time epoll poll 到 `thread::park` 之间的启动竞态。resume 先
   drain 该 event 再唤醒 worker;worker 从 park 恢复后参加第二次 barrier,control
   thread 收齐 resume ACK 才允许下一次 pause。net 与 vhost-user 的自定义 worker
   handle 同时加入 pause wake 和 resume barrier。恢复态设备若尚未收到 runtime
   pause event,不会等待一个不存在的 ACK。

该 patch 不改变 sandbox 配置、CH HTTP API、snapshot 格式或资源协议,也不在
lifecycle 前后修改 `cpu.max`。超时仍是最终有界失败保护,不是竞态修复方法。

### 3.8 0008–0011 — ARM SVE 快照寄存器

启用 SVE 的 ARM 主机通过 KVM 的 SVE Z/P/FFR 寄存器暴露向量状态。
访问旧 FPSIMD V 寄存器偏移会返回 `EINVAL`;因此 VM 可以正常冷启动运行,
但在捕获快照时失败。

0008–0010 回移 Ruben Hakobyan 已合并的
[Cloud Hypervisor PR #8268](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/8268),
保留原作者与签署。审阅的上游提交为
`083b41905ea59b335c4f211c4186312e30bbf022`、
`e9d044730524d12e243737a1888767b3edf123c8` 和
`4fc414cc8bc56a4c5275b3dd44610e5c75dfbbd1`;上下文适配 v51.1 与 0001–0007。
补丁枚举 KVM 寄存器、保存 SVE 状态,并在 vCPU 初始化之后、SVE finalization
之前恢复向量长度选择。不启用 SVE 时仍使用原有向量路径。

0011 额外为两条路径保留 FPSR/FPCR:向量寄存器族改变时,这些控制和状态寄存器
仍然可访问。补丁包含设置非零寄存器状态的原生 KVM 往返测试:

```sh
cd native-deps/build/src/cloud-hypervisor
cargo test --locked -p hypervisor --features kvm test_arm_ -- --ignored --nocapture
```

这些测试需要原生 ARM KVM(SVE 测试还需要 SVE);ignored 标记不代表在没有
`/dev/kvm` 的托管 ARM runner 上验证成功。平台最终验收还使用所选已发布的
Cloud Hypervisor 二进制执行真实冷启动、guest exec、快照与恢复。

快照 CPU 状态新增带默认值的 pre-finalize 与 extended-register 数组,可读取旧的
非 SVE 状态,但不承诺跨主机 CPU 特性兼容,也不承诺向未打补丁的 VMM 反向恢复。
SVE 保持启用,不兼容寄存器状态通过已有 KVM 错误失败。

### 3.9 0012 — 在 vCPU 前恢复 KVM 时钟

补丁 0012 回移上游 [commit ff20f183647c602bf5e5028d99959e5547a7dd84](https://github.com/cloud-hypervisor/cloud-hypervisor/commit/ff20f183647c602bf5e5028d99959e5547a7dd84)，
保留 CMGS 的作者归属和 sign-off。固定的 v51.1 恢复路径可能在还原已保存的 x86
KVM 时钟前执行 vCPU。回移后先还原时钟，再恢复设备，最后恢复 vCPU。时钟操作仍仅
适用于 x86；设备先于 vCPU 的顺序也适用于 ARM。快照 schema、Guest ABI 和固定的
上游版本均不变。

验证前应应用完整编号补丁序列并重建原生 VMM。补丁内容参与 native build/cache
identity；不具备对应身份的既有二进制不能用于验证回移。重复执行单 vCPU 的
`sandbox.memory-budget` 恢复、pause/wake，以及保留当前 direct-I/O 完整性断言的
`snapshot.restore`，之后执行普通全量 E2E。记录原生架构和产品来源；仅交叉编译不会
执行真实 KVM resume。小样本成功不能确定故障率，也不能据此诊断其他恢复错误。

### 3.10 0013–0014 — 累计 dirty memory 与原子 clean 填充

每个外部托管 RAM 实例初始化时就启用 MemoryManager 级 KVM dirty logging，
先消费并累计已有软件 bitmap，不调用会清空软件记录的 VM migration hook。
`GET /api/v1/vm.dirty-memory` 返回 version=1、page_shift=12 以及每个 slot 的
GPA、size、memfd file_offset 和累计 u64 bitmap。各来源/slot 消费后立即 OR 到
实例累计集合，再处理下一个 slot 或返回响应；失败 slot 先全 dirty 再报错。
响应丢失或重复采集不清空累计集合。Go 端使用完整、有界 HTTP 读取并验证 region
覆盖/重叠/偏移与尾部无效位后转换成 memfd 坐标。net/vsock 原始可写别名和 Go
vhost 的 DMA/status/used-ring 写入在暴露缓冲区前标记范围，部分失败不会撤销记录。

0014 在消费日志前先检查完整坐标映射；注册变化时先把全部已知 region 保守标脏再报错。
WP 遇到 EAGAIN 后立即归还 worker，以按页去重、RAM 页数有界的小批轮转队列重试，
避免满 WP 队列阻止 reader 读取 REMOVE/UNMAP；每次重试仍先校验注册并记 D。

va_report v1 显式传 page_shift、file_offset 和 copy_mode_wp，ACK 回显协商结果。
x86 有 shmem WP 能力时注册 MISSING|WP，所有 clean Data、Zero、父 Hole、tail
和 EnsureLoaded 填充都原子使用 COPY_MODE_WP。WP fault 单独分支先标记 D 再
unprotect，不走 missing refill。4 KiB ARM 使用 MISSING 和原生 KVM 日志；x86
能力不足时将物化页保守计 dirty 并记录退化。更大 host page 被明确拒绝。

捕获在 pause、backend quiesce、CH snapshot 写入及日志采集后取得固定 M/D/R。
UNMAP、registration 缺口、事件流损坏、Loaded 无 release 却变 Hole 均使捕获
报错。D/R 不因捕获成功、失败或 merge_ref 切换清空；生命周期和 exec/stdio 屏障
仍沿用既有流程。S 编码及历史裁剪见 [sandbox.md 中文版](sandbox_zh.md#snapshot-format)。

## 4. 构建工作流

`sandboxer/native-deps` 从固定 v51.1 tarball 构建,应用
`git am deps/ch-patches/*.patch`,再执行 `cargo build --release --locked`。
输出为 `native-deps/bin/<arch>/cloud-hypervisor`;从 sandboxer 根目录执行
`make cloud-hypervisor` 还会同步到 `bin/<arch>/cloud-hypervisor`。
构建时间取决于工具链、机器和缓存。

构建使用 `--remap-path-prefix` 将源树映射为相对路径、Cargo 依赖映射为 `/cargo`,
避免把这些构建机绝对源路径嵌入 panic 消息和 DWARF。

`ch-fetch` / `ch-patches-format`、幂等检查、产物同步与边界见
[Native Build 文档](../native-deps/README_zh.md)。

## 5. 启动协议(per-arch)

CH 自适应启动协议,sandbox-ctl 命令行不区分 arch:

| arch | 协议 | kernel 入口 | CH 准备 |
|------|------|-------------|---------|
| x86_64 | PVH | ELF entry,zero page 由 CH 填 | memmap(E820) + cmdline + ACPI 表 |
| aarch64 | EFI stub + ACPI | PE Image start,EFI stub 解析 ACPI | UEFI memmap + ACPI 表 + GICv3 节点 |

### 5.1 设备模型(平台用法)

CH 暴露给 guest 的设备清单(冷启动):

```
virtio-pmem    → sandbox-runtime.bundle (DAX, MAP_SHARED 共享 host page cache)
virtio-blk × 2 → blk0 (base, ro) + blk1 (overlay COW, rw),vhost-user backend
virtio-net     → 可选;配置网络源时为 eth0,host TAP 后端;无源时不创建设备
virtio-console → hvc0,内核 dmesg;--console tty(写到 CH 进程的 stdout = sandbox-ctl
                 给的匿名管道),--serial off(无 8250 UART)。CH 进程的 stdin=/dev/null
                 故 CH 不 raw 化任何宿主终端。应用 stdio 不走此设备(走 vsock MUX)
virtio-vsock   → CID=3。控制面短连接(launch / ping / app_started / app_exited /
                 mem_report / quiesce / restore / attach)+ launch/restore/attach 那条
                 连接握手后升级而成的应用 stdio MUX(详见 sandbox-init_zh.md §4)
virtio-balloon → size=<cold InitialTarget> [+ deflate_on_oom=on];sandbox-local
                 BalloonController 通过 /vm.resize 推 target,并以 vm.info 的
                 memory_actual_size 观察 current(见 [sandbox_zh.md](sandbox_zh.md) §9.3);free_page_reporting
                 不启用(mmu_notifier 广播压力可能影响 guest vsock 进展)
virtio-mem     → host-driven 主动 unplug(CH 能力;当前固定 Capacity Budget 模型不启用)
```

上面的两块 virtio-blk 描述 root base/overlay 对;配置数据盘时还会增加对应设备槽位。

restore 沿用 `config.json` 中的设备拓扑,不能新增或删除 virtio-net.sandboxer 在
启动 CH 前要求 host restore 配置是否提供网络源与快照中的 NIC 是否存在一致.

恢复路径设备拓扑通过 `--restore source_url=<state.json dir>` 从 snapshot
state 还原,不需要重新指定 `--kernel` / `--vsock`。

详细命令行示例与冷启动/恢复差异见 [sandbox_zh.md](sandbox_zh.md) §5(冷启动
数据流)与 §7(恢复数据流)。

### 5.2 vsock hybrid 代理

vsock 在 host 端通过 hybrid 代理映射到 UDS:

```
guest VM (CID=3) → CID=2 (host) → CH 把流量转发到
  /run/sandbox/<sid>/vsock.sock_<port>     guest → host 方向(host 在该 UDS 上 listen)
  /run/sandbox/<sid>/vsock.sock + "CONNECT <port>\n" 行    host → guest 方向(guest 在 port 上 listen)
```

host → guest 方向需要在第一笔写入发 ASCII `CONNECT <port>\n`,CH 回一行
`OK <local_port>\n`(host 须先排空再读后续 payload),之后 CH 把流量代理到 guest
对应 port 的 listener。两个方向的连接对 CH 而言都是普通字节流——`launch` /
`restore` / `attach` 这三种连接在应用层握手后由 sandbox-ctl / sandbox-init 自行
转入帧收发态(stdio MUX),CH 不感知。详细见 [sandbox_zh.md](sandbox_zh.md) §5.2 与
[sandbox-init_zh.md](sandbox-init_zh.md) §4.2。

## 6. 行为契约总结

平台代码(sandbox-ctl + node-ctl)依赖的 CH 行为(包含 patch):

| 行为 | upstream | patched |
|------|----------|---------|
| 不写 `fd=` 时的内存 backing | upstream memfd/file 路径 | 同一分配路径;不表示其他补丁禁用 |
| 命令行写 `fd=` | ✗(parse 错误) | ✓(用 fd mmap,标记 user_managed)|
| 命令行写 `fd=` + `uffd_socket=` | ✗ | ✓(创建 uffd + sendmsg + 等 ack)|
| `/vm.snapshot` 对 user_managed zone | (不适用) | 跳过 dump,memory-ranges 表无此 zone |
| `/vm.restore` 对 user_managed zone | (不适用) | 跳过 fill;配置 uffd 时由映射缺页触发外部 handler |
| balloon release 对 file-backed region 的空洞 run | 原 release 路径 | 先 `MADV_DONTNEED` 再探测；只跳过无效的 `PUNCH_HOLE` |
| balloon `deflate_on_oom=on` | ✓(v51.1 已就绪) | ✓ |
| `vm.resize` `desired_balloon` | ✓ | ✓(sandbox-local BalloonController 调用)|
| virtio-mem `vm.resize` | ✓ | ✓ |
| vsock local-port cursor 跨 restore | ✗ | ✓(`VsockState.local_port_last`) |
| vsock backend/guest transport epoch 对齐 | ✗ | ✓(reset event + RX acknowledgement gate) |

平台**不**使用 `free_page_reporting`。在统一 memfd / 外部 uffd 模型下,持续
`madvise(MADV_DONTNEED)` 可能产生 mmu_notifier/EPT 与 IPI shootdown 压力,
影响 guest vsock 进展(机理与替代反馈环见
[Guest Kernel §5.5](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/vmlinux_zh.md#55-为什么不启用-free_page_reporting以及-virtio_mem-的角色))。改由 sandbox-local BalloonController
经 `/vm.resize` 推 inflate target;steady shrink 每份 fresh report 最多一个
64MiB step。冷启动命令行 target
覆盖的稀疏区间也先执行 `MADV_DONTNEED`，再由 SEEK_DATA 探测避免无效 inode
punch；已驻留区间仍执行必要的 PUNCH_HOLE（§3.4）。
`deflate_on_oom` 是 upstream v51.1 原生,无需新 patch。

## 7. 已知限制

- **包括 v52 及之后的 upstream 升级**:依据实际 `vmm/src/memory_manager.rs` 变化
  复核 0001/0002;0003 的 uffd ioctl 需要重点检查。0004 同时修改 balloon release
  函数及 seccomp allowlist。0005/0006 随 vsock queue/restore lifecycle 变化复核;
  0007 与可靠的 upstream immediate_exit 实现一起评估,不承诺固定 rebase 工作量。
- **vCPU kick 掩码开销**:KVM vCPU 每轮 userspace exit 仍执行一次幂等 signal block;
  unblock/reblock 只发生在已观察 lifecycle 请求的安全点。CPU-bound guest 不增加
  VM exit;PIO/MMIO 密集负载会承担额外 syscall 成本。该路径用于在 sound
  `immediate_exit` 接口可用前保持无漏唤醒语义
- **多 fd-backed zone**:当前限定单 zone(整段 sandbox RAM 一个 memfd)。多
  zone(NUMA / virtio-mem 横向扩展)需要在 patch 0003 处对每 zone 各自 sendmsg
  一次,sandbox-ctl 端各自维护 addrMap

## 8. See Also

- [sandbox_zh.md](sandbox_zh.md) §5(冷启动数据流,§5.2 CH 命令行)/ §7(恢复
  数据流)—— sandbox-ctl 怎么用 patched CH 跑沙箱;命令行示例
- [sandbox_zh.md](sandbox_zh.md) §8(uffd handler)—— sandbox-ctl 接收到 uffd_C
  之后如何处理 fault 事件
- [Guest Kernel](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/vmlinux_zh.md) —— guest kernel 如何配合 CH 启动
  协议(PVH / EFI stub)
- [Native 构建](../native-deps/README_zh.md) —— `make cloud-hypervisor` 工作流与 patch
  开发循环
- [系统总览 §3.2](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox_zh.md#32-组件职责) —— VMM 与 Guest 环境在系统中的位置
