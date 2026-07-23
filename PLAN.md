# nervus-app-sdk — IPC SDK 实施计划

## 项目信息

| 项目 | 值 |
|---|---|
| Git 仓库 | `github.com:Nervus-OS/nervus-app-sdk.git` |
| Kotlin 包名 | `com.nervus.sdk` |
| Gradle 类型 | 单项目（非 multi-module） |
| JDK 目标 | 17 LTS |

## 依赖策略

编译时临时 clone `nervus-ipc` 仓库读取 protocol 生成类型，编译结束后删除 clone。不复制文件、不引用本地路径、无 submodule、无 Maven 坐标。

```kotlin
// build.gradle.kts 核心逻辑
val cloneProtocol by tasks.registering(Exec::class) { ... git clone --depth=1 ... }
kotlin.sourceSets.main { kotlin.srcDir(clone 内的 jvm/protocol/src/main/kotlin) }
java.sourceSets.main  { java.srcDir(clone 内的 jvm/protocol/src/main/java) }
build.finalizedBy(cleanProtocol)
```

## 目录结构

```
nervus-app-sdk/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
├── gradle/libs.versions.toml
├── gradle/wrapper/
└── src/
    ├── main/kotlin/com/nervus/sdk/
    │   ├── ipc/                          ← IPC SDK 独立包
    │   │   ├── NervusClient.kt
    │   │   ├── NervusServiceHost.kt
    │   │   ├── connection/
    │   │   │   ├── FrameReader.kt
    │   │   │   ├── FrameWriter.kt
    │   │   │   ├── UnixDomainSocket.kt
    │   │   │   └── HelloHandshake.kt
    │   │   ├── rpc/
    │   │   │   ├── RequestIdGenerator.kt
    │   │   │   └── PendingMap.kt
    │   │   ├── endpoint/
    │   │   │   └── EndpointCache.kt
    │   │   ├── event/
    │   │   │   └── SubscriptionManager.kt
    │   │   └── dispatch/
    │   │       └── DispatchHandler.kt
    │   ├── annotation/                   ← @Function 等（后续）
    │   └── ui/                           ← Compose 适配层（后续）
    └── test/kotlin/com/nervus/sdk/ipc/
        ├── FrameReaderWriterTest.kt
        ├── PendingMapTest.kt
        ├── RequestIdGeneratorTest.kt
        ├── EndpointCacheTest.kt
        └── InMemoryRoundTripTest.kt
```

## 实施步骤

### Step 1 — 项目脚手架

- `settings.gradle.kts`（rootProject.name = "nervus-app-sdk"）
- `build.gradle.kts`（Kotlin JVM 插件、cloneProtocol task、编译时 source 引用、构建后清理）
- `gradle/libs.versions.toml`（Kotlin 2.1.0, protobuf 4.29.3, kotlinx-coroutines 1.9.0）
- 初始化 Gradle wrapper

**验证**：`./gradlew build` 编译通过（含 protocol 生成类型）

### Step 2 — connection 模块

| 文件 | 职责 |
|---|---|
| `FrameReader.kt` | 读满 4 字节 → BE uint32 N → 校验 1 ≤ N ≤ 128KiB → 读满 N 字节 → Envelope.parseFrom |
| `FrameWriter.kt` | Envelope.toByteArray → BE uint32 len + bytes → 单次 write |
| `UnixDomainSocket.kt` | JDK 16+ UnixDomainSocketAddress + SocketChannel 封装 |
| `HelloHandshake.kt` | 发送 Hello → 接收 HelloAck → 校验版本交集 → 返回 ConnectionLimits |

**测试**：半包、粘包、零长度、128KiB 边界、畸形 varint

### Step 3 — rpc 模块

| 文件 | 职责 |
|---|---|
| `RequestIdGenerator.kt` | AtomicLong 从 1 递增，到达 Long.MAX_VALUE 抛异常，绝不绕回 |
| `PendingMap.kt` | ConcurrentHashMap<Long, CompletableFuture<Response>>，断线时全部 complete UNAVAILABLE |

**测试**：CAS 正确性、MAX_VALUE 边界、完成/超时/取消/断线清空

### Step 4 — endpoint 模块

| 文件 | 职责 |
|---|---|
| `EndpointCache.kt` | ResolveEndpoint 结果缓存（endpoint_id + interface_version + schema_hash），断线全失效 |

**测试**：缓存命中/未命中、断线清除、重连后重新 Resolve

### Step 5 — Client

| 文件 | 职责 |
|---|---|
| `NervusClient.kt` | connect()（握手）→ call(endpoint, method, payload, timeout) 完整流程：生成 request_id → 登记 pending → 写入 Request → reader 协程等待 Response |

**测试**：InMemoryRoundTripTest（管道对模拟完整 RPC 往返）

### Step 6 — event 模块

| 文件 | 职责 |
|---|---|
| `SubscriptionManager.kt` | Subscribe/Unsubscribe，Event 按 subscription_id 分发到 Flow |

**测试**：subscribe → Event 推送 → unsubscribe → 不再接收

### Step 7 — ServiceHost

| 文件 | 职责 |
|---|---|
| `NervusServiceHost.kt` | RegisterEndpoint → reader 循环接收 Dispatch → 调用 DispatchHandler → 写回 DispatchResult |
| `DispatchHandler.kt` | method_id 路由接口 |

**测试**：注册 → 接收 Dispatch → 回复 → 验证 Response 回到调用方

### Step 8 — 集成测试

| 场景 | 验证点 |
|---|---|
| Cancel | 调用方发 Cancel → Service 收到 CancelDispatch → 返回 CANCELLED |
| 断线 pending 清空 | 连接断开 → 全部 pending future 收到 UNAVAILABLE |
| 重复 request_id | 同一连接重复 request_id → 协议违规关闭连接 |
| 超时 | timeout_ms 耗尽 → DEADLINE_EXCEEDED |
