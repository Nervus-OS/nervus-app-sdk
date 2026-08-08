package com.nervus.sdk.ipc.connection

/**
 * 本 SDK 说得上话的线协议版本。
 *
 * 【这里不给可配置的入口】。曾经它是 NervusClient / NervusServiceHost 构造参数
 * 上的默认值，任何调用方都能改。协议版本不是应用的选择——SDK 编译进去的
 * Envelope 类长什么样，它就只能说那一版，改这个数字不会让它多懂一个字段，
 * 只会让握手成功之后开始误解对方。
 *
 * 【下界也是 2，不是 1】。写 min = 1 的话，一个 v1 的 nervud 会握手成功，
 * 然后在语义上悄悄跟我们分道扬镳：v1 允许 ResolveEndpoint 留空 selector 并隐式
 * 取 {nervus.resource.motion.base, main}，v2 把那条默认删了。同一份代码在两版
 * 内核上会拿到不同的设备，而且两边都不报错。宁可连不上。
 *
 * V2 与 V1 不兼容，没有兼容层。
 */
internal const val PROTOCOL_MAJOR_MIN = 2
internal const val PROTOCOL_MAJOR_MAX = 2
internal const val PROTOCOL_MINOR_MAX = 0
