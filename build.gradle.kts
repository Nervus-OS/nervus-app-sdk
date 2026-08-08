plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.compose)
}

group = "com.nervus.sdk"
version = "0.1.0"

kotlin {
    jvmToolchain(21)
}

val ipcRepoPath = run {
    val parent = rootProject.projectDir.parentFile
    if (parent != null) {
        val p = parent.resolve("nervus-ipc")
        if (p.isDirectory) p else null
    } else null
}

if (ipcRepoPath == null) {
    throw GradleException("nervus-ipc not found as sibling directory")
}

val cloneDir = rootProject.projectDir.resolve(".nervus-ipc-clone")

val cloneProtocol by tasks.registering(Copy::class) {
    description = "Copy nervus-ipc protocol sources for compilation"
    // 只拷生成的协议源码，不再拷整个仓库：整仓拷贝会把 .git、build/、
    // python 产物一并搬进来（几百 MB），而编译只需要 jvm/protocol/src/main
    from(ipcRepoPath!!.resolve("jvm/protocol/src/main"))
    into(cloneDir.resolve("jvm/protocol/src/main"))

    // 【不要加 onlyIf { !cloneDir.isDirectory }】。
    //
    // 那样写会让副本在首次构建后【永不更新】：nervus-ipc 里新增的消息在这边
    // 永远看不见，症状是「proto 明明加了、Kotlin 就是 Unresolved reference」，
    // 而且删掉 build/ 重来也没用（副本不在 build/ 下）。已经真实踩过一次 ——
    // LaunchComponent 加进 ipc 之后 SDK 编译不过，就是它。
    //
    // Copy 任务本身有增量支持：输入没变时 Gradle 自动跳过，不需要手写 onlyIf。
    doLast {
        logger.lifecycle("nervus-ipc protocol sources synced to $cloneDir")
    }
}

val cleanProtocol by tasks.registering(Delete::class) {
    description = "Clean cloned nervus-ipc repository"
    delete(cloneDir)
}

val protocolSrcDir = cloneDir.resolve("jvm/protocol/src/main")
kotlin.sourceSets.main {
    kotlin.srcDir(protocolSrcDir.resolve("kotlin"))
}
java.sourceSets.main {
    java.srcDir(protocolSrcDir.resolve("java"))
}

tasks.named("compileKotlin") {
    dependsOn(cloneProtocol)
}

tasks.named("compileJava") {
    dependsOn(cloneProtocol)
}

tasks.named("compileTestKotlin") {
    dependsOn(cloneProtocol)
}

tasks.named("processResources") {
    dependsOn(cloneProtocol)
}

tasks.named("clean") {
    dependsOn(cleanProtocol)
}

// 【javac 必须显式指定 UTF-8】。不指定它就用平台默认编码，在中文 Windows 上
// 是 GBK——而 nervus-ipc 生成的 Java 里全是 UTF-8 中文注释，于是每次构建刷出
// 几百行 "unmappable character for encoding GBK"，注释在 class 文件里变成乱码。
// 更要命的是这行为【跟着开发机的 locale 走】：同一份代码在 CI 上干净，在某个
// 人的机器上刷屏，甚至可能因为某个字符恰好落在 GBK 的非法序列上而直接编译失败。
tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

dependencies {
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.kotlin)
    implementation(libs.kotlinx.coroutines.core)
    implementation("org.jetbrains.kotlin:kotlin-reflect:${libs.versions.kotlin.get()}")

    implementation(compose.desktop.currentOs)
    implementation(compose.material3)

    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(libs.compose.ui.test.junit4)
    testImplementation(compose.desktop.currentOs)
}

tasks.test {
    useJUnitPlatform()
}
