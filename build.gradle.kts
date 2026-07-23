plugins {
    alias(libs.plugins.kotlin.jvm)
}

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
    description = "Copy nervus-ipc repository for protocol types"
    from(ipcRepoPath)
    into(cloneDir)
    onlyIf { !cloneDir.isDirectory }
    doLast {
        logger.lifecycle("nervus-ipc protocol types synced to $cloneDir")
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

tasks.named("processResources") {
    dependsOn(cloneProtocol)
}

tasks.named("clean") {
    dependsOn(cleanProtocol)
}

dependencies {
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.kotlin)
    implementation(libs.kotlinx.coroutines.core)

    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
