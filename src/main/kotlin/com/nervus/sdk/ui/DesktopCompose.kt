package com.nervus.sdk.ui

import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.remember
import androidx.compose.ui.input.key.Key
import androidx.compose.ui.input.key.KeyEventType
import androidx.compose.ui.input.key.key
import androidx.compose.ui.input.key.type
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.WindowPlacement
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import com.nervus.sdk.component.Component
import com.nervus.sdk.ipc.ConnectionState
import java.util.logging.Logger
import kotlin.concurrent.thread
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive

private val composeWindowLog = Logger.getLogger("com.nervus.sdk.ui.DesktopCompose")

/**
 * 给一个已启动的组件挂上 Compose Desktop 窗口。
 *
 * 接收者是 [Component] 而不是 [NervusApp]：桌面、设置这类组件既要提供接口
 * （才能被 Resolve 唤醒），又要有界面，用哪个基类取决于它主要做什么，
 * 不该把"能不能有窗口"和"是不是消费者"绑在一起。窗口只用到 close() 与
 * state，两者都在 Component 上。
 */
fun Component.attachComposeDesktop(
    title: String = "",
    width: Dp = 800.dp,
    height: Dp = 600.dp,
    onDisconnect: () -> Unit = {},
    onUnhandledBack: () -> Boolean = { false },
    content: @Composable () -> Unit
) {
    val app = this

    thread(name = "compose-$title", isDaemon = false) {
        application {
            val backDispatcher = remember(onUnhandledBack) {
                BackDispatcher(onUnhandledBack)
            }
            Window(
                onCloseRequest = {
                    app.close()
                    exitApplication()
                },
                title = title,
                // Nervus is a single-foreground-window environment. Requesting
                // maximization here also removes the fixed-size dependency when
                // the panel is not 1280x800; Openbox's right margin remains part
                // of the maximized work area.
                state = rememberWindowState(
                    placement = WindowPlacement.Maximized,
                    width = width,
                    height = height,
                ),
                onPreviewKeyEvent = { event ->
                    event.type == KeyEventType.KeyDown &&
                        event.key == Key.Escape &&
                        backDispatcher.dispatch()
                },
            ) {
                NervusTheme {
                    CompositionLocalProvider(LocalBackDispatcher provides backDispatcher) {
                        content()
                    }

                    LaunchedEffect(Unit) {
                        // On Linux Skiko starts with OpenGL but can fall back to
                        // software. Record the API actually selected so the
                        // component journal tells us which path is in use.
                        delay(1_000)
                        composeWindowLog.info(
                            "compose window '$title' renderer=${window.renderApi}"
                        )
                        while (isActive) {
                            delay(500)
                            if (app.state == ConnectionState.DISCONNECTED) {
                                onDisconnect()
                                break
                            }
                        }
                    }
                }
            }
        }
    }
}
