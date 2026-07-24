package com.nervus.sdk.ui

import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import com.nervus.sdk.component.NervusApp
import com.nervus.sdk.ipc.ConnectionState
import kotlin.concurrent.thread
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive

fun NervusApp.attachComposeDesktop(
    title: String = "",
    width: Dp = 800.dp,
    height: Dp = 600.dp,
    onDisconnect: () -> Unit = {},
    content: @Composable () -> Unit
) {
    val app = this

    thread(name = "compose-$title", isDaemon = false) {
        application {
            Window(
                onCloseRequest = {
                    app.close()
                    exitApplication()
                },
                title = title,
                state = rememberWindowState(width = width, height = height)
            ) {
                NervusTheme {
                    content()

                    LaunchedEffect(Unit) {
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
