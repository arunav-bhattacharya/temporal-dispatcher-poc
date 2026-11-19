package com.payments.dispatch.dispatch

import kotlinx.coroutines.delay
import kotlin.math.max

/**
 * Smooth rate limiter: hands out one permit every 1/rate seconds, no bursts.
 * [acquireBlocking] parks the thread (option 1); [acquire] suspends the coroutine (option 2),
 * so a waiting start doesn't hold a thread.
 */
class TokenBucket(permitsPerSecond: Int, private val nanoClock: () -> Long = System::nanoTime) {
    private val intervalNanos = (1_000_000_000.0 / permitsPerSecond.coerceAtLeast(1)).toLong()
    private var nextFree = nanoClock()

    /** Reserves the next permit and returns how long the caller must wait for it, in nanoseconds. */
    @Synchronized
    fun reserve(): Long {
        val now = nanoClock()
        val at = max(now, nextFree)
        nextFree = at + intervalNanos
        return at - now
    }

    fun acquireBlocking() {
        val wait = reserve()
        if (wait > 0) Thread.sleep(wait / 1_000_000, (wait % 1_000_000).toInt())
    }

    suspend fun acquire() {
        val wait = reserve()
        if (wait >= 1_000_000) delay(wait / 1_000_000)
    }
}
