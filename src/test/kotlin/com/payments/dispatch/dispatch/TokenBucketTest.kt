package com.payments.dispatch.dispatch

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TokenBucketTest {

    @Test
    fun `spaces permits evenly at the configured rate`() {
        var now = 0L
        val bucket = TokenBucket(permitsPerSecond = 100) { now } // one permit every 10 ms
        assertEquals(0L, bucket.reserve())
        assertEquals(10_000_000L, bucket.reserve())
        assertEquals(20_000_000L, bucket.reserve())
        now = 100_000_000L // idle time does not bank a burst
        assertEquals(0L, bucket.reserve())
        assertEquals(10_000_000L, bucket.reserve())
    }
}
