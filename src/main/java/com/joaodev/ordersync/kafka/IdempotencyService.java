package com.joaodev.ordersync.kafka;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class IdempotencyService {

    private static final String KEY_PREFIX = "processed-event";
    private static final Duration TTL = Duration.ofDays(7);

    private final StringRedisTemplate redisTemplate;

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean isNewEvent(String eventKey) {
        Boolean wasSet = redisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + eventKey, "processed", TTL);
        return Boolean.TRUE.equals(wasSet);
    }
}
