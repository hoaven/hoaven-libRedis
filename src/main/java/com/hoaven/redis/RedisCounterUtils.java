package com.hoaven.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;

import java.util.concurrent.TimeUnit;

/**
 * Created by hoaven on 2016/8/16.
 */
@Slf4j
public class RedisCounterUtils {

    public static RedisAtomicInteger getCounter(final RedisTemplate jedisTemplate,String counterKey, int expire, TimeUnit timeUnit){
        RedisAtomicInteger counter = new RedisAtomicInteger(counterKey, jedisTemplate.getConnectionFactory());
        counter.expire(expire, timeUnit);
        log.info("### get counter {}", counterKey);
        return counter;
    }

    public static RedisAtomicInteger getCounter(final RedisTemplate jedisTemplate,String counterKey, int expireSeconds){
        return getCounter(jedisTemplate, counterKey, expireSeconds, TimeUnit.SECONDS);
    }

    public static RedisAtomicInteger getCounter(final RedisTemplate jedisTemplate,String counterKey){
        RedisAtomicInteger counter = new RedisAtomicInteger(counterKey, jedisTemplate.getConnectionFactory());
        log.info("### get counter {}", counterKey);
        return counter;
    }

    public static int decrementAndGet(RedisAtomicInteger counter){
        log.info("### decrementAndGet counter {}", counter.getKey());
        return counter.decrementAndGet();
    }

    public static int incrementAndGet(RedisAtomicInteger counter){
        log.info("### incrementAndGet counter {}", counter.getKey());
        return counter.incrementAndGet();
    }

    public static boolean compareAndSet(RedisAtomicInteger counter, int expect, int value){
        return counter.compareAndSet(expect, value);
    }
}
