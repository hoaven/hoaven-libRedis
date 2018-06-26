package com.hoaven.redis;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

/**
 * Created by hoaven on 2016/9/19.
 */
@Slf4j
public class RLock {

    public static final String SETNX_EXPIRE_SCRIPT = "if redis.call('setnx', KEYS[1], KEYS[2]) == 1 then\n"
            + "return redis.call('expire', KEYS[1], KEYS[3]);\n" + "end\n" + "return nil;";

    @Getter
    private boolean isLocked = false;

    private RedisTemplate jedisTemplate;
    @Getter
    private String lockName;
    private int lockTimeout;
    private int acquireTimeout;
    private long retryDuration;

    @Getter
    private String id;

    public RLock(RedisTemplate jedisTemplate, String lockName, int lockTimeout, int acquireTimeout, long retryDuration) {
        this.jedisTemplate = jedisTemplate;
        this.lockName = lockName;
        this.lockTimeout = lockTimeout;
        this.acquireTimeout = acquireTimeout;
        this.retryDuration = retryDuration;
    }

    public RLock(RedisTemplate jedisTemplate, String lockName, int lockTimeout, int acquireTimeout) {
        this(jedisTemplate, lockName, lockTimeout, acquireTimeout, 1000L);
    }

    public RLock(RedisTemplate jedisTemplate, String lockName, int lockTimeout) {
        this(jedisTemplate, lockName, lockTimeout, 5);
    }

    private static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public Boolean tryLock() {
        if (isEmpty(lockName) || lockTimeout <= 0) {
            return false;
        }
        final String lockKey = lockName;
        String identifier = UUID.randomUUID().toString();
        this.id = identifier;
        Calendar atoCal = Calendar.getInstance();
        atoCal.add(Calendar.SECOND, acquireTimeout);
        Date atoTime = atoCal.getTime();

        log.info("Try to acquire the lock. lockKey={},acquireTimeout={}s,lockTimeout={}s", lockKey, acquireTimeout, lockTimeout);
        while (true) {
            //开始获取锁,并设置生存期
            Boolean acquiredLock =(Boolean) jedisTemplate.execute(new RedisCallback<Boolean>() {
                @Override
                public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                    Jedis jedis = (Jedis) connection.getNativeConnection();
                    Object result =  jedis.eval(SETNX_EXPIRE_SCRIPT, 3, lockKey, identifier, lockTimeout + "");
                    return result != null;
                }
            });

            //成功获取锁后，并返回成功标志
            if (acquiredLock) {
                log.info("acquired lock. lockKey={}", lockKey);
                this.isLocked = true;
                return true;
            } else { //如果获取失败，则重试获取锁，或者直接返回null
                log.info("Retry to acquire the lock. lockKey={},acquireTimeout={}s,lockTimeout={}s", lockKey, acquireTimeout, lockTimeout);
                if (acquireTimeout < 0) {
                    return false;
                }else {
                    try {
                        log.info("wait 1000 milliseconds before retry. lockKey={}", lockKey);
                        Thread.sleep(retryDuration);
                    } catch (InterruptedException ex) {
                    }
                }
                if (new Date().after(atoTime)) {
                    break;
                }
            }
        }
        return false;
    }

    public void unLock() {
        if (isEmpty(this.lockName) || !this.isLocked) {
            return;
        }
        final String lockKey = this.lockName;
        jedisTemplate.execute(new RedisCallback<Void>() {
            @Override
            public Void doInRedis(RedisConnection connection) throws DataAccessException {
                connection.del(jedisTemplate.getStringSerializer().serialize(lockKey));
                return null;
            }
        });
    }


}
