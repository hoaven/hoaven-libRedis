package com.hoaven.redis;

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
 * Created by hoaven on 2016/8/12.
 */
@Slf4j
public class RedisLockUtils {

    private static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    @Deprecated
    public static Boolean tryLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout){
        return tryLock(jedisTemplate, lockName, lockTimeout, 5);
    }

    @Deprecated
    public static Boolean tryLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout, final int acquireTimeout){
        return tryLock(jedisTemplate, lockName, lockTimeout, acquireTimeout, 1000L);
    }

    /**
     * 获取redis锁，在异常情况下会无法设置生存时间
     *
     * @param jedisTemplate 没啥好说的
     * @param lockName 锁的唯一名称
     * @param lockTimeout 锁过期时间（单位：秒）
     * @param acquireTimeout 请求锁的超时时间 （单位：秒）
     * @param retryDuration 请求锁的重试间隔时间 （单位：毫秒）
     * @return
     */
    @Deprecated
    public static Boolean tryLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout, final int acquireTimeout, final long retryDuration) {
        if (isEmpty(lockName) || lockTimeout <= 0) {
            return false;
        }
        final String lockKey = lockName;
        String identifier = UUID.randomUUID().toString();
        Calendar atoCal = Calendar.getInstance();
        atoCal.add(Calendar.SECOND, acquireTimeout);
        Date atoTime = atoCal.getTime();

        log.info("Try to acquire the lock. lockKey={},acquireTimeout={}s,lockTimeout={}s", lockKey, acquireTimeout, lockTimeout);
        while (true) {
            //开始获取锁
            boolean acquiredLock = (boolean) jedisTemplate.execute(new RedisCallback<Boolean>() {
                @Override
                public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                    return connection.setNX(jedisTemplate.getStringSerializer().serialize(lockKey), jedisTemplate.getStringSerializer().serialize(identifier));
                }
            });

            if (acquiredLock) {//成功获取锁后，设置锁的过期时间，并返回ID
                jedisTemplate.execute(new RedisCallback<Boolean>() {
                    @Override
                    public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                        return connection.expire(jedisTemplate.getStringSerializer().serialize(lockKey), lockTimeout);
                    }
                });
                log.info("acquired lock. lockKey={}", lockKey);
                return true;
            } else { //如果获取失败，则重试获取锁，或者直接返回null
                log.info("Retry to acquire the lock. lockKey={},acquireTimeout={}s,lockTimeout={}s", lockKey, acquireTimeout, lockTimeout);
                if (acquireTimeout < 0)
                    return false;
                else {
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

    public static Boolean tryAtomicLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout){
        return tryAtomicLock(jedisTemplate, lockName, lockTimeout, 5);
    }

    public static Boolean tryAtomicLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout, final int acquireTimeout){
        return tryAtomicLock(jedisTemplate, lockName, lockTimeout, acquireTimeout, 1000L);
    }

    /**
     * 获取redis原子锁
     *
     * @param jedisTemplate 没啥好说的
     * @param lockName 锁的唯一名称
     * @param lockTimeout 锁过期时间（单位：秒）
     * @param acquireTimeout 请求锁的超时时间 （单位：秒）
     * @param retryDuration 请求锁的重试间隔时间 （单位：毫秒）
     * @return
     */
    public static Boolean tryAtomicLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout, final int acquireTimeout, final long retryDuration) {
        if (isEmpty(lockName) || lockTimeout <= 0) {
            return false;
        }
        final String lockKey = lockName;
        String identifier = UUID.randomUUID().toString();
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
                    Object result =  jedis.eval(RLock.SETNX_EXPIRE_SCRIPT, 3, lockKey, identifier, lockTimeout + "");
                    return result != null;
                }
            });

            if (acquiredLock) {//成功获取锁后，设置锁的过期时间，并返回ID
                log.info("acquired lock. lockKey={}", lockKey);
                return true;
            } else { //如果获取失败，则重试获取锁，或者直接返回null
                log.info("Retry to acquire the lock. lockKey={},acquireTimeout={}s,lockTimeout={}s", lockKey, acquireTimeout, lockTimeout);
                if (acquireTimeout < 0) {
                    return false;
                } else {
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

    public static void unLock(final RedisTemplate jedisTemplate, final String lockName) {
        if (isEmpty(lockName)) {
            return;
        }
        final String lockKey = lockName;
        jedisTemplate.execute(new RedisCallback<Void>() {
            @Override
            public Void doInRedis(RedisConnection connection) throws DataAccessException {
                connection.del(jedisTemplate.getStringSerializer().serialize(lockKey));
                return null;
            }
        });
    }

    //检查锁状态，锁住返回true,否则false
    public static Boolean checkWhetherLockExists(final RedisTemplate jedisTemplate, final String lockName) {
        if (isEmpty(lockName)) {
            return false;
        }
        final String lockKey = lockName;
        boolean lockExists = (boolean) jedisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                byte[] bytes = connection.get(jedisTemplate.getStringSerializer().serialize(lockKey));
                if(null != bytes){
                    return true;
                } else {
                    return false;
                }
            }
        });

        return lockExists;
    }
}
