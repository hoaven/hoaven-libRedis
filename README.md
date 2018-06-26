### 1、简介
>使用`Redis`实现分布式锁
### 2、使用
>直接调用`RedisLockUtils.tryLock(final RedisTemplate jedisTemplate, final String lockName, final int lockTimeout, final int acquireTimeout, final long retryDuration)`;

**参数介绍：**
- jedisTemplate：没啥好说的
- lockName：锁的唯一名称
- lockTimeout：锁过期时间（单位：秒）
- acquireTimeout：请求锁的超时时间 （单位：秒）
- retryDuration：请求锁的重试间隔时间 （单位：毫秒）
### 3、设计思路
#### 3 . 1、基本思路
>主要用到的是redis函数`setNX()`，这个应该是实现分布式锁最主要的函数。首先是将某一任务标识名(lockKey)作为键存到redis里，并为其设个过期时间，如果是还有lockKey请求过来，先是通过`setNX()`看看是否能将lockKey插入到redis里，可以的话就返回true，不可以就返回false。

#### 3 . 2、锁过期时间
>为避免特殊原因导致获得的锁无法释放，在加锁成功后，通过redis函数`expire()`给锁赋予一个生存时间，超出生存时间锁会被自动释放。