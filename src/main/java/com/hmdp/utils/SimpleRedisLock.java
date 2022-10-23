package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String lockName;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    //提前加载
    static{
        UNLOCK_SCRIPT=new DefaultRedisScript<>();
        //ClassPathResource --> resources目录下
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String lockName, StringRedisTemplate stringRedisTemplate) {
        this.lockName = lockName;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //给当前线程加锁
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + lockName, threadId, timeoutSec, TimeUnit.SECONDS);
        //自动拆箱 避免空指针
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        String threadId = ID_PREFIX + Thread.currentThread().getId(); //线程
        String curLockName = stringRedisTemplate.opsForValue().get(KEY_PREFIX + lockName);
        if (curLockName.equals(threadId)) {   //超时释放锁也一样会有并发问题
            stringRedisTemplate.delete(KEY_PREFIX + lockName);
        }
    }

    //使用lua脚本保证原子性
    @Override
    public void unlockWithLua() {
        stringRedisTemplate
                .execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX + lockName)
                        ,ID_PREFIX+Thread.currentThread().getId());
    }
}
