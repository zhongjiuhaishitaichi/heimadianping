package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 解决 击穿
 * 使用逻辑过期解决
 * 另一种方法 : 使用互斥锁
 * @param <T>
 */
//封装一个新的类 带有过期时间和具体哪个东西
@Data
public class RedisData<T> {
    private LocalDateTime expireTime;
    private T data;
}
