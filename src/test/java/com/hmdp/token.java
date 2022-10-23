package com.hmdp;

import cn.hutool.core.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Locale;
import java.util.Random;

public class token {
    @Test
    public void generateSessionKey() {

        for (int i = 0; i < 1000; i++) {
            String s = RandomStringUtils.randomAlphanumeric(32).toLowerCase(Locale.ROOT);
            System.out.println(s);
        }
    }
}