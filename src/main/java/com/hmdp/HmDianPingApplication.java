package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy(exposeProxy = true)   // 引入依赖 暴露代理对象 才能拿到代理对象
@Slf4j
@MapperScan("com.hmdp.mapper")
@SpringBootApplication(exclude= {DataSourceAutoConfiguration.class})
public class HmDianPingApplication {
    static {
        System.setProperty("druid.mysql.usePingMethod","false");
    }

    public static void main(String[] args) {
        SpringApplication.run(HmDianPingApplication.class, args);
        log.info("项目启动成功.");
    }

}
