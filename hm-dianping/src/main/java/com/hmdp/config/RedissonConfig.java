package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient() {
        //配置Redisson
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.80.159:6379").setPassword("zyj2005!");
        // 创建RedissonClient对象
        return Redisson.create(config);
    }
//    @Bean
//    public RedissonClient redissonClient2() {
//        //配置Redisson
//        Config config = new Config();
//        config.useSingleServer().setAddress("redis://192.168.80.156:6380").setPassword("zyj2005!");
//        // 创建RedissonClient对象
//        return Redisson.create(config);
//    }
//    @Bean
//    public RedissonClient redissonClient3() {
//        //配置Redisson
//        Config config = new Config();
//        config.useSingleServer().setAddress("redis://192.168.80.156:6381").setPassword("zyj2005!");
//        // 创建RedissonClient对象
//        return Redisson.create(config);
//    }

}
