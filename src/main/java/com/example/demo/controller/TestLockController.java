 /*
  * Copyright © 2022 Yazaki Kako Corporation. All Rights Reserved
  */

 package com.example.demo.controller;

 import com.example.demo.entity.Product;
 import com.example.demo.mapper.GetProductMapper;
 import com.example.demo.utils.RedisUtils;
 import org.redisson.api.RLock;
 import org.redisson.api.RedissonClient;
 import org.springframework.beans.factory.annotation.Autowired;
 import org.springframework.web.bind.annotation.GetMapping;
 import org.springframework.web.bind.annotation.RequestParam;
 import org.springframework.web.bind.annotation.RestController;

 /**
  * 分布式锁.
  *
  * @author IBM陳玉体
  * @version 0.0.1
  * @since 2023/9/15 10:00
  */
 @RestController
 public class TestLockController {
     @Autowired
     private RedisUtils redisUtils;

     @Autowired
     private GetProductMapper getProductMapper;
     @Autowired
     private RedissonClient redissonClient;

     @GetMapping("/testRedisLock")
     public String testRedisLock(@RequestParam String id) {
         //应该缓存空数据，或者布隆过滤器防止缓存穿透
         String key = "test";
         long timeOut = 5;
         RLock lock = redissonClient.getLock("rLock");
         lock.lock();
         if (redisUtils.hasKey(key)) {
             System.out.println("read redis");
             lock.unlock();
             return redisUtils.getString(key);
         }
         Product product = getProductMapper.getProduct(id);
         if (product != null) {
             System.out.println("read database");
             redisUtils.putString(key, product.toString(), timeOut);
             lock.unlock();
             return product.toString();
         }
         lock.unlock();
         return "success";
     }
 }
