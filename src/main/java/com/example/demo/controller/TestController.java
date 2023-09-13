 /*
  * Copyright © 2022 Yazaki Kako Corporation. All Rights Reserved
  */

 package com.example.demo.controller;

 import org.springframework.web.bind.annotation.GetMapping;
 import org.springframework.web.bind.annotation.RestController;

 /**
  * @author IBM陳玉体
  * @version 0.0.1
  * @since 2023/9/13 14:50
  */
 @RestController
 public class TestController {
  @GetMapping("/test")
  public String test() {
   return "testjenkins";
  }
 }
