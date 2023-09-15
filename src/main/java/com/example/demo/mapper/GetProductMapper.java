package com.example.demo.mapper;

import com.example.demo.entity.Product;
import org.apache.ibatis.annotations.Param;

public interface GetProductMapper {
  Product getProduct(@Param("id") String id);
}
