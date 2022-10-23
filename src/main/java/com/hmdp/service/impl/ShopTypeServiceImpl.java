package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopTypeMapper shopTypeMapper;

    @Override
    public Result queryList() {
        //redis mysql 保存到redis
        String cacheStr = stringRedisTemplate.opsForValue().get("shop:type:");
        if (cacheStr!=null){
            String res = JSON.toJSON(cacheStr).toString();
            List<ShopType> shopTypeList = JSONArray.parseArray(res, ShopType.class);
            return Result.ok(shopTypeList);
        }
        List<ShopType> shopTypes = shopTypeMapper.getList();
        if (shopTypes==null){
            return Result.fail("没有任何分类信息");
        }
        String shopTypesStr = JSONUtil.toJsonStr(shopTypes);
        stringRedisTemplate.opsForValue().set("shop:type:",shopTypesStr,60, TimeUnit.MINUTES);
        return Result.ok(shopTypes);
    }
}
