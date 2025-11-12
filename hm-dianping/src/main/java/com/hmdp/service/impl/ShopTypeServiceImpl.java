package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

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

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_KEY;
        // 先删除可能存在的错误类型数据
        try {
            // 尝试用String方式读取
            String shopTypeJson = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isNotBlank(shopTypeJson)) {
                List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
                return Result.ok(shopTypes);
            }
        } catch (Exception e) {
            // 如果类型不匹配，删除错误数据
            log.warn("Redis键类型不匹配，清理数据: " + key);
            stringRedisTemplate.delete(key);
        }

        // 从redis中查询缓存
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);

        // 缓存中有,返回数据
        if (StrUtil.isNotBlank(shopTypeJson)) {
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypes);
        }

        // 缓存中没有,查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if (CollectionUtil.isEmpty(shopTypes)) {
            return Result.fail("商铺类型不存在");
        }

        // 将整个List转为JSON字符串存储
        String jsonStr = JSONUtil.toJsonStr(shopTypes);
        stringRedisTemplate.opsForValue().set(key, jsonStr);
        // 可以设置过期时间
        stringRedisTemplate.expire(key, CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

        return Result.ok(shopTypes);
    }
}
