package com.hmdp.config;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class RefrechTokenInterceptor implements HandlerInterceptor {
    private StringRedisTemplate stringRedisTemplate;

    public RefrechTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.获取请求头
        String token = String.valueOf(request.getHeader("authorization"));
        if(StrUtil.isBlank(token)){
            return true;
        }
        String key = LOGIN_USER_KEY + token;
        Map<Object, Object> userMap= stringRedisTemplate.opsForHash().entries(key);
        // 3.判断user对象是否为空，如果为空，则重定向到登录页面
        if (userMap.isEmpty()) {
            return true;
        }
        //将hash数据转为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        // 4.如果user对象不为空，存到ThreadLocal中
        UserHolder.saveUser(userDTO);
        stringRedisTemplate.expire(key,LOGIN_USER_TTL, TimeUnit.MINUTES);

        return  true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
