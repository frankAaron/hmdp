package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.config.RedissonConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import com.hmdp.service.ISeckillVoucherService;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.hmdp.dto.Result.fail;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        checkAndCreateStream();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private void checkAndCreateStream() {
        try {
            // 检查Stream信息
            StreamInfo.XInfoStream info = stringRedisTemplate.opsForStream().info("stream.orders");
            log.info("Stream信息: {}", info);
        } catch (Exception e) {
            log.warn("Stream不存在或无法访问，将自动创建");
        }
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    if (list == null || list.isEmpty()){
                        continue;
                    }
                    // 获取订单信息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                    /*
                    创建订单 ack  SACK stream.orders g1 id
                     */
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1",entries.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    if (list == null || list.isEmpty()){
                        // pending-list没有消息
                        break;
                    }
                    // 获取订单信息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                    //创建订单 ack  SACK stream.orders g1 id

                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1",entries.getId());
                } catch (Exception e) {
                    log.error("pending-list 处理订单异常", e);

                }
            }
        }
    }


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVourcherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    /**
     * 秒杀优惠券
     *
     * @param voucherId 优惠券ID
     * @return 秒杀结果，成功返回订单ID，失败返回错误信息
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisIdWorker.nextId("order");

        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                UserHolder.getUser().getId().toString(),
                String.valueOf(orderId)
        );
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }



    /**
     * 创建优惠券订单
     */
    @Transactional
    public void createVourcherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();

        Long count = query()
                .eq("userId", userId)
                .eq("voucherId", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }
        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }


    /**
     * 秒杀订单阻塞队列
     */
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//
//    }

    /**
     * 秒杀优惠券阻塞队列
     *
     * @param voucherId 优惠券ID
     * @return 秒杀结果，成功返回订单ID，失败返回错误信息
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                UserHolder.getUser().getId().toString()
//        );
//        int r = result.intValue();
//        if (r != 0) {
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //获取代理对象
//         proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        return Result.ok(orderId);
//    }


/**
 * 秒杀优惠卷，分布式锁解决
 */
//     @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        //判断是否开始
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("lock:order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if (!isLock) {
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            //获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVourcherOrder(voucherId, voucher);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//    }
/**
 * 创建优惠券订单，分布式锁解决
 */
//    @Transactional
//    public Result createVourcherOrder(Long voucherId, SeckillVoucher voucher) {
//        //一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        Long count = query().eq("userId", userId).eq("voucherId", voucherId).count();
//        if (count > 0) {
//            return Result.fail("不能重复下单");
//        }
//        //扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock=stock-1")
//                .eq("voucher_id", voucherId)
//                .gt("stock", voucher.getStock())
//                .update();
//        if (!success) {
//            return Result.fail("库存不足");
//        }
//
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        //生成订单
//        save(voucherOrder);
//
//        return Result.ok(orderId);
//
//    }

}
