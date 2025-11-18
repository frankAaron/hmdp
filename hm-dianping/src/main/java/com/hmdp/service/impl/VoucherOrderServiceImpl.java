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
import javax.annotation.PreDestroy;
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

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 常量定义
    private static final String STREAM_KEY = "stream.orders";
    private static final String CONSUMER_GROUP = "g1";
    private static final String CONSUMER_NAME = "c1";

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 使用单线程线程池处理订单
    private final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private volatile boolean running = true; // 控制线程运行状态
    private IVoucherOrderService proxy;

    /**
     * 初始化方法 - 创建Stream和Consumer Group，并启动订单处理线程
     */
    @PostConstruct
    private void init() {
        log.info("开始初始化秒杀订单服务...");
        try {
            initStreamAndGroup();
            SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
            log.info("秒杀订单服务初始化完成");
        } catch (Exception e) {
            log.error("秒杀订单服务初始化失败", e);
        }
    }

    /**
     * 优雅关闭方法
     */
    @PreDestroy
    public void destroy() {
        log.info("开始关闭秒杀订单服务...");

        // 停止线程运行
        running = false;

        // 关闭线程池
        SECKILL_ORDER_EXECUTOR.shutdown();
        try {
            // 等待线程池终止，最多等待30秒
            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("订单处理线程未正常终止，强制关闭");
                SECKILL_ORDER_EXECUTOR.shutdownNow();
                // 再次等待终止
                if (!SECKILL_ORDER_EXECUTOR.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("订单处理线程池未能终止");
                }
            }
        } catch (InterruptedException e) {
            log.error("关闭订单处理服务时被中断", e);
            Thread.currentThread().interrupt();
            SECKILL_ORDER_EXECUTOR.shutdownNow();
        }

        log.info("秒杀订单服务已关闭");
    }

    /**
     * 初始化Stream和Consumer Group
     */
    private void initStreamAndGroup() {
        try {
            // 检查Stream是否存在
            Boolean exists = stringRedisTemplate.hasKey(STREAM_KEY);

            if (Boolean.TRUE.equals(exists)) {
                // Stream存在，检查Consumer Group是否存在
                try {
                    StreamInfo.XInfoGroups groups = stringRedisTemplate.opsForStream().groups(STREAM_KEY);
                    boolean groupExists = groups.stream()
                            .anyMatch(group -> CONSUMER_GROUP.equals(group.groupName()));

                    if (!groupExists) {
                        // Stream存在但Consumer Group不存在，创建Consumer Group
                        stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), CONSUMER_GROUP);
                        log.info("创建Consumer Group: {} for Stream: {}", CONSUMER_GROUP, STREAM_KEY);
                    } else {
                        log.info("Stream和Consumer Group已存在");
                    }
                } catch (Exception e) {
                    // 如果获取groups失败，说明Consumer Group不存在，创建它
                    log.warn("Consumer Group不存在，正在创建...");
                    stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), CONSUMER_GROUP);
                    log.info("创建Consumer Group: {} for Stream: {}", CONSUMER_GROUP, STREAM_KEY);
                }
            } else {
                // Stream不存在，创建Stream和Consumer Group
                Map<String, String> initialData = Collections.singletonMap("init", "true");
                RecordId recordId = stringRedisTemplate.opsForStream().add(STREAM_KEY, initialData);
                stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), CONSUMER_GROUP);
                // 删除初始化消息
                stringRedisTemplate.opsForStream().delete(STREAM_KEY, recordId);
                log.info("创建Stream: {} 和 Consumer Group: {}", STREAM_KEY, CONSUMER_GROUP);
            }
        } catch (Exception e) {
            log.error("初始化Stream和Consumer Group失败", e);
            // 重试初始化
            retryInitStream();
        }
    }

    /**
     * 重试初始化Stream
     */
    private void retryInitStream() {
        int retryCount = 0;
        int maxRetries = 3;

        while (retryCount < maxRetries) {
            try {
                Thread.sleep(2000); // 等待2秒后重试
                initStreamAndGroup();
                log.info("Stream初始化重试成功");
                return;
            } catch (Exception e) {
                retryCount++;
                log.error("Stream初始化重试失败 ({}/{})", retryCount, maxRetries, e);
            }
        }
        log.error("Stream初始化彻底失败，请检查Redis连接");
    }

    /**
     * 订单处理线程
     */
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            log.info("订单处理线程启动");

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 从Stream读取订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(CONSUMER_GROUP, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );

                    if (list == null || list.isEmpty()) {
                        continue;
                    }

                    // 处理订单
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();

                    // 跳过初始化消息
                    if ("true".equals(value.get("init"))) {
                        stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, entries.getId());
                        continue;
                    }

                    VoucherOrder voucherOrder = createVoucherOrderFromMap(value);
                    if (voucherOrder != null) {
                        handleVoucherOrder(voucherOrder);
                        stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, entries.getId());
                        log.info("订单处理成功: {}", voucherOrder.getId());
                    }

                } catch (Exception e) {
                    if (running && !Thread.currentThread().isInterrupted()) {
                        log.error("处理订单异常", e);
                        handlePendingList();
                    } else {
                        log.info("订单处理线程被中断");
                        break;
                    }
                }
            }

            log.info("订单处理线程已停止");
        }

        /**
         * 处理pending-list中的消息
         */
        private void handlePendingList() {
            if (!running || Thread.currentThread().isInterrupted()) {
                return;
            }

            log.info("开始处理pending-list");
            int processedCount = 0;

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 从pending-list读取消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(CONSUMER_GROUP, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );

                    if (list == null || list.isEmpty()) {
                        log.info("pending-list处理完成，共处理{}条消息", processedCount);
                        break;
                    }

                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();

                    // 跳过初始化消息
                    if ("true".equals(value.get("init"))) {
                        stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, entries.getId());
                        continue;
                    }

                    VoucherOrder voucherOrder = createVoucherOrderFromMap(value);
                    if (voucherOrder != null) {
                        handleVoucherOrder(voucherOrder);
                        stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, entries.getId());
                        processedCount++;
                        log.info("pending-list订单处理成功: {}", voucherOrder.getId());
                    }

                } catch (Exception e) {
                    if (running && !Thread.currentThread().isInterrupted()) {
                        log.error("pending-list处理订单异常", e);
                        try {
                            Thread.sleep(2000); // 等待2秒后继续
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    /**
     * 从Map创建VoucherOrder对象
     */
    private VoucherOrder createVoucherOrderFromMap(Map<Object, Object> value) {
        try {
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
            log.debug("成功创建订单对象: {}", voucherOrder);
            return voucherOrder;
        } catch (Exception e) {
            log.error("从Map创建VoucherOrder失败: {}", value, e);
            return null;
        }
    }

    /**
     * 处理订单业务逻辑
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        if (voucherOrder == null || voucherOrder.getUserId() == null || voucherOrder.getVoucherId() == null) {
            log.error("订单数据不完整: {}", voucherOrder);
            return;
        }

        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁，设置超时时间避免死锁
        boolean isLock = false;
        try {
            isLock = lock.tryLock(2, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("获取锁时被中断", e);
            return;
        }

        if (!isLock) {
            log.error("不允许重复下单, userId: {}", userId);
            return;
        }

        try {
            if (proxy != null) {
                proxy.createVourcherOrder(voucherOrder);
            } else {
                log.warn("代理对象未初始化，直接处理订单");
                createVourcherOrder(voucherOrder);
            }
        } catch (Exception e) {
            log.error("创建订单失败: {}", voucherOrder, e);
        } finally {
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 秒杀优惠券
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户ID和生成订单ID
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 执行Lua脚本进行库存校验和订单创建
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 获取代理对象用于事务管理
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }

    /**
     * 创建优惠券订单（事务方法）
     */
    @Transactional
    public void createVourcherOrder(VoucherOrder voucherOrder) {
        // 参数校验
        if (voucherOrder == null || voucherOrder.getUserId() == null || voucherOrder.getVoucherId() == null) {
            log.error("订单数据不完整");
            return;
        }

        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 一人一单校验
        Long count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();

        if (count > 0) {
            log.error("用户已经购买过一次, userId: {}, voucherId: {}", userId, voucherId);
            return;
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            log.error("库存不足, voucherId: {}", voucherId);
            return;
        }

        // 保存订单
        save(voucherOrder);
        log.info("订单创建成功: {}", voucherOrder.getId());
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
