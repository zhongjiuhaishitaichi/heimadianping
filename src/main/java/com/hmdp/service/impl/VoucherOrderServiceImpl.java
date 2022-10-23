package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.config.RunException;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */

/**
 * 悲观锁-->查
 * 乐观锁-->改
 *   //悲观锁  当一开始就用有并发的时候用悲观锁  当是有值修改的时候用乐观锁
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    //redis的阻塞队列
   // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //单线程的线程池
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //成员变量 整个线程都能拿到
    public IVoucherOrderService proxy;


    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    //注解 当类加载的时候执行这个方法
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());   //类一初始化 就开始执行run方法
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName="stream.orders";
        @Override
        public void run() {
            while (true) {
                //获取消息队列!!!中的订单信息
                try {   //XGROUP CREATE stream.orders g1 0 MKSTREAM

                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2))
                            , StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断list是否为空
                    if (list==null||list.isEmpty()){
                        continue;
                    }
                    //解析订单信息
                    MapRecord<String, Object, Object> mapRecord = list.get(0);  //三个成员变量 ->两个object-->map
                    Map<Object, Object> values = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(),true);
                    //下单
                    createVoucherOrder2(voucherOrder);
                    // ACK确定      从队列里移除
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",mapRecord.getId());

                } catch (Exception e) {   //有异常 就没有被ack确认
                    /**
                     * 在消费者组模式下，当一个消息被消费者取出，为了解决组内消息读取但处理期间消费者崩溃带来的消息丢失问题
                     * ，STREAM 设计了 Pending 列表，用于记录读(XREADGROUP)取但并未处理完毕(未ACK)的消息。
                     */
                    log.error("订单异常!", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //获取 pendingList!!!里的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1)
                            , StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断Pendinglist是否为空
                    if (list==null||list.isEmpty()){
                        //没有了
                        break;
                    }
                    //解析订单信息
                    MapRecord<String, Object, Object> mapRecord = list.get(0);  //三个成员变量 ->两个object-->map
                    Map<Object, Object> values = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(),true);
                    //下单
                    createVoucherOrder2(voucherOrder);
                    // ACK确定      从队列里移除
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",mapRecord.getId());

                } catch (Exception e) {   //有异常 接着在pendingList里处理
                    log.error("pendingList里订单异常!", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

    /*    // 阻塞队列实现     子线程
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            RLock lock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());
            //获得锁阻塞会重试 参数代表重试时间
            boolean isLock = lock.tryLock();
            if (!isLock) {
                throw new RunException("无法获取,不能重复下单!");
            }
            try {
                proxy.createVoucherOrder2(voucherOrder);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            } finally {
                //释放
                lock.unlock();
            }
        }*/
    }
    @Override
    public Result beginSeckillVoucher(Long voucherId){
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //未开始
            throw new RunException("秒杀未开始!");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            throw new RunException("秒杀已经结束!");
        }
        if (seckillVoucher.getStock() < 1) {
            throw new RunException("库存不足!");
        }
            return  seckillVoucherWithRedissonAndLua(voucherId);
    }

    @Override
    public Result seckillVoucherWithRedissonAndLua(Long voucherId) {
        //执行lua脚本
        Long userId = UserHolder.getUser().getId();
        long orderId = redisWorker.nextId("order");
        Long result = stringRedisTemplate.execute
                (SECKILL_SCRIPT, Collections.emptyList(),
                        voucherId.toString(), userId.toString(), String.valueOf(orderId));
        // 1 2 失败 0 成功
        int resultInt = result.intValue();
        if (resultInt != 0) {
            throw new RunException(resultInt == 1 ? "库存不足!" : "不能重复下单!");
        }

        //子线程无法获得代理对象 只能在父线程获取
     //  proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    @Transactional
    @Override
    public void createVoucherOrder2(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }






    //阻塞队列
  /*  private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                //获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("订单异常!", e);
                }
            }
        }*/


    /**
     * 基于redisson 使用redis缓存优化
     *
     * @param
     * @return
     */
   /* @Override
    public Result seckillVoucherWithRedissonAndLua(Long voucherId) {
        //执行lua脚本
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute
                (SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
        // 1 2 失败 0 成功
        int resultInt = result.intValue();
        if (resultInt != 0) {
            throw new RunException(resultInt == 1 ? "库存不足!" : "不能重复下单!");
        }
        //生成订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        Long orderId = redisWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        //子线程无法获得代理对象 只能在父线程获取
        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
        //保存到阻塞队列
        orderTasks.add(voucherOrder);
        return Result.ok(orderId);
    }*/



    /**
     * 使用redisson提供的锁
     *
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucherWithRedisson(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //未开始
            throw new RunException("秒杀未开始!");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            throw new RunException("秒杀已经结束!");
        }
        if (seckillVoucher.getStock() < 1) {
            throw new RunException("库存不足!");
        }

        Long userId = UserHolder.getUser().getId();
        //redisson框架
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获得锁阻塞会重试 参数代表重试时间
        boolean isLock = lock.tryLock();
        if (!isLock) {
            throw new RunException("无法获取,不能重复下单!");
        }
        try {
            IVoucherOrderService currentProxy = (IVoucherOrderService) AopContext.currentProxy();
            return currentProxy.createVoucherOrder(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放
            lock.unlock();
        }

    }
    @Transactional  //spring管理事务是代理模式 代理的这个类 当调用这个方法的时候需要先获得代理对象 才能被spring管理
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //一人一单
        Long count = this.query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            throw new RunException("您已经下过一单了!");
        }
        //gt  大于
        //乐观锁  -->改
        boolean success = seckillVoucherService.update().
                setSql("stock=stock-1").eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            //失败
            throw new RunException("库存不足!");
        }
        //生成订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        Long orderId = redisWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        this.save(voucherOrder);
        return Result.ok(voucherId);
    }



    /**
     * 集群的情况下 用redis分布式锁 解决并发问题
     *
     * @param voucherId
     * @return
     */
    public Result seckillVoucherWithRedis(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //未开始
            throw new RunException("秒杀未开始!");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            throw new RunException("秒杀已经结束!");
        }
        if (seckillVoucher.getStock() < 1) {
            throw new RunException("库存不足!");
        }

        Long userId = UserHolder.getUser().getId();
        //使用redis分布式锁
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock(1200);
        if (!isLock) {
            throw new RunException("无法获取,不能重复下单!");
        }
        try {
            IVoucherOrderService currentProxy = (IVoucherOrderService) AopContext.currentProxy();
            return currentProxy.createVoucherOrder(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放
            lock.unlockWithLua();
        }

    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //未开始
            throw new RunException("秒杀未开始!");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            throw new RunException("秒杀已经结束!");
        }
        if (seckillVoucher.getStock() < 1) {
            throw new RunException("库存不足!");
        }

        //给用户加锁
        //每次toString都是新的对象  intern可以让常量池返回同一个对象
        Long userId = UserHolder.getUser().getId();
        /**
         * 单机的话 直接synchronized 悲观锁 保证同一用户只有一个线程
         */
        synchronized (userId.toString().intern()) {  //先拿锁再执行方法 可以让事务提交了再释放锁 避免并发问题
            //获取代理对象
            IVoucherOrderService currentProxy = (IVoucherOrderService) AopContext.currentProxy();
            return currentProxy.createVoucherOrder(voucherId);
        }
    }


}
