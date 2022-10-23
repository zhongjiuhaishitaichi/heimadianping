package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result createVoucherOrder(Long voucherId);
    Result seckillVoucherWithRedis(Long voucherId);

    Result seckillVoucherWithRedissonAndLua(Long voucherId);

    Result seckillVoucherWithRedisson(Long voucherId);

    void createVoucherOrder2(VoucherOrder voucherOrder);

    Result beginSeckillVoucher(Long voucherId);
}
