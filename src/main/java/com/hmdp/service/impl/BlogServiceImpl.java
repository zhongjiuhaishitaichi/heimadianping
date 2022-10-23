package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.config.RunException;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            isBlogLiked(blog);
            getUserBlog(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = this.getById(id);
        if (blog == null) {
            throw new RunException("该博客已不存在!");
        }
        //查询用户信息
        getUserBlog(blog);
        //判断是否点赞了
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO userDTO = UserHolder.getUser();
        if (userDTO == null) {
            return;
        }
        Long userId = userDTO.getId();
        String blogKey = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        String blogKey = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        if (score == null) {
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            if (isSuccess) {      //zadd key value score 根据时间排序
                stringRedisTemplate.opsForZSet().add(blogKey, userId.toString(), System.currentTimeMillis());
            }
        } else {
            boolean isSuccess = update().setSql("liked=liked-1").eq("id", id).update();
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(blogKey, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryLikes(Long id) {
        String blogKey = BLOG_LIKED_KEY + id;
        Set<String> likeSet = stringRedisTemplate.opsForZSet().range(blogKey, 0, 4);
        if (likeSet == null || likeSet.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = likeSet.stream().map(Long::valueOf).collect(Collectors.toList());
        //转化为字符串 加,
        String idsStr = StrUtil.join(",", ids);
        // where id IN (5,1) order by Filed(id,5,1) 排序              last 最后一段sql->拼接
        List<UserDTO> userDTOList = userService.query().in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        blog.setUserId(userId);
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败!");
        }
        //查询所有粉丝
        Set<String> keys = stringRedisTemplate.keys("follows:*");
        if (keys == null || keys.isEmpty()) {
            return Result.ok(blog.getId());
        }
        for (String key : keys) {
            Boolean isMyFollow = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
            if (BooleanUtil.isTrue(isMyFollow)) {
                String[] strings = key.split(":");
                long keyInt = Long.parseLong(strings[1]);
                String feedKey = FEED_KEY + keyInt;
                stringRedisTemplate.opsForZSet().add(feedKey, blog.getId().toString(), System.currentTimeMillis());
            }
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
        // 滚动查询收件箱  ZREVRANGEBYSCORE key max min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());
        Long minTime = 0L;
        int osCount = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //获取id
            String idStr = tuple.getValue();  //博客的id
            ids.add(Long.valueOf(idStr));
            //每次遍历都会重新赋值 得到最小的时间
            long time = tuple.getScore().longValue();   //分数时间戳
            if (time == minTime) {
                osCount++;
            } else {
                minTime = time;
                osCount = 1;
            }
        }
        //ids是由大到小 数据库查询的时候要改变顺序
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogList = query()
                .in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")").list();
        blogList.forEach(blog -> {
            //查询用户信息
            getUserBlog(blog);
            //判断是否点赞了
            isBlogLiked(blog);
        });
        //封装返回
        ScrollResult<Blog> r = new ScrollResult<>();
        r.setList(blogList);
        r.setOffset(osCount);
        r.setMinTime(minTime);

        return Result.ok(r);
    }

    private void getUserBlog(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setIcon(user.getIcon());
        blog.setName(user.getNickName());
    }
}
