package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult<T> {
    private List<T> list;
    private Long minTime;
    private Integer offset;
}
