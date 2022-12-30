package org.example.entity;

import lombok.Data;

import java.util.Map;

/**
 * @Description 测试发送数据到 kafka topic
 * @Author: harden
 * @Date: 2022-12-29 15:42
 **/
@Data
public class Metric {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
    public Map<String, String> tags;

}
