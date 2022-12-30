package org.example.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: harden
 * @Date: 2022-12-29 14:38
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    @TableId(type = IdType.AUTO)
    public int id;
    public String name;
    public String password;
    public int age;
}
