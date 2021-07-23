package com.jbyf.es1.bean;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class House {

    String id;
    String address;
    User user;
}
