package com.jbyf.es1.bean;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

@Data
@ToString
public class Goods {

    String title ;
    String images;
    Integer price;
    Integer stock;
    Map attr;
    String highLightTitle;
}
