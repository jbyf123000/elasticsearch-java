package com.jbyf.es1.controller;


import com.google.gson.Gson;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
public class EXController {
    @Autowired
    RestHighLevelClient client;

    @GetMapping("/helloes")
    public Object helloes(){
        try {
            //info:获取es节点的信息
            return client.info(RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "fail";
    }

//    @GetMapping("createDoc")
//    public Object createDoc() throws IOException{
//        IndexRequest indexRequest = new IndexRequest("atguigu3","house");
//        Map<String ,Object> map =new HashMap<>();
//        map.put("id",1001);
//        map.put("address","百合公寓a011");
//        map.put("user",new User(1,"峰哥"));
//        indexRequest.source(new Gson().toJson(map), XContentType.JSON);
//        IndexResponse response = client.index(indexRequest,RequestOptions.DEFAULT);
//        return response;
//    }




}
