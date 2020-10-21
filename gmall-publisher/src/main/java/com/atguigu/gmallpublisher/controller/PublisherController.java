package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.server.PublisherServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherServer publisherServer;
    @RequestMapping("realtime-total")
    public String getDayTotalCount(@RequestParam("date") String date){
        Integer dayTotalCount = publisherServer.getCount(date);
        Double orderAmount = publisherServer.getOrderAmount(date);


        HashMap<String, Object> map = new HashMap<>();
        map.put("id","dau");
        map.put("name","新增日活");
        map.put("value",dayTotalCount);

        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("id","new_mid");
        map1.put("name","新增设备");
        map1.put("value",233);

        HashMap<String, Object> orderMap = new HashMap<>();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmount);

        ArrayList<Map<String, Object>> arrayList = new ArrayList<>();
        arrayList.add(map);
        arrayList.add(map1);
        arrayList.add(orderMap);



        return JSON.toJSONString(arrayList);
    }

    @RequestMapping("realtime-hours")
    public String getHoursCount(@RequestParam("id") String id,@RequestParam("date") String date){


        HashMap hashMap = new HashMap();
        Map todayHashMap = new HashMap();
        Map yesterdayMap = new HashMap();

        if ("dau".equals(id)){
            todayHashMap = publisherServer.getHourCount(date);
            //利用LocalDate类进行日期的计算
            yesterdayMap = publisherServer.getHourCount(LocalDate.parse(date).plusDays(-1).toString());

        }else if("order_amount".equals(id)){
            todayHashMap = publisherServer.getOrderHourAmount(date);
            yesterdayMap = publisherServer.getOrderHourAmount(LocalDate.parse(date).plusDays(-1).toString());
        }
        hashMap.put("yesterday",yesterdayMap);
        hashMap.put("today",todayHashMap);
        return JSON.toJSONString(hashMap);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetailJson(
            @RequestParam("date") String date,
            @RequestParam("startpage") int startpage,
            @RequestParam("size") int size,
            @RequestParam("keyword") String keyword
    ){
        String saleDetail = null;
        try {
            saleDetail = publisherServer.getSaleDetail(date, startpage, size, keyword);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return saleDetail;
    }

}
