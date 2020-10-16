package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.server.PublisherServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

        HashMap<String, Object> map = new HashMap<>();
        map.put("id","dau");
        map.put("name","新增日活");
        map.put("value",dayTotalCount);

        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("id","new_mid");
        map1.put("name","新增设备");
        map1.put("value",233);

        ArrayList<Map<String, Object>> arrayList = new ArrayList<>();
        arrayList.add(map);
        arrayList.add(map1);

        return JSON.toJSONString(arrayList);
    }

    @RequestMapping("realtime-hours")
    public String getHoursCount(@RequestParam("id") String id,@RequestParam("date") String date){
        Map todayMap = publisherServer.getHourCount(date);
        //利用LocalDate类进行日期的计算
        Map yesterdayMap = publisherServer.getHourCount(LocalDate.parse(date).plusDays(-1).toString());
        HashMap hashMap = new HashMap();
        hashMap.put("yesterday",yesterdayMap);
        hashMap.put("today",todayMap);

        return JSON.toJSONString(hashMap);


    }

}
