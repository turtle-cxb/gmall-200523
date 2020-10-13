package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.JsonObjectDeserializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
//@RestController=@Controller+@ResponseBody
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @RequestMapping("test1")
//    @ResponseBody 不返回页面只返回字符串
    public String test1(){
        System.out.println("*************");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,@RequestParam("age") int age){
        System.out.println(nn+":"+age);
        return "success";
    }
    @RequestMapping("log")
    public String getLog(@RequestParam("logString") String logStr){

        //为数据加上时间戳
        JSONObject jsonObject = JSONObject.parseObject(logStr);
        jsonObject.put("ts",System.currentTimeMillis());
        String addTsLogStr = jsonObject.toString();


        //将数据写入日志
        log.info(addTsLogStr);
        //将数据写入kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_START,addTsLogStr);

        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,addTsLogStr);
        }



        //返回
        return "success";

    }
}
