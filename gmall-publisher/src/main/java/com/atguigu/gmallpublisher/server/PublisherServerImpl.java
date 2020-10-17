package com.atguigu.gmallpublisher.server;

import com.atguigu.gmallpublisher.mapper.OrderInfoMapper;
import com.atguigu.gmallpublisher.mapper.PublisherMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServerImpl implements PublisherServer {
    //
    /*  所有的包都必须在com.atguigu.gmallpublisher包下,不然无法运行web主程序，使用Mybatis
    框架时，在gmallpublisher包下新建mapper包，并在resources下创建同名的文件夹mapper，在
    mapper包下创建PublisherMapper接口写其中的抽象方法，其抽象方法的方法名，返回值要与mapper
    文件夹下的.xml文件件中的id和resultType一致，并且.xml文件中的属性
        <mapper namespace="com.atguigu.gmallpublisher.mapper.PublisherMapper">
        要为mapper包下接口的全类名
    Mybatis框架会自动关联mapper包和mapper文件夹，mapper文件夹中.xml文件中会有对应的sql语句，
    实质为mapper包下的接口实现类所实现的方法，通过springboot的注释自动注入PublisherMapper接口
    的实现类，然后通实现类来调用方法从Phoenix(Hbase)中选取数据，注意:返回值的确定，若是选择的
    结果有多行多列则返回Map类型的数组（这个根据sql查询的结果来确定数据类型）
    List(map(列名字段名,数据),map().....)，List中的每个map都封装一行的数据其key值为字段名，value
    值为数据，这样把从表中选出的每一行数据就都封装成一个map,而选择的数据有多行的话就有多个map,最后
    返回值由List封装。

    然后通过server包中的方法进行处理得到我们想要的格式
    的数据，最后在controller包中完成对请求的回应

        server包中的类和接口是对查询的结果进行一定的处理，让Controller转化为Json数据更为简单
        Controller包中的类接收请求并返回相应的Json类型的结果

     */
    @Autowired
    PublisherMapper publisherMapper;
    @Autowired
    OrderInfoMapper orderInfoMapper;
    @Override
    public Integer getCount(String date) {
        Integer dayCount = publisherMapper.selectDauTotal(date);

        return dayCount;
    }

    @Override
    public Map getHourCount(String date) {
        List<Map> map = publisherMapper.selectDauTotalHourMap(date);
        HashMap hashMap = new HashMap();
        for (Map map1 : map) {
            //注意：别名在表里都是大写，所以这里获取value时，key都必须大写
            hashMap.put(map1.get("LH"),map1.get("CT"));
        }

        return hashMap;


    }

    @Override
    public Double getOrderAmount(String date) {
       return orderInfoMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderHourAmount(String date) {
        List<Map> list = orderInfoMapper.selectOrderAmountHourMap(date);
        HashMap orderHashMap = new HashMap<>();
        for (Map map1 : list) {
            orderHashMap.put(map1.get("CREATE_HOUR"),map1.get("SUM_AMOUNT"));
        }
        return orderHashMap;

    }

}
