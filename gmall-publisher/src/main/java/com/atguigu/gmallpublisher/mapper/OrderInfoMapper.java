package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderInfoMapper {

    public Double selectOrderAmountTotal(String date);
    public List<Map> selectOrderAmountHourMap(String date);

}
