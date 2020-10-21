package com.atguigu.gmallpublisher.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PublisherServer {
    public Integer getCount(String date);
    public Map getHourCount(String date);

    public Double getOrderAmount(String date);
    public Map getOrderHourAmount(String date);

    public String getSaleDetail(String date,int startpage,int size,String keyword) throws IOException;
}
