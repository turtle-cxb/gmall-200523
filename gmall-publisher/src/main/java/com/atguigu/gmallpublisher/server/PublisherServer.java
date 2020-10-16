package com.atguigu.gmallpublisher.server;

import java.util.List;
import java.util.Map;

public interface PublisherServer {
    public Integer getCount(String date);
    public Map getHourCount(String date);
}
