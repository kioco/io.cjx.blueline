package io.cjx.blueline.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class DBMangerPool implements Serializable {
    public ComboPooledDataSource combo;
    public  DBMangerPool (String user,String password,String url,int maxpoolsize,int minpoolsize) throws Exception{
            combo = new ComboPooledDataSource();
            combo.setDriverClass("com.mysql.jdbc.Driver"); // 加载驱动
            combo.setPassword(password);
            combo.setUser(user);
            combo.setJdbcUrl(url); // 地址可以换成云端
            combo.setMaxPoolSize(maxpoolsize); //池中最大的数量
            combo.setMinPoolSize(minpoolsize);
            combo.setInitialPoolSize(1);

    }
    //外面调用获得数据库连接对象，调用此方法
    public Connection getconnection() throws SQLException {
        return combo.getConnection();
    }
}
