package io.cjx.blueline.utils;

import org.apache.spark.sql.SparkSession;

import java.util.TimerTask;

public class TriggerMysql extends TimerTask {
    private SparkSession _SparkSession;
    private String url;
    private String table;
    private String user;
    private String password;
    private String tmp_table;
    public TriggerMysql(SparkSession SparkSession_,String url,String table,String user,String password,String tmp_table)
    {
        this._SparkSession=SparkSession_;
        this.url=url;
        this.table=table;
        this.user=user;
        this.password=password;
        this.tmp_table=tmp_table;

    }
    @Override
    public void run() {
        this._SparkSession.read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "user")
                .option("password",password)
                .load().createOrReplaceTempView(tmp_table);

    }
}
