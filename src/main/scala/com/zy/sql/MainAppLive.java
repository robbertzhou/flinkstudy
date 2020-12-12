package com.zy.sql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @description 视频直播解决方案之 视频核心指标监控
 * @author: ZhiWen
 * @create: 2020-04-08 11:33
 **/
public class MainAppLive {
    /**源表建表语句*/
    public static final String KAFKA_SOURCE_SQL = "CREATE TABLE app_heartbeat_stream_source (\n" +
            "\t`ip`                   VARCHAR,\n" +
            "\tagent                   VARCHAR,\n" +
            "\troomid                  VARCHAR,\n" +
            "\tuserid                  VARCHAR,\n" +
            "\tabytes                  VARCHAR,\n" +
            "\tafcnt                   VARCHAR,\n" +
            "\tadrop                   VARCHAR,\n" +
            "\tafts                    VARCHAR,\n" +
            "\talat                    VARCHAR,\n" +
            "\tvbytes                  VARCHAR,\n" +
            "\tvfcnt                   VARCHAR,\n" +
            "\tvdrop                   VARCHAR,\n" +
            "\tvfts                    VARCHAR,\n" +
            "\tvlat                    VARCHAR,\n" +
            "\tublock                  VARCHAR,\n" +
            "\tdblock                  VARCHAR,\n" +
            "\tregion                  VARCHAR,\n" +
            "\tstamp             VARCHAR,\n" +
            " app_ts AS TO_TIMESTAMP(stamp), --定义生成WATERMARK的字段，\n" +
            " WATERMARK FOR app_ts AS app_ts - INTERVAL '10' SECOND --WATERMARK比数据时间线性增加10S\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'kafka',\n" +
            "\t'connector.version' = 'universal',\n" +
            "\t'connector.topic' = 'topic_live',\n" +
            "\t'update-mode' = 'append',\n" +
            "\t'connector.properties.zookeeper.connect' = '172.24.103.8:2181',\n" +
            "\t'connector.properties.bootstrap.servers' = '172.24.103.8:9092',\n" +
            "\t'connector.startup-mode' = 'latest-offset',\n" +
            "\t'format.type' = 'json'\n" +
            ")";
    /**类型转换语句*/
    public static final String TYPE_CONVER_SQL = "SELECT\n" +
            "    ip,\n" +
            "    agent,\n" +
            "    CAST(roomid AS BIGINT) as roomid,\n" +
            "    CAST(userid AS BIGINT) as userid,\n" +
            "    CAST(abytes AS BIGINT) as abytes,\n" +
            "    CAST(afcnt AS BIGINT) as afcnt,\n" +
            "    CAST(adrop AS BIGINT) as adrop,\n" +
            "    unix_timestamp(afts) as afts,\n" +
            "    CAST(alat AS BIGINT) as alat,\n" +
            "    CAST(vbytes AS BIGINT) as vbytes,\n" +
            "    CAST(vfcnt AS BIGINT) as vfcnt,\n" +
            "    CAST(vdrop AS BIGINT) as vdrop,\n" +
            "    unix_timestamp(vfts) as vfts,\n" +
            "    CAST(vlat AS BIGINT) as vlat,\n" +
            "    CAST(ublock AS BIGINT) as ublock,\n" +
            "    CAST(dblock AS BIGINT) as dblock,\n" +
            "    app_ts,\n" +
            "\tregion\n" +
            "FROM\n" +
            " app_heartbeat_stream_source";
    /**案例一*/
    public static final String WINDOW_CASE01_SQL01 = "SELECT\n" +
            "    CAST(TUMBLE_START(app_ts, INTERVAL '1' MINUTE) as VARCHAR) as app_ts,\n" +
            "    roomid,\n" +
            "    SUM(ublock) as ublock, \n" +
            "    SUM(dblock) as dblock, \n" +
            "    SUM(adrop) as adrop, \n" +
            "    SUM(vdrop) as vdrop, \n" +
            "    SUM(alat) as alat, \n" +
            "    SUM(vlat) as vlat  \n" +
            "FROM\n" +
            " view_app_heartbeat_stream\n" +
            "GROUP BY\n" +
            " TUMBLE(app_ts,INTERVAL '1' MINUTE),roomid";
    public static final String WINDOW_CASE01_SQL02 = "CREATE TABLE output_1 (\n" +
            "\tapp_ts             VARCHAR,\n" +
            "\troomid             BIGINT,\n" +
            "\tublock             BIGINT,\n" +
            "\tdblock             BIGINT,\n" +
            "\tadrop              BIGINT,\n" +
            "\tvdrop              BIGINT,\n" +
            "\talat               BIGINT,\n" +
            "\tvlat               BIGINT\n" +
            "--不支持 \tPRIMARY KEY (roomid)\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'jdbc',\n" +
            "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
            "\t'connector.table' = 'live_output_1',\n" +
            "\t'connector.username' = 'root',\n" +
            "\t'connector.password' = '123456',\n" +
            "\t'connector.write.flush.max-rows' = '10',\n" +
            "\t'connector.write.flush.interval' = '5s'\n" +
            ")";
    public static final String WINDOW_CASE01_SQL03 = "INSERT INTO output_1\n" +
            "SELECT * FROM room_error_statistics_10min";

    /**案例二*/
    public static final String WINDOW_CASE02_SQL01 = "SELECT\n" +
            "    CAST(TUMBLE_START(app_ts, INTERVAL '1' MINUTE) as VARCHAR) as app_ts,\n" +
            "    region,\n" +
            "    SUM(alat)/COUNT(alat) as alat,\n" +
            "    SUM(vlat)/COUNT(vlat) as vlat\n" +
            "FROM\n" +
            "    view_app_heartbeat_stream\n" +
            "GROUP BY\n" +
            "    TUMBLE(app_ts, INTERVAL '1' MINUTE), region";
    public static final String WINDOW_CASE02_SQL02 = "CREATE TABLE output_2 (\n" +
            "    app_ts VARCHAR,\n" +
            "    region VARCHAR,\n" +
            "    alat   DOUBLE,\n" +
            "    vlat   DOUBLE\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'jdbc',\n" +
            "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
            "\t'connector.table' = 'live_output_2',\n" +
            "\t'connector.username' = 'root',\n" +
            "\t'connector.password' = '123456',\n" +
            "\t'connector.write.flush.max-rows' = '10',\n" +
            "\t'connector.write.flush.interval' = '5s'\n" +
            ")";
    public static final String WINDOW_CASE02_SQL03 = "INSERT INTO output_2\n" +
            "SELECT * FROM region_lat_statistics_10min";

    /**案例三*/
    public static final String WINDOW_CASE03_SQL01 = "SELECT\n" +
            "    CAST(TUMBLE_START(app_ts, INTERVAL '1' MINUTE) as VARCHAR) as app_ts,\n" +
            "    SUM(IF(ublock <> 0 OR dblock <> 0, 1, 0)) / CAST(COUNT(DISTINCT userid) AS DOUBLE) as block_rate\n" +
            "FROM\n" +
            "    view_app_heartbeat_stream\n" +
            "GROUP BY\n" +
            "    TUMBLE(app_ts, INTERVAL '1' MINUTE)";
    public static final String WINDOW_CASE03_SQL02 = "CREATE TABLE output_3 (\n" +
            "\tapp_ts                VARCHAR,\n" +
            "\tblock_rate            DOUBLE\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'jdbc',\n" +
            "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
            "\t'connector.table' = 'live_output_3',\n" +
            "\t'connector.username' = 'root',\n" +
            "\t'connector.password' = '123456',\n" +
            "\t'connector.write.flush.max-rows' = '10',\n" +
            "\t'connector.write.flush.interval' = '5s'\n" +
            ")";
    public static final String WINDOW_CASE03_SQL03 = "INSERT INTO output_3\n" +
            "SELECT * FROM block_total_statistics_10min";

    /**案例四*/
    public static final String WINDOW_CASE04_SQL01 = "SELECT\n" +
            "    CAST(TUMBLE_START(app_ts, INTERVAL '1' MINUTE) as VARCHAR) as app_ts,\n" +
            "    SUM(ublock+dblock) / CAST(COUNT(DISTINCT userid) AS DOUBLE) as block_peruser\n" +
            "FROM\n" +
            "    view_app_heartbeat_stream\n" +
            "GROUP BY\n" +
            "    TUMBLE(app_ts, INTERVAL '1' MINUTE)";
    public static final String WINDOW_CASE04_SQL02 = "CREATE TABLE output_4 (\n" +
            "\tapp_ts                   VARCHAR,\n" +
            "\tblock_peruser            DOUBLE\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'jdbc',\n" +
            "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
            "\t'connector.table' = 'live_output_4',\n" +
            "\t'connector.username' = 'root',\n" +
            "\t'connector.password' = '123456',\n" +
            "\t'connector.write.flush.max-rows' = '10',\n" +
            "\t'connector.write.flush.interval' = '5s'\n" +
            ")";
    public static final String WINDOW_CASE04_SQL03 = "INSERT INTO output_4\n" +
            "SELECT * FROM block_peruser_statistics_10min";

    public static void main(String[] args) throws Exception {//todo 已实现

        //获取流式环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//
//        //设置时间类型 至关重要
//        bsEnv.setParallelism(5).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        //todo 创建一个 TableEnvironment
//        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//
//
//
//
//
//        /*--***********************建源表 转类型*******************--*/
//        bsTableEnv.sqlUpdate(KAFKA_SOURCE_SQL);
//        Table view01 = bsTableEnv.sqlQuery(TYPE_CONVER_SQL);
//        bsTableEnv.createTemporaryView("view_app_heartbeat_stream",view01);
//
//        bsTableEnv.toAppendStream(view01,Row.class).print();
//
//        /*--***********************************1.统计房间故障，故障包括卡顿、丢帧、音视频不同步信息，使用10分钟一个窗口进行统计*********************************--*/
//        Table view11 = bsTableEnv.sqlQuery(WINDOW_CASE01_SQL01);
//        bsTableEnv.createTemporaryView("room_error_statistics_10min",view11);
//
//        //bsTableEnv.toRetractStream(view11,Row.class).print();
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE01_SQL02);
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE01_SQL03);
//
//
//        /*-***********************************2.分地域统计数据端到端延迟平均情况，每10分钟统计音频、视频平均延迟情况***********************************-*/
//
//
//        Table view21 = bsTableEnv.sqlQuery(WINDOW_CASE02_SQL01);
//        bsTableEnv.createTemporaryView("region_lat_statistics_10min",view21);
//
//        //bsTableEnv.toRetractStream(view21,Row.class).print();
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE02_SQL02);
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE02_SQL03);
//
//
//
//        /*-************************3.统计实时整体卡顿率，即出现卡顿的在线用户数/在线总用户数*100%，通过此指标可以衡量当前卡顿影响的人群范围。***********************-*/
//
//        Table view31 = bsTableEnv.sqlQuery(WINDOW_CASE03_SQL01);
//        bsTableEnv.createTemporaryView("block_total_statistics_10min",view31);
//
//        //bsTableEnv.toRetractStream(view31,Row.class).print();
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE03_SQL02);
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE03_SQL03);
//
//        /*-***************************4.统计人均卡顿次数，即在线卡顿总次数/在线用户数，通过此指标可以从卡顿频次上衡量整体的卡顿严重程度。**************************-*/
//
//        Table view41 = bsTableEnv.sqlQuery(WINDOW_CASE04_SQL01);
//        bsTableEnv.createTemporaryView("block_peruser_statistics_10min",view41);
//        //bsTableEnv.toRetractStream(view41,Row.class).print();
//
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE04_SQL02);
//
//        bsTableEnv.sqlUpdate(WINDOW_CASE04_SQL03);
//
//        /*-***********************************开始执行**************************************-*/
//        //输出并行度
//        System.out.println(bsEnv.getParallelism());
//        //执行query语句 1.10在execute之前会先执行update语句 1.11后更改
//        bsTableEnv.execute("live");



    }
}
//
//        案例补充 topN
///**案例七*/
//public static final String WINDOW_CASE07_SQL01 = "SELECT\n" +
//        "    CAST(TUMBLE_START(app_ts, INTERVAL '60' SECOND) as VARCHAR) as app_ts,\n" +
//        "    roomid as room_id,\n" +
//        "    COUNT(DISTINCT userid) as app_room_user_cnt\n" +
//        "FROM\n" +
//        "    view_app_heartbeat_stream\n" +
//        "GROUP BY\n" +
//        "    TUMBLE(app_ts, INTERVAL '60' SECOND), roomid";
//public static final String WINDOW_CASE07_SQL02 = "SELECT\n" +
//        "  app_ts,\n" +
//        "  room_id,\n" +
//        "  app_room_user_cnt,\n" +
//        "  ranking\n" +
//        "  --PRIMARY KEY (app_ts,room_id,ranking)\n" +
//        "FROM\n" +
//        "(\n" +
//        "    SELECT \n" +
//        "        app_ts,\n" +
//        "        room_id,\n" +
//        "        app_room_user_cnt,\n" +
//        "        ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY app_room_user_cnt desc) AS ranking\n" +
//        "    FROM\n" +
//        "        view_app_room_visit_1min\n" +
//        ") WHERE ranking <= 10";
//public static final String WINDOW_CASE07_SQL03 = "CREATE TABLE output_7 (\n" +
//        "\tapp_ts                       VARCHAR,\n" +
//        "\troomid                       BIGINT,\n" +
//        "\tapp_room_user_cnt            BIGINT,\n" +
//        "\tranking                      BIGINT\n" +
//        ") WITH (\n" +
//        "\t'connector.type' = 'jdbc',\n" +
//        "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
//        "\t'connector.table' = 'live_output_7',\n" +
//        "\t'connector.username' = 'root',\n" +
//        "\t'connector.password' = '123456',\n" +
//        "\t'connector.write.flush.max-rows' = '10',\n" +
//        "\t'connector.write.flush.interval' = '5s'\n" +
//        ")";
//public static final String WINDOW_CASE07_SQL04 = "INSERT INTO output_7\n" +
//        "SELECT * FROM view_app_room_visit_top10";


/*-***********************************7.热门直播房间排行**************************************-*/
       /*Table view71 = bsTableEnv.sqlQuery(WINDOW_CASE07_SQL01);
        bsTableEnv.createTemporaryView("view_app_room_visit_1min",view71);
        Table view72 = bsTableEnv.sqlQuery(WINDOW_CASE07_SQL02);
        bsTableEnv.createTemporaryView("view_app_room_visit_top10",view72);
        bsTableEnv.toRetractStream(view72,Row.class).print();
        bsTableEnv.sqlUpdate(WINDOW_CASE07_SQL03);
        bsTableEnv.sqlUpdate(WINDOW_CASE07_SQL04);*/
