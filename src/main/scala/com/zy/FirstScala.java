package com.zy;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;


import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class FirstScala {
    public static void main(String[] args){
        System.out.println("hello world.");  ///
    }

    public static DataSet getTest(BatchTableEnvironment tableEnv, Table query){
        DataSet result = tableEnv.toDataSet(query, Row.class);
        return result;
    }
}