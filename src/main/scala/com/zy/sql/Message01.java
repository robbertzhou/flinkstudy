package com.zy.sql;

public class Message01 {
    private String account_id;

    public void setAccount_id(String toString) {
        this.account_id = toString;
    }

    public String getAccount_id() {
        return account_id;
    }

    private String client_ip;

    public void setClient_ip(String s) {
        this.client_ip = s;
    }

    public String getClient_ip() {
        return this.client_ip;
    }

    private String client_info;

    public void setClient_info(String s) {
        this.client_info = s;
    }

    public String getClient_info() {
        return client_info;
    }

    private String action;

    public void setAction(String source) {
        this.action = source;
    }

    public String getAction() {
        return action;
    }
}
