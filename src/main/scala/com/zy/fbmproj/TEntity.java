package com.zy.fbmproj;


import java.io.Serializable;

public class TEntity implements Serializable {
//    uniqueKey:String,depId:String,depName:String,dimDate:String
    public String unqiueKey;
    public String depId;
    public String depName;
    public String dimDate;

    public String getUnqiueKey() {
        return unqiueKey;
    }

    public void setUnqiueKey(String unqiueKey) {
        this.unqiueKey = unqiueKey;
    }

    public String getDepId() {
        return depId;
    }

    public void setDepId(String depId) {
        this.depId = depId;
    }

    public String getDepName() {
        return depName;
    }

    public void setDepName(String depName) {
        this.depName = depName;
    }

    public String getDimDate() {
        return dimDate;
    }

    public void setDimDate(String dimDate) {
        this.dimDate = dimDate;
    }
}
