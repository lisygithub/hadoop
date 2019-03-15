package com.lisy.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesData implements Writable {
    private String timeFormat;
    private double price;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(timeFormat);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timeFormat = in.readUTF();
        this.price = in.readDouble();
    }
    public void set(String timeFormat,double price){
        this.timeFormat = timeFormat;
        this.price = price;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
