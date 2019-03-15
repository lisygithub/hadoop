package com.lisy.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String stockSymbol;
    private long timestamp;
    @Override
    public int compareTo(CompositeKey o) {
        int compare = this.stockSymbol.compareTo(o.getStockSymbol());
        if(0 == compare){
            compare = this.timestamp>o.getTimestamp()?1:-1;
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stockSymbol);
        out.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockSymbol = in.readUTF();
        this.timestamp = in.readLong();
    }

    public void set(String stockSymbol,long timestamp){
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
