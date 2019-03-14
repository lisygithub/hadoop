package com.lisy.pair;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class PairOfStrings implements WritableComparable<PairOfStrings> {

    private String left;
    private String right;
    @Override
    public int compareTo(PairOfStrings pair) {
        int compare = this.left.compareTo(pair.left);
        if(compare == 0){
            compare = this.right.compareTo(pair.right);
        }
        return compare;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(left);
        out.writeUTF(right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.left = in.readUTF();
        this.right = in.readUTF();
    }

    public PairOfStrings() {
    }

    public PairOfStrings(String left, String right) {
        set(left,right);
    }

    public void set(String left, String right) {
        this.left = left;
        this.right = right;
    }

    public String getLeft() {
        return left;
    }

    public void setLeft(String left) {
        this.left = left;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }
}
