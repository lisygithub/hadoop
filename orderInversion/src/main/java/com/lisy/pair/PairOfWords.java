package com.lisy.pair;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairOfWords implements WritableComparable<PairOfWords> {
    private String word;
    private String neighbor;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getNeighbor() {
        return neighbor;
    }

    public void setNeighbor(String neighbor) {
        this.neighbor = neighbor;
    }

    public PairOfWords(String word, String neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public PairOfWords() {
    }

    @Override
    public int compareTo(PairOfWords o) {
        int compare = this.word.compareTo(o.getWord());
        if(compare == 0){
            compare = this.getNeighbor().compareTo(o.neighbor);
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.word);
        out.writeUTF(this.neighbor);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = in.readUTF();
        this.neighbor = in.readUTF();
    }

    @Override
    public String toString() {
        StringBuilder sb =new StringBuilder("(")
                .append(this.word)
                .append(",")
                .append(this.neighbor)
                .append(")");
        return sb.toString();
    }
}
