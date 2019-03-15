package com.lisy.util;
/**
*   移动平均算法
*/
public class MovingAverage {
    private double sum = 0;
    private final int period ;
    private double[]  window = null;
    private int pointer = 0;
    private int size = 0;

    public MovingAverage(int period) {
        if(period < 1){
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new double[period];
    }

    public void addNewNumber(double number){
        sum += number;
        if(period>size){
            window[pointer++] = number;
            size++;
        }else {
            pointer = pointer%pointer;
            sum -= window[pointer];
            window[pointer++] = number;
        }
    }

    public double getMovingAverage(){
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum/size;
    }
}
