package com.empdetails.poc.producer;
public class Counter {

  private int total = 0;
  private int success = 0;
  private int failure = 0;

  public Counter(){}

  public void incTotal(){
    this.total ++;
  }

  public int getTotal(){
    return total;
  }

  public void incSuccess(){
    this.success ++;
  }

  public int getSuccess(){
    return success;
  }
  public void incFailure(){
    this.failure ++;
  }

  public int getFailure(){
    return failure ;
  }
}