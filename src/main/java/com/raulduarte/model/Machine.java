package com.raulduarte.model;

public class Machine {

    private String id;
    private long amount;

    public Machine(String id, long amount) {

        this.id     = id;
        this.amount = amount;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
