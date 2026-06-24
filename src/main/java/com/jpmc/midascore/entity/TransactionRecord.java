package com.jpmc.midascore.entity;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
public class TransactionRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @ManyToOne
    private UserRecord sender;

    @ManyToOne
    private UserRecord recipient;

    @Column(nullable = false)
    private float amount;

    private float incentive;

    protected TransactionRecord() {
    }

    public TransactionRecord(UserRecord sender,
                             UserRecord recipient,
                             float amount,
                             float incentive) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.incentive = incentive;
    }
}
