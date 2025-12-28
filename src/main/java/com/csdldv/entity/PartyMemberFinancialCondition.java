package com.csdldv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Entity
@Data
@Table(name = "PARTY_MEMBER_FINANCIAL_CONDITION", schema = "CSDLDV_PARTY_MEMBER")
public class PartyMemberFinancialCondition implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "FINANCIAL_CONDITION_ID", length = 4000, nullable = false)
    private String financialConditionId;

    @Column(name = "IS_ACTIVE")
    private Integer isActive;

    @Column(name = "HOUSEHOLD_ANNUAL_INCOME")
    private BigDecimal householdAnnualIncome;

    @Column(name = "PER_CAPITA_ANNUAL_INCOME")
    private BigDecimal perCapitaAnnualIncome;

    @Column(name = "ECONOMIC_ACTIVITY", length = 4000)
    private String economicActivity;

    @Column(name = "COUNT_WORKER")
    private Integer countWorker;

    @Column(name = "PARTY_MEMBER_ID", length = 4000)
    private String partyMemberId;
}

