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
@Table(name = "FINANCIAL_CONDITION_LIST", schema = "CSDLDV_PARTY_MEMBER")
public class FinancialConditionList implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "FINANCIAL_CONDITION_LIST_ID", length = 4000, nullable = false)
    private String financialConditionListId;

    @Column(name = "ASSET_GROUP")
    private Integer assetGroup;

    @Column(name = "ASSET_GROUP_TYPE")
    private Integer assetGroupType;

    @Column(name = "ASSETS_VALUE")
    private BigDecimal assetsValue;

    @Column(name = "DETAIL", length = 4000)
    private String detail;

    @Column(name = "IS_ACTIVE")
    private Integer isActive;

    @Column(name = "PARTY_MEMBER_ID", length = 4000)
    private String partyMemberId;

    @Column(name = "FINANCIAL_CONDITION_ID", length = 4000)
    private String financialConditionId;
}

