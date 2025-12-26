package com.csdldv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.io.Serializable;

@Entity
@Data
@Table(name = "PARTY_MEMBER_DISCIPLINE", schema = "CSDLDV_PARTY_MEMBER")
public class PartyMemberDiscipline implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "PARTY_MEMBER_DISCIPLINE_ID", length = 4000, nullable = false)
    private String partyMemberDisciplineId;
}

