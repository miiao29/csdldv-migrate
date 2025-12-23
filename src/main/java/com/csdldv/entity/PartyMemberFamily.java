package com.csdldv.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "PARTY_MEMBER_FAMILY", schema = "CSDLDV_PARTY_MEMBER")
public class PartyMemberFamily implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "PARTY_MEMBER_FAMILY_ID", length = 255, nullable = false)
    private String partyMemberFamilyId;

    @Column(name = "CREATED_BY", length = 50)
    private String createdBy;

    @Column(name = "CREATED_DATE")
    private LocalDateTime createdDate;

    @Column(name = "LAST_MODIFIED_BY", length = 50)
    private String lastModifiedBy;

    @Column(name = "LAST_MODIFIED_DATE")
    private LocalDateTime lastModifiedDate;

    @Column(name = "IS_ACTIVE")
    private Integer isActive;

    @Column(name = "FAMILY_MEMBER_ID", length = 50)
    private String familyMemberId;

    @Column(name = "FULL_NAME", length = 255)
    private String fullName;

    @Column(name = "BIRTH_DATE")
    private LocalDateTime birthDate;

    @Column(name = "HOMETOWN", length = 2000)
    private String hometown;

    @Column(name = "RESIDENT_PLACE", length = 2000)
    private String residentPlace;

    @Column(name = "SYNC_SOURCE")
    private Integer syncSource;

    @Column(name = "IS_ADDED_INFORMATION")
    private Integer isAddedInformation;

    @Column(name = "TYPE")
    private Integer type;

    @Column(name = "IS_PARTY_MEMBER")
    private Integer isPartyMember;

    @Column(name = "IDENTIFY_NO", length = 50)
    private String identifyNo;

    @Column(name = "PARTY_ID", length = 50)
    private String partyId;

    @Column(name = "NATION_ID", length = 50)
    private String nationId;

    @Column(name = "CURRENT_JOB", length = 255)
    private String currentJob;

    @Column(name = "NOTE", length = 2000)
    private String note;

    @Column(name = "PARTY_MEMBER_ID", length = 255)
    private String partyMemberId;

    @Column(name = "NATIONALITY_ID", length = 50)
    private String nationalityId;

    @Column(name = "V3_QUANHE_GD_GUID", length = 50)
    private String v3QuanheGdGuid;
}

