package com.csdldv.repository;

import com.csdldv.entity.PartyMemberTrainingProcess;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface PartyMemberTrainingProcessRepository extends JpaRepository<PartyMemberTrainingProcess, String> {

    @Modifying
    @Query(value = """
            DELETE FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
            WHERE
                  tp.TRAINING_TYPE_ID IS NULL
               OR NOT EXISTS (
                    SELECT 1
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    WHERE TO_CHAR(c.CATEGORY_ID) = tp.TRAINING_TYPE_ID
               )
               OR tp.TRAINING_LEVEL_ID IS NULL
               OR NOT EXISTS (
                    SELECT 1
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    WHERE TO_CHAR(c.CATEGORY_ID) = tp.TRAINING_LEVEL_ID
               )
            """, nativeQuery = true)
    int deleteInvalidRecords();

    @Modifying
    @Query(value = """
            DELETE FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS
            WHERE ROWID NOT IN (
                SELECT MIN(ROWID)
                FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS
                GROUP BY
                    TRAINING_TYPE_ID,
                    TRAINING_LEVEL_ID,
                    V3_QUATRINH_DAOTAO_GUID
            )
            """, nativeQuery = true)
    int deleteDuplicateRecords();

    @Modifying
    @Query(value = """
            INSERT INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS
            (PARTY_MEMBER_TRAINING_ID,
             PARTY_MEMBER_ID,
             IS_ACTIVE,
             V3_QUATRINH_DAOTAO_GUID,
             FROM_DATE,
             TO_DATE,
             TRAINING_COUNTRY_ID,
             TRAINING_MODE_ID,
             NOTE,
             TRAINING_LEVEL_ID,
             TRAINING_TYPE_ID)
            SELECT PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                   (SELECT p.PARTY_MEMBER_ID
                    FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                    WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                      AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                   1                  AS IS_ACTIVE,
                   dt.GUIDKEY,
                   CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC),
                   CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP),
                   (SELECT c.CATEGORY_ID
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    WHERE c.CATEGORY_GROUP_CODE = 'QUOCGIA'
                      AND c.CATEGORY_CODE = dt.MA_NUOC
                      AND ROWNUM = 1) AS TRAINING_COUNTRY_ID,
                   (SELECT c.CATEGORY_ID
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCDAOTAO'
                      AND c.CATEGORY_CODE = dt.MA_HTDTAO
                      AND ROWNUM = 1) AS TRAINING_MODE_ID,
                   dt.GHICHU,
                   c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                   (SELECT c2.CATEGORY_ID
                    FROM CSDLDV_CATEGORY.CATEGORY c2
                    WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                      AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                      AND ROWNUM = 1) AS TRAINING_TYPE_ID
            FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
                     JOIN CSDLDV_PARTY_MEMBER.ORGLLCT o
                          ON dt.MA_LLCT = o.MA_LLCT
                     JOIN CSDLDV_CATEGORY.CATEGORY c1
                          ON c1.CATEGORY_NAME = o.TEN_LLCT
            WHERE o.SYNCCODE <> 3
              AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
              AND c1.TCTK_CODE LIKE 'LLCT%'
            """, nativeQuery = true)
    int insertMaLLCTRecords();
}

