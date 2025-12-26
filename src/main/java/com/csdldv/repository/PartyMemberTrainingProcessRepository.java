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
}

