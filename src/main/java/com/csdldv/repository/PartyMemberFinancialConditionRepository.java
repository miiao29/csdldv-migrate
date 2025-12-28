package com.csdldv.repository;

import com.csdldv.entity.PartyMemberFinancialCondition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface PartyMemberFinancialConditionRepository extends JpaRepository<PartyMemberFinancialCondition, String> {

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE (NVL(s.THUNHAP1, 0) > 0
                OR s.TMP_THUNHAP1 IS NOT NULL
                OR s.MA_HDKT IS NOT NULL)
              AND s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE (NVL(s.THUNHAP1, 0) > 0
                OR s.TMP_THUNHAP1 IS NOT NULL
                OR s.MA_HDKT IS NOT NULL)
              AND s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE (NVL(s.THUNHAP1, 0) > 0
                OR s.TMP_THUNHAP1 IS NOT NULL
                OR s.MA_HDKT IS NOT NULL)
              AND s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom26();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FINANCIAL_CONDITION t
            USING (
                SELECT 1                                                   AS IS_ACTIVE,
                       s.THUNHAP1                                          AS HOUSEHOLD_ANNUAL_INCOME,
                       s.TMP_THUNHAP1                                      AS PER_CAPITA_ANNUAL_INCOME,
                       k.TEN_HDKT                                          AS ECONOMIC_ACTIVITY,
                       s.SOLDTHUE                                          AS COUNT_WORKER,
                       (SELECT a.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER a
                        WHERE a.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1)                                  AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                         LEFT JOIN CSDLDV_20.orgHDONG_KT k
                                   ON s.MA_HDKT = k.MA_HDKT
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) s
            ON (t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.HOUSEHOLD_ANNUAL_INCOME = s.HOUSEHOLD_ANNUAL_INCOME,
                    t.PER_CAPITA_ANNUAL_INCOME = s.PER_CAPITA_ANNUAL_INCOME,
                    t.ECONOMIC_ACTIVITY = s.ECONOMIC_ACTIVITY,
                    t.COUNT_WORKER = s.COUNT_WORKER
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_ID,
                        IS_ACTIVE,
                        HOUSEHOLD_ANNUAL_INCOME,
                        PER_CAPITA_ANNUAL_INCOME,
                        ECONOMIC_ACTIVITY,
                        COUNT_WORKER,
                        PARTY_MEMBER_ID)
                VALUES (CSDLDV.PARTY_MEMBER_FINANCIAL_CONDITION_seq.NEXTVAL,
                        s.IS_ACTIVE,
                        s.HOUSEHOLD_ANNUAL_INCOME,
                        s.PER_CAPITA_ANNUAL_INCOME,
                        s.ECONOMIC_ACTIVITY,
                        s.COUNT_WORKER,
                        s.PARTY_MEMBER_ID)
            """, nativeQuery = true)
    int mergeFinancialConditionFrom20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FINANCIAL_CONDITION t
            USING (
                SELECT 1                                                   AS IS_ACTIVE,
                       s.THUNHAP1                                          AS HOUSEHOLD_ANNUAL_INCOME,
                       s.TMP_THUNHAP1                                      AS PER_CAPITA_ANNUAL_INCOME,
                       k.TEN_HDKT                                          AS ECONOMIC_ACTIVITY,
                       s.SOLDTHUE                                          AS COUNT_WORKER,
                       (SELECT a.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER a
                        WHERE a.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1)                                  AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                         LEFT JOIN CSDLDV_25.orgHDONG_KT k
                                   ON s.MA_HDKT = k.MA_HDKT
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) s
            ON (t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.HOUSEHOLD_ANNUAL_INCOME = s.HOUSEHOLD_ANNUAL_INCOME,
                    t.PER_CAPITA_ANNUAL_INCOME = s.PER_CAPITA_ANNUAL_INCOME,
                    t.ECONOMIC_ACTIVITY = s.ECONOMIC_ACTIVITY,
                    t.COUNT_WORKER = s.COUNT_WORKER
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_ID,
                        IS_ACTIVE,
                        HOUSEHOLD_ANNUAL_INCOME,
                        PER_CAPITA_ANNUAL_INCOME,
                        ECONOMIC_ACTIVITY,
                        COUNT_WORKER,
                        PARTY_MEMBER_ID)
                VALUES (CSDLDV.PARTY_MEMBER_FINANCIAL_CONDITION_seq.NEXTVAL,
                        s.IS_ACTIVE,
                        s.HOUSEHOLD_ANNUAL_INCOME,
                        s.PER_CAPITA_ANNUAL_INCOME,
                        s.ECONOMIC_ACTIVITY,
                        s.COUNT_WORKER,
                        s.PARTY_MEMBER_ID)
            """, nativeQuery = true)
    int mergeFinancialConditionFrom25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FINANCIAL_CONDITION t
            USING (
                SELECT 1                                                   AS IS_ACTIVE,
                       s.THUNHAP1                                          AS HOUSEHOLD_ANNUAL_INCOME,
                       s.TMP_THUNHAP1                                      AS PER_CAPITA_ANNUAL_INCOME,
                       k.TEN_HDKT                                          AS ECONOMIC_ACTIVITY,
                       s.SOLDTHUE                                          AS COUNT_WORKER,
                       (SELECT a.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER a
                        WHERE a.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1)                                  AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                         LEFT JOIN CSDLDV_26.orgHDONG_KT k
                                   ON s.MA_HDKT = k.MA_HDKT
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) s
            ON (t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.HOUSEHOLD_ANNUAL_INCOME = s.HOUSEHOLD_ANNUAL_INCOME,
                    t.PER_CAPITA_ANNUAL_INCOME = s.PER_CAPITA_ANNUAL_INCOME,
                    t.ECONOMIC_ACTIVITY = s.ECONOMIC_ACTIVITY,
                    t.COUNT_WORKER = s.COUNT_WORKER
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_ID,
                        IS_ACTIVE,
                        HOUSEHOLD_ANNUAL_INCOME,
                        PER_CAPITA_ANNUAL_INCOME,
                        ECONOMIC_ACTIVITY,
                        COUNT_WORKER,
                        PARTY_MEMBER_ID)
                VALUES (CSDLDV.PARTY_MEMBER_FINANCIAL_CONDITION_seq.NEXTVAL,
                        s.IS_ACTIVE,
                        s.HOUSEHOLD_ANNUAL_INCOME,
                        s.PER_CAPITA_ANNUAL_INCOME,
                        s.ECONOMIC_ACTIVITY,
                        s.COUNT_WORKER,
                        s.PARTY_MEMBER_ID)
            """, nativeQuery = true)
    int mergeFinancialConditionFrom26(List<String> soyeuIds);
}

