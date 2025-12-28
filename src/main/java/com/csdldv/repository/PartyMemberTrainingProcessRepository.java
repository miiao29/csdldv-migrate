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
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_20.QTRINH_DT dt
                         JOIN CSDLDV_20.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaLLCTRecordsFrom20();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_25.QTRINH_DT dt
                         JOIN CSDLDV_25.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaLLCTRecordsFrom25();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_26.QTRINH_DT dt
                         JOIN CSDLDV_26.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaLLCTRecordsFrom26();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_20.QTRINH_DT dt
                         JOIN CSDLDV_20.ORGDAOTAO o
                              ON dt.MA_BANGDT = o.MA_BANGDT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGDT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'CM%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGDTRecordsFrom20();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_25.QTRINH_DT dt
                         JOIN CSDLDV_25.ORGDAOTAO o
                              ON dt.MA_BANGDT = o.MA_BANGDT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGDT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'CM%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGDTRecordsFrom25();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_26.QTRINH_DT dt
                         JOIN CSDLDV_26.ORGDAOTAO o
                              ON dt.MA_BANGDT = o.MA_BANGDT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGDT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'CM%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGDTRecordsFrom26();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                          AND c2.TCTK_CODE <> '0'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_20.QTRINH_DT dt
                         JOIN CSDLDV_20.ORGN_NGU o
                              ON dt.MA_BANGNN = o.MA_BANGNN
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGNN
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'NN%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGNNRecordsFrom20();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                          AND c2.TCTK_CODE <> '0'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_25.QTRINH_DT dt
                         JOIN CSDLDV_25.ORGN_NGU o
                              ON dt.MA_BANGNN = o.MA_BANGNN
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGNN
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'NN%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGNNRecordsFrom25();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
            USING (
                SELECT (SELECT p.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                        WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID,
                       1                  AS IS_ACTIVE,
                       dt.GUIDKEY         AS V3_QUATRINH_DAOTAO_GUID,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC) AS FROM_DATE,
                       CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
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
                       dt.GHICHU         AS NOTE,
                       c1.CATEGORY_ID     AS TRAINING_LEVEL_ID,
                       (SELECT c2.CATEGORY_ID
                        FROM CSDLDV_CATEGORY.CATEGORY c2
                        WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                          AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                          AND c2.TCTK_CODE <> '0'
                          AND ROWNUM = 1) AS TRAINING_TYPE_ID
                FROM CSDLDV_26.QTRINH_DT dt
                         JOIN CSDLDV_26.ORGN_NGU o
                              ON dt.MA_BANGNN = o.MA_BANGNN
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_BANGNN
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'NN%'
            ) s
            ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
                AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
                AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID,
                    t.IS_ACTIVE = s.IS_ACTIVE,
                    t.FROM_DATE = s.FROM_DATE,
                    t.TO_DATE = s.TO_DATE,
                    t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                    t.TRAINING_MODE_ID = s.TRAINING_MODE_ID,
                    t.NOTE = s.NOTE
            WHEN NOT MATCHED THEN
                INSERT (PARTY_MEMBER_TRAINING_ID,
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
                VALUES (CSDLDV.PARTY_MEMBER_TRAINING_PROCESS_SEQ.NEXTVAL,
                        s.PARTY_MEMBER_ID,
                        s.IS_ACTIVE,
                        s.V3_QUATRINH_DAOTAO_GUID,
                        s.FROM_DATE,
                        s.TO_DATE,
                        s.TRAINING_COUNTRY_ID,
                        s.TRAINING_MODE_ID,
                        s.NOTE,
                        s.TRAINING_LEVEL_ID,
                        s.TRAINING_TYPE_ID)
            """, nativeQuery = true)
    int insertMaBANGNNRecordsFrom26();
}

