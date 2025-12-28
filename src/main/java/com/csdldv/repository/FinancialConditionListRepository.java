package com.csdldv.repository;

import com.csdldv.entity.FinancialConditionList;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface FinancialConditionListRepository extends JpaRepository<FinancialConditionList, String> {

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsFrom26();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.DTNHA2 IS NOT NULL OR s.NHA2 IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu9From20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.DTNHA2 IS NOT NULL OR s.NHA2 IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu9From25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.DTNHA2 IS NOT NULL OR s.NHA2 IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu9From26();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.TMP_DATCAP IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu10From20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.TMP_DATCAP IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu10From25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.TMP_DATCAP IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu10From26();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATMUA IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu11From20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATMUA IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu11From25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATMUA IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu11From26();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATTT IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu12From20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATTT IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu12From25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND s.DATTT IS NOT NULL
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu12From26();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_20.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.TSCOGTRI IS NOT NULL OR s.TSGTRI IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu13From20();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_25.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.TSCOGTRI IS NOT NULL OR s.TSGTRI IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu13From25();

    @Query(value = """
            SELECT s.SOYEU_ID
            FROM CSDLDV_26.SOYEU s
            WHERE s.SYNCCODE <> 3
              AND (s.TSCOGTRI IS NOT NULL OR s.TSGTRI IS NOT NULL)
            ORDER BY s.SOYEU_ID
            """, nativeQuery = true)
    List<String> findAllSoyeuIdsForMenu13From26();

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA,
                       s.MA_NHADAT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 1
                    AND tgt.DETAIL = src.MA_NHADAT
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        1,
                        src.DTNHA,
                        src.MA_NHADAT,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionListFrom20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA,
                       s.MA_NHADAT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 1
                    AND tgt.DETAIL = src.MA_NHADAT
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        1,
                        src.DTNHA,
                        src.MA_NHADAT,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionListFrom25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA,
                       s.MA_NHADAT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 1
                    AND tgt.DETAIL = src.MA_NHADAT
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        1,
                        src.DTNHA,
                        src.MA_NHADAT,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionListFrom26(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA2,
                       s.NHA2,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 2
                    AND tgt.DETAIL = src.NHA2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA2,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        2,
                        src.DTNHA2,
                        src.NHA2,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList2From20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA2,
                       s.NHA2,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 2
                    AND tgt.DETAIL = src.NHA2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA2,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        2,
                        src.DTNHA2,
                        src.NHA2,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList2From25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DTNHA2,
                       s.NHA2,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 1
                    AND tgt.ASSET_GROUP_TYPE = 2
                    AND tgt.DETAIL = src.NHA2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DTNHA2,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        1,
                        2,
                        src.DTNHA2,
                        src.NHA2,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList2From26(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TMP_DATCAP,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 1
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TMP_DATCAP,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        1,
                        src.TMP_DATCAP,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList3From20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TMP_DATCAP,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 1
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TMP_DATCAP,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        1,
                        src.TMP_DATCAP,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList3From25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TMP_DATCAP,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 1
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TMP_DATCAP,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        1,
                        src.TMP_DATCAP,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList3From26(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATMUA,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATMUA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        2,
                        src.DATMUA,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList4From20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATMUA,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATMUA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        2,
                        src.DATMUA,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList4From25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATMUA,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 2
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATMUA,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        2,
                        src.DATMUA,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList4From26(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATTT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATTT,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        3,
                        src.DATTT,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList5From20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATTT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATTT,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        3,
                        src.DATTT,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList5From25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.DATTT,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 2
                    AND tgt.ASSET_GROUP_TYPE = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.DATTT,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        2,
                        3,
                        src.DATTT,
                        NULL,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList5From26(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TSCOGTRI,
                       s.TSGTRI,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_20.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TSGTRI,
                    tgt.DETAIL = src.TSCOGTRI,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        3,
                        NULL,
                        src.TSGTRI,
                        src.TSCOGTRI,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList6From20(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TSCOGTRI,
                       s.TSGTRI,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_25.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TSGTRI,
                    tgt.DETAIL = src.TSCOGTRI,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        3,
                        NULL,
                        src.TSGTRI,
                        src.TSCOGTRI,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList6From25(List<String> soyeuIds);

    @Modifying
    @Query(value = """
            MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
            USING (
                SELECT s.SOYEU_ID,
                       s.TSCOGTRI,
                       s.TSGTRI,
                       (SELECT m.PARTY_MEMBER_ID
                        FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                        WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                          AND ROWNUM = 1) AS PARTY_MEMBER_ID
                FROM CSDLDV_26.SOYEU s
                WHERE s.SOYEU_ID IN (:soyeuIds)
            ) src
            ON (
                tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                    AND tgt.ASSET_GROUP = 3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ASSETS_VALUE = src.TSGTRI,
                    tgt.DETAIL = src.TSCOGTRI,
                    tgt.IS_ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (FINANCIAL_CONDITION_LIST_ID,
                        ASSET_GROUP,
                        ASSET_GROUP_TYPE,
                        ASSETS_VALUE,
                        DETAIL,
                        IS_ACTIVE,
                        PARTY_MEMBER_ID,
                        FINANCIAL_CONDITION_ID)
                VALUES (CSDLDV.FINANCIAL_CONDITION_LIST_seq.NEXTVAL,
                        3,
                        NULL,
                        src.TSGTRI,
                        src.TSCOGTRI,
                        1,
                        src.PARTY_MEMBER_ID,
                        NULL)
            """, nativeQuery = true)
    int mergeFinancialConditionList6From26(List<String> soyeuIds);
}

