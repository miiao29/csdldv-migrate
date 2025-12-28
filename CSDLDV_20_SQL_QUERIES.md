# Tất cả câu SELECT và MERGE cho CSDLDV_20

## Menu 1: Family Migration

### SELECT
```sql
SELECT a.PARTY_MEMBER_FAMILY_ID
FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY a
WHERE a.TYPE IS NULL
  AND EXISTS (SELECT 1
              FROM CSDLDV_PARTY_MEMBER.qhe_gd b
              WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                AND b.MA_TV_GD IN (
                                   'A2', 'A3', 'B5', 'B6', 'C7', 'C8',
                                   'E2', 'E3', 'M2', 'M3', 'R1', 'R2'
                  ))
ORDER BY a.PARTY_MEMBER_FAMILY_ID
```

### UPDATE
```sql
UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
SET TYPE = 1
WHERE PARTY_MEMBER_FAMILY_ID IN :ids
```

---

## Menu 2: PARTY_MEMBER_DISCIPLINE Migration

### SELECT
```sql
SELECT x.PARTY_MEMBER_DISCIPLINE_ID
FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
WHERE EXISTS (SELECT 1
              FROM CSDLDV_PARTY_MEMBER.KT_KL kl
                       JOIN CSDLDV_CATEGORY.CATEGORY c
                            ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
              WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                AND kl.GUIDKEY = x.V3_KT_KL_GUID)
ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
```

### UPDATE
```sql
UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
        (SELECT c.CATEGORY_ID, kl.LYDO
         FROM CSDLDV_CATEGORY.CATEGORY c
                  JOIN CSDLDV_PARTY_MEMBER.KT_KL kl
                       ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
         WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
           AND kl.GUIDKEY = x.V3_KT_KL_GUID)
WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN :ids
```

---

## Menu 4: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_LLCT)

### SELECT
```sql
SELECT dt.GUIDKEY
FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
         JOIN CSDLDV_PARTY_MEMBER.ORGLLCT o
              ON dt.MA_LLCT = o.MA_LLCT
         JOIN CSDLDV_CATEGORY.CATEGORY c1
              ON c1.CATEGORY_NAME = o.TEN_LLCT
WHERE o.SYNCCODE <> 3
  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
  AND c1.TCTK_CODE LIKE 'LLCT%'
ORDER BY dt.GUIDKEY
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
    USING (SELECT (SELECT p.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                   WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                     AND ROWNUM = 1)                              AS PARTY_MEMBER_ID,
                  1                                               AS IS_ACTIVE,
                  dt.GUIDKEY                                      AS V3_QUATRINH_DAOTAO_GUID,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC)  AS FROM_DATE,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'QUOCGIA'
                     AND c.CATEGORY_CODE = dt.MA_NUOC
                     AND ROWNUM = 1)                              AS TRAINING_COUNTRY_ID,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCDAOTAO'
                     AND c.CATEGORY_CODE = dt.MA_HTDTAO
                     AND ROWNUM = 1)                              AS TRAINING_MODE_ID,
                  dt.GHICHU                                       AS NOTE,
                  c1.CATEGORY_ID                                  AS TRAINING_LEVEL_ID,
                  (SELECT c2.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c2
                   WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                     AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                     AND ROWNUM = 1)                              AS TRAINING_TYPE_ID
           FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
                    JOIN CSDLDV_PARTY_MEMBER.ORGLLCT o
                         ON dt.MA_LLCT = o.MA_LLCT
                    JOIN CSDLDV_CATEGORY.CATEGORY c1
                         ON c1.CATEGORY_NAME = o.TEN_LLCT
           WHERE o.SYNCCODE <> 3
             AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
             AND c1.TCTK_CODE LIKE 'LLCT%'
             AND dt.GUIDKEY IN :guids) s
    ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
        AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
        AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
    WHEN MATCHED THEN
        UPDATE
            SET t.PARTY_MEMBER_ID     = s.PARTY_MEMBER_ID,
                t.IS_ACTIVE           = s.IS_ACTIVE,
                t.FROM_DATE           = s.FROM_DATE,
                t.TO_DATE             = s.TO_DATE,
                t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                t.TRAINING_MODE_ID    = s.TRAINING_MODE_ID,
                t.NOTE                = s.NOTE
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
```

---

## Menu 5: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_BANGDT)

### SELECT
```sql
SELECT dt.GUIDKEY
FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
         JOIN CSDLDV_PARTY_MEMBER.ORGDAOTAO o
              ON dt.MA_BANGDT = o.MA_BANGDT
         JOIN CSDLDV_CATEGORY.CATEGORY c1
              ON c1.CATEGORY_NAME = o.TEN_BANGDT
WHERE o.SYNCCODE <> 3
  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
  AND c1.TCTK_CODE LIKE 'CM%'
ORDER BY dt.GUIDKEY
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
    USING (SELECT (SELECT p.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                   WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                     AND ROWNUM = 1)                              AS PARTY_MEMBER_ID,
                  1                                               AS IS_ACTIVE,
                  dt.GUIDKEY                                      AS V3_QUATRINH_DAOTAO_GUID,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC)  AS FROM_DATE,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'QUOCGIA'
                     AND c.CATEGORY_CODE = dt.MA_NUOC
                     AND ROWNUM = 1)                              AS TRAINING_COUNTRY_ID,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCDAOTAO'
                     AND c.CATEGORY_CODE = dt.MA_HTDTAO
                     AND ROWNUM = 1)                              AS TRAINING_MODE_ID,
                  dt.GHICHU                                       AS NOTE,
                  c1.CATEGORY_ID                                  AS TRAINING_LEVEL_ID,
                  (SELECT c2.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c2
                   WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                     AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                     AND ROWNUM = 1)                              AS TRAINING_TYPE_ID
           FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
                    JOIN CSDLDV_PARTY_MEMBER.ORGDAOTAO o
                         ON dt.MA_BANGDT = o.MA_BANGDT
                    JOIN CSDLDV_CATEGORY.CATEGORY c1
                         ON c1.CATEGORY_NAME = o.TEN_BANGDT
           WHERE o.SYNCCODE <> 3
             AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
             AND c1.TCTK_CODE LIKE 'CM%'
             AND dt.GUIDKEY IN :guids) s
    ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
        AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
        AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
    WHEN MATCHED THEN
        UPDATE
            SET t.PARTY_MEMBER_ID     = s.PARTY_MEMBER_ID,
                t.IS_ACTIVE           = s.IS_ACTIVE,
                t.FROM_DATE           = s.FROM_DATE,
                t.TO_DATE             = s.TO_DATE,
                t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                t.TRAINING_MODE_ID    = s.TRAINING_MODE_ID,
                t.NOTE                = s.NOTE
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
```

---

## Menu 6: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_BANGNN)

### SELECT
```sql
SELECT dt.GUIDKEY
FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
         JOIN CSDLDV_PARTY_MEMBER.ORGN_NGU o
              ON dt.MA_BANGNN = o.MA_BANGNN
         JOIN CSDLDV_CATEGORY.CATEGORY c1
              ON c1.CATEGORY_NAME = o.TEN_BANGNN
WHERE o.SYNCCODE <> 3
  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
  AND c1.TCTK_CODE LIKE 'NN%'
ORDER BY dt.GUIDKEY
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS t
    USING (SELECT (SELECT p.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER p
                   WHERE p.V3_SOYEU_ID = dt.SOYEU_ID
                     AND ROWNUM = 1)                              AS PARTY_MEMBER_ID,
                  1                                               AS IS_ACTIVE,
                  dt.GUIDKEY                                      AS V3_QUATRINH_DAOTAO_GUID,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.NHAPHOC)  AS FROM_DATE,
                  CSDLDV_PARTY_MEMBER.STR_2_DDMMYYYY(dt.T_NGHIEP) AS TO_DATE,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'QUOCGIA'
                     AND c.CATEGORY_CODE = dt.MA_NUOC
                     AND ROWNUM = 1)                              AS TRAINING_COUNTRY_ID,
                  (SELECT c.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c
                   WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCDAOTAO'
                     AND c.CATEGORY_CODE = dt.MA_HTDTAO
                     AND ROWNUM = 1)                              AS TRAINING_MODE_ID,
                  dt.GHICHU                                       AS NOTE,
                  c1.CATEGORY_ID                                  AS TRAINING_LEVEL_ID,
                  (SELECT c2.CATEGORY_ID
                   FROM CSDLDV_CATEGORY.CATEGORY c2
                   WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                     AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                     AND c2.TCTK_CODE <> '0'
                     AND ROWNUM = 1)                              AS TRAINING_TYPE_ID
           FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
                    JOIN CSDLDV_PARTY_MEMBER.ORGN_NGU o
                         ON dt.MA_BANGNN = o.MA_BANGNN
                    JOIN CSDLDV_CATEGORY.CATEGORY c1
                         ON c1.CATEGORY_NAME = o.TEN_BANGNN
           WHERE o.SYNCCODE <> 3
             AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
             AND c1.TCTK_CODE LIKE 'NN%'
             AND dt.GUIDKEY IN :guids) s
    ON (t.V3_QUATRINH_DAOTAO_GUID = s.V3_QUATRINH_DAOTAO_GUID
        AND t.TRAINING_TYPE_ID = s.TRAINING_TYPE_ID
        AND t.TRAINING_LEVEL_ID = s.TRAINING_LEVEL_ID)
    WHEN MATCHED THEN
        UPDATE
            SET t.PARTY_MEMBER_ID     = s.PARTY_MEMBER_ID,
                t.IS_ACTIVE           = s.IS_ACTIVE,
                t.FROM_DATE           = s.FROM_DATE,
                t.TO_DATE             = s.TO_DATE,
                t.TRAINING_COUNTRY_ID = s.TRAINING_COUNTRY_ID,
                t.TRAINING_MODE_ID    = s.TRAINING_MODE_ID,
                t.NOTE                = s.NOTE
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
```

---

## Menu 7: FINANCIAL_CONDITION Migration

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE (NVL(s.THUNHAP1, 0) > 0
    OR s.TMP_THUNHAP1 IS NOT NULL
    OR s.MA_HDKT IS NOT NULL)
  AND s.SYNCCODE <> 3
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FINANCIAL_CONDITION t
    USING (SELECT 1                  AS IS_ACTIVE,
                  s.THUNHAP1         AS HOUSEHOLD_ANNUAL_INCOME,
                  s.TMP_THUNHAP1     AS PER_CAPITA_ANNUAL_INCOME,
                  k.TEN_HDKT         AS ECONOMIC_ACTIVITY,
                  s.SOLDTHUE         AS COUNT_WORKER,
                  (SELECT a.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER a
                   WHERE a.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
                    LEFT JOIN CSDLDV_PARTY_MEMBER.orgHDONG_KT k
                              ON s.MA_HDKT = k.MA_HDKT
           WHERE s.SOYEU_ID IN (:soyeuIds)) s
    ON (t.PARTY_MEMBER_ID = s.PARTY_MEMBER_ID)
    WHEN MATCHED THEN
        UPDATE
            SET t.IS_ACTIVE                = s.IS_ACTIVE,
                t.HOUSEHOLD_ANNUAL_INCOME  = s.HOUSEHOLD_ANNUAL_INCOME,
                t.PER_CAPITA_ANNUAL_INCOME = s.PER_CAPITA_ANNUAL_INCOME,
                t.ECONOMIC_ACTIVITY        = s.ECONOMIC_ACTIVITY,
                t.COUNT_WORKER             = s.COUNT_WORKER
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
```

---

## Menu 8: FINANCIAL_CONDITION_LIST Migration (Merge MA_NHADAT, DTNHA)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.DTNHA,
                  s.MA_NHADAT,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 1
            AND tgt.ASSET_GROUP_TYPE = 1
            AND tgt.DETAIL = src.MA_NHADAT
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.DTNHA,
                tgt.IS_ACTIVE    = 1
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
```

---

## Menu 9: FINANCIAL_CONDITION_LIST Migration (Merge NHA2, DTNHA2)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
  AND (s.DTNHA2 IS NOT NULL OR s.NHA2 IS NOT NULL)
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.DTNHA2,
                  s.NHA2,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 1
            AND tgt.ASSET_GROUP_TYPE = 2
            AND tgt.DETAIL = src.NHA2
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.DTNHA2,
                tgt.IS_ACTIVE    = 1
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
```

---

## Menu 10: FINANCIAL_CONDITION_LIST Migration (Merge TMP_DATCAP)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
  AND s.TMP_DATCAP IS NOT NULL
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.TMP_DATCAP,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 2
            AND tgt.ASSET_GROUP_TYPE = 1
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.TMP_DATCAP,
                tgt.IS_ACTIVE    = 1
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
```

---

## Menu 11: FINANCIAL_CONDITION_LIST Migration (Merge DATMUA)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
  AND s.DATMUA IS NOT NULL
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.DATMUA,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 2
            AND tgt.ASSET_GROUP_TYPE = 2
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.DATMUA,
                tgt.IS_ACTIVE    = 1
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
```

---

## Menu 12: FINANCIAL_CONDITION_LIST Migration (Merge DATTT)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
  AND s.DATTT IS NOT NULL
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.DATTT,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 2
            AND tgt.ASSET_GROUP_TYPE = 3
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.DATTT,
                tgt.IS_ACTIVE    = 1
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
```

---

## Menu 13: FINANCIAL_CONDITION_LIST Migration (Merge TSCOGTRI, TSGTRI)

### SELECT
```sql
SELECT s.SOYEU_ID
FROM CSDLDV_PARTY_MEMBER.SOYEU s
WHERE s.SYNCCODE <> 3
  AND (s.TSCOGTRI IS NOT NULL OR s.TSGTRI IS NOT NULL)
ORDER BY s.SOYEU_ID
```

### MERGE
```sql
MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
    USING (SELECT s.SOYEU_ID,
                  s.TSCOGTRI,
                  s.TSGTRI,
                  (SELECT m.PARTY_MEMBER_ID
                   FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                   WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                     AND ROWNUM = 1) AS PARTY_MEMBER_ID
           FROM CSDLDV_PARTY_MEMBER.SOYEU s
           WHERE s.SOYEU_ID IN (:soyeuIds)) src
    ON (
        tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
            AND tgt.ASSET_GROUP = 3
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.ASSETS_VALUE = src.TSGTRI,
                tgt.DETAIL       = src.TSCOGTRI,
                tgt.IS_ACTIVE    = 1
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
```

