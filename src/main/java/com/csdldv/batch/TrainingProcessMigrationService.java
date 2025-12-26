package com.csdldv.batch;

import com.csdldv.repository.PartyMemberTrainingProcessRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class TrainingProcessMigrationService {

    private static final Logger log = LoggerFactory.getLogger(TrainingProcessMigrationService.class);

    private final PartyMemberTrainingProcessRepository partyMemberTrainingProcessRepository;
    private final TransactionTemplate transactionTemplate;

    public TrainingProcessMigrationService(PartyMemberTrainingProcessRepository partyMemberTrainingProcessRepository, TransactionTemplate transactionTemplate) {
        this.partyMemberTrainingProcessRepository = partyMemberTrainingProcessRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public void displaySql() {
        String deleteInvalidSql = getDeleteInvalidSql();
        String deleteDuplicateSql = getDeleteDuplicateSql();

        System.out.println("\n=== SQL DELETE Invalid Records ===");
        System.out.println(deleteInvalidSql);
        System.out.println("\n=== SQL DELETE Duplicate Records ===");
        System.out.println(deleteDuplicateSql);
        System.out.println();
    }

    public void displaySqlFor31() {
        String insertSql20 = getInsertMaLLCTSqlFrom20();
        String insertSql25 = getInsertMaLLCTSqlFrom25();
        String insertSql26 = getInsertMaLLCTSqlFrom26();

        System.out.println("\n=== SQL INSERT MA_LLCT Records from CSDLDV_20 ===");
        System.out.println(insertSql20);
        System.out.println("\n=== SQL INSERT MA_LLCT Records from CSDLDV_25 ===");
        System.out.println(insertSql25);
        System.out.println("\n=== SQL INSERT MA_LLCT Records from CSDLDV_26 ===");
        System.out.println(insertSql26);
        System.out.println();
    }

    private String getInsertMaLLCTSqlFrom20() {
        return """
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
                FROM CSDLDV_20.QTRINH_DT dt
                         JOIN CSDLDV_20.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaLLCTSqlFrom25() {
        return """
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
                FROM CSDLDV_25.QTRINH_DT dt
                         JOIN CSDLDV_25.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaLLCTSqlFrom26() {
        return """
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
                FROM CSDLDV_26.QTRINH_DT dt
                         JOIN CSDLDV_26.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo lý luận chính trị'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getDeleteInvalidSql() {
        return """
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
                """;
    }

    private String getDeleteDuplicateSql() {
        return """
                DELETE FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS
                WHERE ROWID NOT IN (
                    SELECT MIN(ROWID)
                    FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS
                    GROUP BY
                        TRAINING_TYPE_ID,
                        TRAINING_LEVEL_ID,
                        V3_QUATRINH_DAOTAO_GUID
                )
                """;
    }

    public void deleteInvalidAndDuplicateRecords(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Delete invalid or duplicate records with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        Integer deletedInvalid = transactionTemplate.execute(status -> {
            try {
                int count = partyMemberTrainingProcessRepository.deleteInvalidRecords();
                log.info("Deleted {} invalid records from PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error deleting invalid records from PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (deletedInvalid == null) {
            log.error("Delete invalid records transaction returned null, rolling back");
            throw new RuntimeException("Delete invalid records failed, rolled back");
        }

        Integer deletedDuplicate = transactionTemplate.execute(status -> {
            try {
                int count = partyMemberTrainingProcessRepository.deleteDuplicateRecords();
                log.info("Deleted {} duplicate records from PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error deleting duplicate records from PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (deletedDuplicate == null) {
            log.error("Delete duplicate records transaction returned null, rolling back");
            throw new RuntimeException("Delete duplicate records failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished PARTY_MEMBER_TRAINING_PROCESS migration");
        log.info("Total invalid records deleted: {}", deletedInvalid);
        log.info("Total duplicate records deleted: {}", deletedDuplicate);
        log.info("Total records deleted: {}", deletedInvalid + deletedDuplicate);
        log.info("=============================================");
    }

    public void insertMaLLCTRecords(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.1: Insert MA_LLCT records with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        Integer inserted20 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_LLCT records from CSDLDV_20");
                int count = partyMemberTrainingProcessRepository.insertMaLLCTRecordsFrom20();
                log.info("Inserted {} MA_LLCT records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_LLCT records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted20 == null) {
            log.error("Insert MA_LLCT records from CSDLDV_20 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_LLCT records from CSDLDV_20 failed, rolled back");
        }

        Integer inserted25 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_LLCT records from CSDLDV_25");
                int count = partyMemberTrainingProcessRepository.insertMaLLCTRecordsFrom25();
                log.info("Inserted {} MA_LLCT records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_LLCT records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted25 == null) {
            log.error("Insert MA_LLCT records from CSDLDV_25 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_LLCT records from CSDLDV_25 failed, rolled back");
        }

        Integer inserted26 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_LLCT records from CSDLDV_26");
                int count = partyMemberTrainingProcessRepository.insertMaLLCTRecordsFrom26();
                log.info("Inserted {} MA_LLCT records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_LLCT records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted26 == null) {
            log.error("Insert MA_LLCT records from CSDLDV_26 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_LLCT records from CSDLDV_26 failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.1");
        log.info("Total records inserted from CSDLDV_20: {}", inserted20);
        log.info("Total records inserted from CSDLDV_25: {}", inserted25);
        log.info("Total records inserted from CSDLDV_26: {}", inserted26);
        log.info("Total records inserted: {}", inserted20 + inserted25 + inserted26);
        log.info("=============================================");
    }

    public void displaySqlFor33() {
        String insertSql20 = getInsertMaBANGDTSqlFrom20();
        String insertSql25 = getInsertMaBANGDTSqlFrom25();
        String insertSql26 = getInsertMaBANGDTSqlFrom26();

        System.out.println("\n=== SQL INSERT MA_BANGDT Records from CSDLDV_20 ===");
        System.out.println(insertSql20);
        System.out.println("\n=== SQL INSERT MA_BANGDT Records from CSDLDV_25 ===");
        System.out.println(insertSql25);
        System.out.println("\n=== SQL INSERT MA_BANGDT Records from CSDLDV_26 ===");
        System.out.println(insertSql26);
        System.out.println();
    }

    private String getInsertMaBANGDTSqlFrom20() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaBANGDTSqlFrom25() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaBANGDTSqlFrom26() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo chuyên môn'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    public void insertMaBANGDTRecords(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.3: Insert MA_BANGDT records with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        Integer inserted20 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGDT records from CSDLDV_20");
                int count = partyMemberTrainingProcessRepository.insertMaBANGDTRecordsFrom20();
                log.info("Inserted {} MA_BANGDT records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGDT records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted20 == null) {
            log.error("Insert MA_BANGDT records from CSDLDV_20 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGDT records from CSDLDV_20 failed, rolled back");
        }

        Integer inserted25 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGDT records from CSDLDV_25");
                int count = partyMemberTrainingProcessRepository.insertMaBANGDTRecordsFrom25();
                log.info("Inserted {} MA_BANGDT records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGDT records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted25 == null) {
            log.error("Insert MA_BANGDT records from CSDLDV_25 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGDT records from CSDLDV_25 failed, rolled back");
        }

        Integer inserted26 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGDT records from CSDLDV_26");
                int count = partyMemberTrainingProcessRepository.insertMaBANGDTRecordsFrom26();
                log.info("Inserted {} MA_BANGDT records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGDT records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted26 == null) {
            log.error("Insert MA_BANGDT records from CSDLDV_26 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGDT records from CSDLDV_26 failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.3");
        log.info("Total records inserted from CSDLDV_20: {}", inserted20);
        log.info("Total records inserted from CSDLDV_25: {}", inserted25);
        log.info("Total records inserted from CSDLDV_26: {}", inserted26);
        log.info("Total records inserted: {}", inserted20 + inserted25 + inserted26);
        log.info("=============================================");
    }

    public void displaySqlFor34() {
        String insertSql20 = getInsertMaBANGNNSqlFrom20();
        String insertSql25 = getInsertMaBANGNNSqlFrom25();
        String insertSql26 = getInsertMaBANGNNSqlFrom26();

        System.out.println("\n=== SQL INSERT MA_BANGNN Records from CSDLDV_20 ===");
        System.out.println(insertSql20);
        System.out.println("\n=== SQL INSERT MA_BANGNN Records from CSDLDV_25 ===");
        System.out.println(insertSql25);
        System.out.println("\n=== SQL INSERT MA_BANGNN Records from CSDLDV_26 ===");
        System.out.println(insertSql26);
        System.out.println();
    }

    private String getInsertMaBANGNNSqlFrom20() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                              AND c2.TCTK_CODE <> '0'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaBANGNNSqlFrom25() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                              AND c2.TCTK_CODE <> '0'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    private String getInsertMaBANGNNSqlFrom26() {
        return """
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_TRAINING_PROCESS tp
                      WHERE tp.V3_QUATRINH_DAOTAO_GUID = dt.GUIDKEY
                        AND tp.TRAINING_TYPE_ID = (
                            SELECT c2.CATEGORY_ID
                            FROM CSDLDV_CATEGORY.CATEGORY c2
                            WHERE c2.CATEGORY_GROUP_CODE = 'PHANLOAIDAOTAO'
                              AND c2.CATEGORY_NAME = 'Đào tạo ngoại ngữ'
                              AND c2.TCTK_CODE <> '0'
                              AND ROWNUM = 1
                        )
                        AND tp.TRAINING_LEVEL_ID = c1.CATEGORY_ID
                  )
                """;
    }

    public void insertMaBANGNNRecords(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.4: Insert MA_BANGNN records with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        Integer inserted20 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGNN records from CSDLDV_20");
                int count = partyMemberTrainingProcessRepository.insertMaBANGNNRecordsFrom20();
                log.info("Inserted {} MA_BANGNN records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGNN records from CSDLDV_20 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted20 == null) {
            log.error("Insert MA_BANGNN records from CSDLDV_20 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGNN records from CSDLDV_20 failed, rolled back");
        }

        Integer inserted25 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGNN records from CSDLDV_25");
                int count = partyMemberTrainingProcessRepository.insertMaBANGNNRecordsFrom25();
                log.info("Inserted {} MA_BANGNN records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGNN records from CSDLDV_25 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted25 == null) {
            log.error("Insert MA_BANGNN records from CSDLDV_25 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGNN records from CSDLDV_25 failed, rolled back");
        }

        Integer inserted26 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting insert MA_BANGNN records from CSDLDV_26");
                int count = partyMemberTrainingProcessRepository.insertMaBANGNNRecordsFrom26();
                log.info("Inserted {} MA_BANGNN records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_BANGNN records from CSDLDV_26 into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted26 == null) {
            log.error("Insert MA_BANGNN records from CSDLDV_26 transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_BANGNN records from CSDLDV_26 failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.4");
        log.info("Total records inserted from CSDLDV_20: {}", inserted20);
        log.info("Total records inserted from CSDLDV_25: {}", inserted25);
        log.info("Total records inserted from CSDLDV_26: {}", inserted26);
        log.info("Total records inserted: {}", inserted20 + inserted25 + inserted26);
        log.info("=============================================");
    }
}

