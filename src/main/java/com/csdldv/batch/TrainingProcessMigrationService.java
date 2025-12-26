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
        String insertSql = getInsertMaLLCTSql();

        System.out.println("\n=== SQL INSERT MA_LLCT Records ===");
        System.out.println(insertSql);
        System.out.println();
    }

    private String getInsertMaLLCTSql() {
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
                FROM CSDLDV_PARTY_MEMBER.QTRINH_DT dt
                         JOIN CSDLDV_PARTY_MEMBER.ORGLLCT o
                              ON dt.MA_LLCT = o.MA_LLCT
                         JOIN CSDLDV_CATEGORY.CATEGORY c1
                              ON c1.CATEGORY_NAME = o.TEN_LLCT
                WHERE o.SYNCCODE <> 3
                  AND c1.CATEGORY_GROUP_CODE = 'TRINHDODAOTAO'
                  AND c1.TCTK_CODE LIKE 'LLCT%'
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

        Integer inserted = transactionTemplate.execute(status -> {
            try {
                int count = partyMemberTrainingProcessRepository.insertMaLLCTRecords();
                log.info("Inserted {} MA_LLCT records into PARTY_MEMBER_TRAINING_PROCESS", count);
                return count;
            } catch (Exception ex) {
                log.error("Error inserting MA_LLCT records into PARTY_MEMBER_TRAINING_PROCESS: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (inserted == null) {
            log.error("Insert MA_LLCT records transaction returned null, rolling back");
            throw new RuntimeException("Insert MA_LLCT records failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished PARTY_MEMBER_TRAINING_PROCESS migration - Chức năng 3.1");
        log.info("Total records inserted: {}", inserted);
        log.info("=============================================");
    }
}

