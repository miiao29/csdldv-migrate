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

    public void deleteInvalidAndDuplicateRecords() {
        log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Delete invalid or duplicate records");

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
}

