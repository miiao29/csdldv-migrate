package com.csdldv.batch;

import com.csdldv.repository.PartyMemberDisciplineRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.List;

@Service
public class DisciplineMigrationService {

    private static final Logger log = LoggerFactory.getLogger(DisciplineMigrationService.class);

    private final PartyMemberDisciplineRepository partyMemberDisciplineRepository;
    private final TransactionTemplate transactionTemplate;

    public DisciplineMigrationService(PartyMemberDisciplineRepository partyMemberDisciplineRepository, TransactionTemplate transactionTemplate) {
        this.partyMemberDisciplineRepository = partyMemberDisciplineRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public void displaySql() {
        String selectSql = getSelectSql();
        String updateSql = getUpdateSql();

        System.out.println("\n=== SQL SELECT ===");
        System.out.println(selectSql);
        System.out.println("\n=== SQL UPDATE ===");
        System.out.println(updateSql);
        System.out.println();
    }

    private String getSelectSql() {
        return """
                SELECT x.PARTY_MEMBER_DISCIPLINE_ID
                FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                WHERE EXISTS (
                    SELECT 1
                    FROM CSDLDV_20.KT_KL kl
                    JOIN CSDLDV_CATEGORY.CATEGORY c
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                OR EXISTS (
                    SELECT 1
                    FROM CSDLDV_25.KT_KL kl
                    JOIN CSDLDV_CATEGORY.CATEGORY c
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                OR EXISTS (
                    SELECT 1
                    FROM CSDLDV_26.KT_KL kl
                    JOIN CSDLDV_CATEGORY.CATEGORY c
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
                """;
    }

    private String getUpdateSql() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
                (
                    SELECT s.CATEGORY_ID, s.LYDO
                    FROM (
                        SELECT c.CATEGORY_ID,
                               kl.GUIDKEY,
                               kl.LYDO
                        FROM CSDLDV_CATEGORY.CATEGORY c
                        JOIN CSDLDV_20.KT_KL kl
                          ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                        WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                        UNION ALL
                        SELECT c.CATEGORY_ID,
                               kl.GUIDKEY,
                               kl.LYDO
                        FROM CSDLDV_CATEGORY.CATEGORY c
                        JOIN CSDLDV_25.KT_KL kl
                          ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                        WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                        UNION ALL
                        SELECT c.CATEGORY_ID,
                               kl.GUIDKEY,
                               kl.LYDO
                        FROM CSDLDV_CATEGORY.CATEGORY c
                        JOIN CSDLDV_26.KT_KL kl
                          ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                        WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                    ) s
                    WHERE s.GUIDKEY = x.V3_KT_KL_GUID
                )
                WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN (:ids)
                """;
    }

    public void updateDisciplineFormAndReasonInBatches(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_DISCIPLINE migration (MA_KL, LYDO) with batch size {}", batchSize);

        String selectSql = getSelectSql();
        log.info("SQL SELECT to find IDs:\n{}", selectSql);

        List<String> allIds = partyMemberDisciplineRepository.findAllIdsForUpdate();
        log.info("Found {} total records to update", allIds.size());

        int totalBatches = (int) Math.ceil((double) allIds.size() / batchSize);
        int totalMegaBatches = (int) Math.ceil((double) allIds.size() / megaBatchSize);
        log.info("Will execute {} UPDATE statements in {} mega-batches ({} records per mega-batch), {} COMMITs total", totalBatches, totalMegaBatches, megaBatchSize, totalMegaBatches);

        long totalUpdated = 0;
        int committedMegaBatches = 0;

        try {
            for (int megaStart = 0; megaStart < allIds.size(); megaStart += megaBatchSize) {
                final int currentMegaStart = megaStart;
                int megaEnd = Math.min(currentMegaStart + megaBatchSize, allIds.size());
                int megaBatchNumber = (currentMegaStart / megaBatchSize) + 1;

                log.info("Processing mega-batch {}/{} (records {} to {})", megaBatchNumber, totalMegaBatches, currentMegaStart + 1, megaEnd);

                Integer megaUpdated = transactionTemplate.execute(status -> {
                    long updated = 0;

                    for (int i = currentMegaStart; i < megaEnd; i += batchSize) {
                        int endIndex = Math.min(i + batchSize, megaEnd);
                        List<String> batchIds = allIds.subList(i, endIndex);

                        Integer batchUpdated = partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReason(batchIds);
                        updated += batchUpdated;
                    }

                    return (int) updated;
                });

                if (megaUpdated == null) {
                    log.error("Mega-batch {}/{} transaction returned null, rolling back this mega-batch", megaBatchNumber, totalMegaBatches);
                    throw new RuntimeException("Mega-batch " + megaBatchNumber + " failed, rolled back");
                }

                totalUpdated += megaUpdated;
                committedMegaBatches++;
                log.info("Mega-batch {}/{} committed successfully ({} records updated, total: {})", megaBatchNumber, totalMegaBatches, megaUpdated, totalUpdated);
            }

            log.info("=============================================");
            log.info("Finished PARTY_MEMBER_DISCIPLINE migration, total updated {} rows", totalUpdated);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("PARTY_MEMBER_DISCIPLINE migration encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }
}

