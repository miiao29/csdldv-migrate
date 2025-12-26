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
        String selectSql20 = getSelectSqlFrom20();
        String selectSql25 = getSelectSqlFrom25();
        String selectSql26 = getSelectSqlFrom26();
        String updateSql20 = getUpdateSqlFrom20();
        String updateSql25 = getUpdateSqlFrom25();
        String updateSql26 = getUpdateSqlFrom26();

        System.out.println("\n=== SQL SELECT from CSDLDV_20 ===");
        System.out.println(selectSql20);
        System.out.println("\n=== SQL SELECT from CSDLDV_25 ===");
        System.out.println(selectSql25);
        System.out.println("\n=== SQL SELECT from CSDLDV_26 ===");
        System.out.println(selectSql26);
        System.out.println("\n=== SQL UPDATE from CSDLDV_20 ===");
        System.out.println(updateSql20);
        System.out.println("\n=== SQL UPDATE from CSDLDV_25 ===");
        System.out.println(updateSql25);
        System.out.println("\n=== SQL UPDATE from CSDLDV_26 ===");
        System.out.println(updateSql26);
        System.out.println();
    }

    private String getSelectSqlFrom20() {
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
                ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
                """;
    }

    private String getSelectSqlFrom25() {
        return """
                SELECT x.PARTY_MEMBER_DISCIPLINE_ID
                FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                WHERE EXISTS (
                    SELECT 1
                    FROM CSDLDV_25.KT_KL kl
                    JOIN CSDLDV_CATEGORY.CATEGORY c
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
                """;
    }

    private String getSelectSqlFrom26() {
        return """
                SELECT x.PARTY_MEMBER_DISCIPLINE_ID
                FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                WHERE EXISTS (
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

    private String getUpdateSqlFrom20() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
                (
                    SELECT c.CATEGORY_ID, kl.LYDO
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    JOIN CSDLDV_20.KT_KL kl
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN (:ids)
                """;
    }

    private String getUpdateSqlFrom25() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
                (
                    SELECT c.CATEGORY_ID, kl.LYDO
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    JOIN CSDLDV_25.KT_KL kl
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN (:ids)
                """;
    }

    private String getUpdateSqlFrom26() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
                SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
                (
                    SELECT c.CATEGORY_ID, kl.LYDO
                    FROM CSDLDV_CATEGORY.CATEGORY c
                    JOIN CSDLDV_26.KT_KL kl
                      ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                    WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                      AND kl.GUIDKEY = x.V3_KT_KL_GUID
                )
                WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN (:ids)
                """;
    }

    public void updateDisciplineFormAndReasonInBatches(int batchSize, int megaBatchSize) {
        log.info("Starting PARTY_MEMBER_DISCIPLINE migration (MA_KL, LYDO) with batch size {}", batchSize);

        log.info("Starting find IDs from CSDLDV_20");
        List<String> ids20 = partyMemberDisciplineRepository.findAllIdsForUpdateFrom20();
        log.info("Found {} records from CSDLDV_20 to update", ids20.size());

        log.info("Starting find IDs from CSDLDV_25");
        List<String> ids25 = partyMemberDisciplineRepository.findAllIdsForUpdateFrom25();
        log.info("Found {} records from CSDLDV_25 to update", ids25.size());

        log.info("Starting find IDs from CSDLDV_26");
        List<String> ids26 = partyMemberDisciplineRepository.findAllIdsForUpdateFrom26();
        log.info("Found {} records from CSDLDV_26 to update", ids26.size());

        List<String> allIds = new java.util.ArrayList<>();
        allIds.addAll(ids20);
        allIds.addAll(ids25);
        allIds.addAll(ids26);
        log.info("Found {} total records to update ({} from CSDLDV_20, {} from CSDLDV_25, {} from CSDLDV_26)", allIds.size(), ids20.size(), ids25.size(), ids26.size());

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

                        List<String> batchIds20 = new java.util.ArrayList<>();
                        List<String> batchIds25 = new java.util.ArrayList<>();
                        List<String> batchIds26 = new java.util.ArrayList<>();

                        for (String id : batchIds) {
                            if (ids20.contains(id)) {
                                batchIds20.add(id);
                            } else if (ids25.contains(id)) {
                                batchIds25.add(id);
                            } else if (ids26.contains(id)) {
                                batchIds26.add(id);
                            }
                        }

                        if (!batchIds20.isEmpty()) {
                            Integer batchUpdated20 = partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReasonFrom20(batchIds20);
                            updated += batchUpdated20;
                        }

                        if (!batchIds25.isEmpty()) {
                            Integer batchUpdated25 = partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReasonFrom25(batchIds25);
                            updated += batchUpdated25;
                        }

                        if (!batchIds26.isEmpty()) {
                            Integer batchUpdated26 = partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReasonFrom26(batchIds26);
                            updated += batchUpdated26;
                        }
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

