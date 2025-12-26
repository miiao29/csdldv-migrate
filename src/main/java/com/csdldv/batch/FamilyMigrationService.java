package com.csdldv.batch;

import com.csdldv.repository.PartyMemberFamilyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.List;

@Service
public class FamilyMigrationService {

    private static final Logger log = LoggerFactory.getLogger(FamilyMigrationService.class);

    private final PartyMemberFamilyRepository partyMemberFamilyRepository;
    private final TransactionTemplate transactionTemplate;

    public FamilyMigrationService(PartyMemberFamilyRepository partyMemberFamilyRepository, TransactionTemplate transactionTemplate) {
        this.partyMemberFamilyRepository = partyMemberFamilyRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public void displaySql() {
        String selectSql = getSelectSql();
        String updateTypeOneSql = getUpdateTypeOneSql();
        String updateTypeZeroSql = getUpdateTypeZeroSql();

        System.out.println("\n=== SQL SELECT ===");
        System.out.println(selectSql);
        System.out.println("\n=== SQL UPDATE (Set TYPE = 1) ===");
        System.out.println(updateTypeOneSql);
        System.out.println("\n=== SQL UPDATE (Set TYPE = 0) ===");
        System.out.println(updateTypeZeroSql);
        System.out.println();
    }

    private String getSelectSql() {
        return """
                SELECT a.PARTY_MEMBER_FAMILY_ID
                FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY a
                WHERE a.TYPE IS NULL
                  AND EXISTS (
                      SELECT 1
                      FROM CSDLDV_20.qhe_gd b
                      WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                        AND b.MA_TV_GD IN (
                            'A2','A3','B5','B6','C7','C8',
                            'E2','E3','M2','M3','R1','R2'
                        )
                  )
                   OR EXISTS (
                       SELECT 1
                       FROM CSDLDV_25.qhe_gd b
                       WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                         AND b.MA_TV_GD IN (
                             'A2','A3','B5','B6','C7','C8',
                             'E2','E3','M2','M3','R1','R2'
                         )
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM CSDLDV_26.qhe_gd b
                       WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                         AND b.MA_TV_GD IN (
                             'A2','A3','B5','B6','C7','C8',
                             'E2','E3','M2','M3','R1','R2'
                         )
                   )
                ORDER BY a.PARTY_MEMBER_FAMILY_ID
                """;
    }

    private String getUpdateTypeOneSql() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
                SET TYPE = 1
                WHERE PARTY_MEMBER_FAMILY_ID IN (:ids)
                """;
    }

    private String getUpdateTypeZeroSql() {
        return """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
                SET TYPE = 0
                WHERE TYPE IS NULL
                """;
    }

    public void migrateFamilyTypeInBatches(int batchSize, int megaBatchSize) {
        log.info("Starting family type migration with batch size {}", batchSize);

        String selectSql = getSelectSql();
        log.info("SQL SELECT to find IDs:\n{}", selectSql);

        List<String> allIds = partyMemberFamilyRepository.findAllIdsForTypeOne();
        log.info("Found {} total records to update", allIds.size());

        int totalBatches = (int) Math.ceil((double) allIds.size() / batchSize);
        int totalMegaBatches = (int) Math.ceil((double) allIds.size() / megaBatchSize);
        log.info("Will execute {} UPDATE statements in {} mega-batches ({} records per mega-batch), {} COMMITs total", totalBatches, totalMegaBatches, megaBatchSize, totalMegaBatches);

        String updateTypeOneSqlTemplate = getUpdateTypeOneSql();
        log.info("SQL UPDATE template to set TYPE = 1:\n{}", updateTypeOneSqlTemplate);

        long totalUpdated = 0;
        int committedMegaBatches = 0;

        try {
            for (int megaStart = 0; megaStart < allIds.size(); megaStart += megaBatchSize) {
                final int currentMegaStart = megaStart;
                int megaEnd = Math.min(currentMegaStart + megaBatchSize, allIds.size());
                int megaBatchNumber = (currentMegaStart / megaBatchSize) + 1;

                log.info("Processing mega-batch {}/{} (records {} to {})", megaBatchNumber, totalMegaBatches, currentMegaStart + 1, megaEnd);

                Integer[] result = transactionTemplate.execute(status -> {
                    long megaUpdated = 0;

                    for (int i = currentMegaStart; i < megaEnd; i += batchSize) {
                        int endIndex = Math.min(i + batchSize, megaEnd);
                        List<String> batchIds = allIds.subList(i, endIndex);

                        Integer updated = partyMemberFamilyRepository.bulkSetTypeOne(batchIds);
                        megaUpdated += updated;
                    }

                    return new Integer[]{(int) megaUpdated};
                });

                if (result == null || result[0] == null) {
                    log.error("Mega-batch {}/{} transaction returned null, rolling back this mega-batch", megaBatchNumber, totalMegaBatches);
                    throw new RuntimeException("Mega-batch " + megaBatchNumber + " failed, rolled back");
                }

                totalUpdated += result[0];
                committedMegaBatches++;
                log.info("Mega-batch {}/{} committed successfully ({} records updated, total: {})", megaBatchNumber, totalMegaBatches, result[0], totalUpdated);
            }

            Integer setZero = transactionTemplate.execute(status -> partyMemberFamilyRepository.bulkSetTypeZero());

            log.info("=============================================");
            log.info("Finished family type migration, set type=1 {}, set type=0 {}", totalUpdated, setZero);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("Family type migration encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }
}

