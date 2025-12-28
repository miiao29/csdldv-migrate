package com.csdldv.batch;

import com.csdldv.repository.PartyMemberFinancialConditionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class FinancialConditionMigrationService {

    private static final Logger log = LoggerFactory.getLogger(FinancialConditionMigrationService.class);

    private final PartyMemberFinancialConditionRepository partyMemberFinancialConditionRepository;
    private final TransactionTemplate transactionTemplate;

    public FinancialConditionMigrationService(PartyMemberFinancialConditionRepository partyMemberFinancialConditionRepository, TransactionTemplate transactionTemplate) {
        this.partyMemberFinancialConditionRepository = partyMemberFinancialConditionRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public void displaySql() {
        String mergeSql20 = getMergeSqlFrom20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION Records from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSqlFrom20() {
        return """
                MERGE INTO CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FINANCIAL_CONDITION t
                USING (
                    SELECT CSDLDV.PARTY_MEMBER_FINANCIAL_CONDITION_seq.NEXTVAL AS FINANCIAL_CONDITION_ID,
                           1                                                   AS IS_ACTIVE,
                           s.THUNHAP1                                          AS HOUSEHOLD_ANNUAL_INCOME,
                           s.TMP_THUNHAP1                                      AS PER_CAPITA_ANNUAL_INCOME,
                           k.TEN_HDKT                                          AS ECONOMIC_ACTIVITY,
                           s.SOLDTHUE                                          AS COUNT_WORKER,
                           (SELECT a.PARTY_MEMBER_ID
                            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER a
                            WHERE a.V3_SOYEU_ID = s.SOYEU_ID
                              AND ROWNUM = 1)                                  AS PARTY_MEMBER_ID
                    FROM CSDLDV_PARTY_MEMBER.SOYEU s
                             LEFT JOIN CSDLDV_PARTY_MEMBER.orgHDONG_KT k
                                       ON s.MA_HDKT = k.MA_HDKT
                    WHERE (NVL(s.THUNHAP1, 0) > 0
                        OR s.TMP_THUNHAP1 IS NOT NULL
                        OR s.MA_HDKT IS NOT NULL)
                      AND s.SYNCCODE <> 3
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
                    VALUES (s.FINANCIAL_CONDITION_ID,
                            s.IS_ACTIVE,
                            s.HOUSEHOLD_ANNUAL_INCOME,
                            s.PER_CAPITA_ANNUAL_INCOME,
                            s.ECONOMIC_ACTIVITY,
                            s.COUNT_WORKER,
                            s.PARTY_MEMBER_ID)
                """;
    }


    public void mergeFinancialCondition(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION migration with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = partyMemberFinancialConditionRepository.findAllSoyeuIdsFrom20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = partyMemberFinancialConditionRepository.findAllSoyeuIdsFrom25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = partyMemberFinancialConditionRepository.findAllSoyeuIdsFrom26();
        log.info("Found {} SOYEU_IDs from CSDLDV_26 to merge", soyeuIds26.size());

        java.util.List<String> allSoyeuIds = new java.util.ArrayList<>();
        allSoyeuIds.addAll(soyeuIds20);
        allSoyeuIds.addAll(soyeuIds25);
        allSoyeuIds.addAll(soyeuIds26);
        log.info("Found {} total SOYEU_IDs to merge ({} from CSDLDV_20, {} from CSDLDV_25, {} from CSDLDV_26)", allSoyeuIds.size(), soyeuIds20.size(), soyeuIds25.size(), soyeuIds26.size());

        int totalBatches = (int) Math.ceil((double) allSoyeuIds.size() / batchSize);
        int totalMegaBatches = (int) Math.ceil((double) allSoyeuIds.size() / megaBatchSize);
        log.info("Will execute {} MERGE statements in {} mega-batches ({} records per mega-batch), {} COMMITs total", totalBatches, totalMegaBatches, megaBatchSize, totalMegaBatches);

        long totalMerged = 0;
        int committedMegaBatches = 0;

        try {
            for (int megaStart = 0; megaStart < allSoyeuIds.size(); megaStart += megaBatchSize) {
                final int currentMegaStart = megaStart;
                int megaEnd = Math.min(currentMegaStart + megaBatchSize, allSoyeuIds.size());
                int megaBatchNumber = (currentMegaStart / megaBatchSize) + 1;

                log.info("Processing mega-batch {}/{} (records {} to {})", megaBatchNumber, totalMegaBatches, currentMegaStart + 1, megaEnd);

                Integer megaMerged = transactionTemplate.execute(status -> {
                    long merged = 0;

                    for (int i = currentMegaStart; i < megaEnd; i += batchSize) {
                        int endIndex = Math.min(i + batchSize, megaEnd);
                        java.util.List<String> batchSoyeuIds = allSoyeuIds.subList(i, endIndex);

                        java.util.List<String> batchSoyeuIds20 = new java.util.ArrayList<>();
                        java.util.List<String> batchSoyeuIds25 = new java.util.ArrayList<>();
                        java.util.List<String> batchSoyeuIds26 = new java.util.ArrayList<>();

                        for (String soyeuId : batchSoyeuIds) {
                            if (soyeuIds20.contains(soyeuId)) {
                                batchSoyeuIds20.add(soyeuId);
                            } else if (soyeuIds25.contains(soyeuId)) {
                                batchSoyeuIds25.add(soyeuId);
                            } else if (soyeuIds26.contains(soyeuId)) {
                                batchSoyeuIds26.add(soyeuId);
                            }
                        }

                        if (!batchSoyeuIds20.isEmpty()) {
                            Integer batchMerged20 = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom26(batchSoyeuIds26);
                            merged += batchMerged26;
                        }
                    }

                    return (int) merged;
                });

                if (megaMerged == null) {
                    log.error("Mega-batch {}/{} transaction returned null, rolling back this mega-batch", megaBatchNumber, totalMegaBatches);
                    throw new RuntimeException("Mega-batch " + megaBatchNumber + " failed, rolled back");
                }

                totalMerged += megaMerged;
                committedMegaBatches++;
                log.info("Mega-batch {}/{} committed successfully ({} records merged, total: {})", megaBatchNumber, totalMegaBatches, megaMerged, totalMerged);
            }

            log.info("=============================================");
            log.info("Finished FINANCIAL_CONDITION migration, total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION migration encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }
}

