package com.csdldv.batch;

import com.csdldv.repository.FinancialConditionListRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class FinancialConditionListMigrationService {

    private static final Logger log = LoggerFactory.getLogger(FinancialConditionListMigrationService.class);

    private final FinancialConditionListRepository financialConditionListRepository;
    private final TransactionTemplate transactionTemplate;

    public FinancialConditionListMigrationService(FinancialConditionListRepository financialConditionListRepository, TransactionTemplate transactionTemplate) {
        this.financialConditionListRepository = financialConditionListRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public void displaySql() {
        String mergeSql20 = getMergeSqlFrom20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSqlFrom20() {
        return """
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
                    WHERE s.SYNCCODE <> 3
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 1
                        AND tgt.ASSET_GROUP_TYPE = 1
                        AND tgt.DETAIL = src.MA_NHADAT
                )
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
                """;
    }


    public void mergeFinancialConditionList(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsFrom20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsFrom25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsFrom26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionListFrom20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionListFrom25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionListFrom26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration, total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }

    public void displaySqlFor9() {
        String mergeSql20 = getMergeSql2From20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records (Menu 9) from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSql2From20() {
        return """
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
                    WHERE s.SYNCCODE <> 3
                      AND (s.DTNHA2 IS NOT NULL OR s.NHA2 IS NOT NULL)
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 1
                        AND tgt.ASSET_GROUP_TYPE = 2
                        AND tgt.DETAIL = src.NHA2
                )
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
                """;
    }


    public void mergeFinancialConditionList2(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 9) with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsForMenu9From20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsForMenu9From25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsForMenu9From26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionList2From20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionList2From25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionList2From26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration (Menu 9), total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration (Menu 9) encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }

    public void displaySqlFor10() {
        String mergeSql20 = getMergeSql3From20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records (Menu 10) from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSql3From20() {
        return """
                MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
                USING (
                    SELECT s.SOYEU_ID,
                           s.TMP_DATCAP,
                           (SELECT m.PARTY_MEMBER_ID
                            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                            WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                              AND ROWNUM = 1) AS PARTY_MEMBER_ID
                    FROM CSDLDV_20.SOYEU s
                    WHERE s.SYNCCODE <> 3
                      AND s.TMP_DATCAP IS NOT NULL
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 2
                        AND tgt.ASSET_GROUP_TYPE = 1
                )
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
                """;
    }


    public void mergeFinancialConditionList3(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 10) with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsForMenu10From20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsForMenu10From25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsForMenu10From26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionList3From20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionList3From25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionList3From26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration (Menu 10), total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration (Menu 10) encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }

    public void displaySqlFor11() {
        String mergeSql20 = getMergeSql4From20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records (Menu 11) from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSql4From20() {
        return """
                MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
                USING (
                    SELECT s.SOYEU_ID,
                           s.DATMUA,
                           (SELECT m.PARTY_MEMBER_ID
                            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                            WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                              AND ROWNUM = 1) AS PARTY_MEMBER_ID
                    FROM CSDLDV_20.SOYEU s
                    WHERE s.SYNCCODE <> 3
                      AND s.DATMUA IS NOT NULL
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 2
                        AND tgt.ASSET_GROUP_TYPE = 2
                )
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
                """;
    }


    public void mergeFinancialConditionList4(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 11) with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsForMenu11From20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsForMenu11From25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsForMenu11From26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionList4From20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionList4From25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionList4From26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration (Menu 11), total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration (Menu 11) encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }

    public void displaySqlFor12() {
        String mergeSql20 = getMergeSql5From20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records (Menu 12) from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSql5From20() {
        return """
                MERGE INTO CSDLDV_PARTY_MEMBER.FINANCIAL_CONDITION_LIST tgt
                USING (
                    SELECT s.SOYEU_ID,
                           s.DATTT,
                           (SELECT m.PARTY_MEMBER_ID
                            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER m
                            WHERE m.V3_SOYEU_ID = s.SOYEU_ID
                              AND ROWNUM = 1) AS PARTY_MEMBER_ID
                    FROM CSDLDV_20.SOYEU s
                    WHERE s.SYNCCODE <> 3
                      AND s.DATTT IS NOT NULL
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 2
                        AND tgt.ASSET_GROUP_TYPE = 3
                )
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
                """;
    }


    public void mergeFinancialConditionList5(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 12) with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsForMenu12From20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsForMenu12From25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsForMenu12From26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionList5From20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionList5From25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionList5From26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration (Menu 12), total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration (Menu 12) encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }

    public void displaySqlFor13() {
        String mergeSql20 = getMergeSql6From20();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION_LIST Records (Menu 13) from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println();
    }

    private String getMergeSql6From20() {
        return """
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
                    WHERE s.SYNCCODE <> 3
                      AND (s.TSCOGTRI IS NOT NULL OR s.TSGTRI IS NOT NULL)
                ) src
                ON (
                    tgt.PARTY_MEMBER_ID = src.PARTY_MEMBER_ID
                        AND tgt.ASSET_GROUP = 3
                )
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
                """;
    }


    public void mergeFinancialConditionList6(int batchSize, int megaBatchSize) {
        log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 13) with batch size {}, mega-batch size {}", batchSize, megaBatchSize);

        log.info("Starting find SOYEU_IDs from CSDLDV_20");
        java.util.List<String> soyeuIds20 = financialConditionListRepository.findAllSoyeuIdsForMenu13From20();
        log.info("Found {} SOYEU_IDs from CSDLDV_20 to merge", soyeuIds20.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_25");
        java.util.List<String> soyeuIds25 = financialConditionListRepository.findAllSoyeuIdsForMenu13From25();
        log.info("Found {} SOYEU_IDs from CSDLDV_25 to merge", soyeuIds25.size());

        log.info("Starting find SOYEU_IDs from CSDLDV_26");
        java.util.List<String> soyeuIds26 = financialConditionListRepository.findAllSoyeuIdsForMenu13From26();
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
                            Integer batchMerged20 = financialConditionListRepository.mergeFinancialConditionList6From20(batchSoyeuIds20);
                            merged += batchMerged20;
                        }

                        if (!batchSoyeuIds25.isEmpty()) {
                            Integer batchMerged25 = financialConditionListRepository.mergeFinancialConditionList6From25(batchSoyeuIds25);
                            merged += batchMerged25;
                        }

                        if (!batchSoyeuIds26.isEmpty()) {
                            Integer batchMerged26 = financialConditionListRepository.mergeFinancialConditionList6From26(batchSoyeuIds26);
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
            log.info("Finished FINANCIAL_CONDITION_LIST migration (Menu 13), total merged {} rows", totalMerged);
            log.info("Total {} mega-batches committed successfully ({} COMMITs)", committedMegaBatches, committedMegaBatches);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("FINANCIAL_CONDITION_LIST migration (Menu 13) encountered error at mega-batch {}/{}, last {} mega-batches rolled back: {}", committedMegaBatches + 1, totalMegaBatches, committedMegaBatches, ex.getMessage(), ex);

            throw ex;
        }
    }
}

