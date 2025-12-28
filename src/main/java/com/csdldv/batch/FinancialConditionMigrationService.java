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
        String mergeSql25 = getMergeSqlFrom25();
        String mergeSql26 = getMergeSqlFrom26();

        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION Records from CSDLDV_20 ===");
        System.out.println(mergeSql20);
        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION Records from CSDLDV_25 ===");
        System.out.println(mergeSql25);
        System.out.println("\n=== SQL MERGE FINANCIAL_CONDITION Records from CSDLDV_26 ===");
        System.out.println(mergeSql26);
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
                    FROM CSDLDV_20.SOYEU s
                             LEFT JOIN CSDLDV_20.orgHDONG_KT k
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

    private String getMergeSqlFrom25() {
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
                    FROM CSDLDV_25.SOYEU s
                             LEFT JOIN CSDLDV_25.orgHDONG_KT k
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

    private String getMergeSqlFrom26() {
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
                    FROM CSDLDV_26.SOYEU s
                             LEFT JOIN CSDLDV_26.orgHDONG_KT k
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

        Integer merged20 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting merge FINANCIAL_CONDITION records from CSDLDV_20");
                int count = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom20();
                log.info("Merged {} FINANCIAL_CONDITION records from CSDLDV_20", count);
                return count;
            } catch (Exception ex) {
                log.error("Error merging FINANCIAL_CONDITION records from CSDLDV_20: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (merged20 == null) {
            log.error("Merge FINANCIAL_CONDITION records from CSDLDV_20 transaction returned null, rolling back");
            throw new RuntimeException("Merge FINANCIAL_CONDITION records from CSDLDV_20 failed, rolled back");
        }

        Integer merged25 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting merge FINANCIAL_CONDITION records from CSDLDV_25");
                int count = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom25();
                log.info("Merged {} FINANCIAL_CONDITION records from CSDLDV_25", count);
                return count;
            } catch (Exception ex) {
                log.error("Error merging FINANCIAL_CONDITION records from CSDLDV_25: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (merged25 == null) {
            log.error("Merge FINANCIAL_CONDITION records from CSDLDV_25 transaction returned null, rolling back");
            throw new RuntimeException("Merge FINANCIAL_CONDITION records from CSDLDV_25 failed, rolled back");
        }

        Integer merged26 = transactionTemplate.execute(status -> {
            try {
                log.info("Starting merge FINANCIAL_CONDITION records from CSDLDV_26");
                int count = partyMemberFinancialConditionRepository.mergeFinancialConditionFrom26();
                log.info("Merged {} FINANCIAL_CONDITION records from CSDLDV_26", count);
                return count;
            } catch (Exception ex) {
                log.error("Error merging FINANCIAL_CONDITION records from CSDLDV_26: {}", ex.getMessage(), ex);
                throw ex;
            }
        });

        if (merged26 == null) {
            log.error("Merge FINANCIAL_CONDITION records from CSDLDV_26 transaction returned null, rolling back");
            throw new RuntimeException("Merge FINANCIAL_CONDITION records from CSDLDV_26 failed, rolled back");
        }

        log.info("=============================================");
        log.info("Finished FINANCIAL_CONDITION migration");
        log.info("Total records merged from CSDLDV_20: {}", merged20);
        log.info("Total records merged from CSDLDV_25: {}", merged25);
        log.info("Total records merged from CSDLDV_26: {}", merged26);
        log.info("Total records merged: {}", merged20 + merged25 + merged26);
        log.info("=============================================");
    }
}

