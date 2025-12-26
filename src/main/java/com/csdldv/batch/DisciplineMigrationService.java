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

    public void updateDisciplineFormAndReasonInBatches(int batchSize) {
        log.info("Starting PARTY_MEMBER_DISCIPLINE migration (MA_KL, LYDO) with batch size {}", batchSize);

        String selectSql = """
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
        log.info("SQL SELECT to find IDs:\n{}", selectSql);

        List<String> allIds = partyMemberDisciplineRepository.findAllIdsForUpdate();
        log.info("Found {} total records to update", allIds.size());

        String updateSqlTemplate = """
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
        log.info("SQL UPDATE template:\n{}", updateSqlTemplate);

        long totalUpdated = 0;

        try {
            for (int i = 0; i < allIds.size(); i += batchSize) {
                final int startIndex = i;
                int endIndex = Math.min(i + batchSize, allIds.size());
                List<String> batchIds = allIds.subList(i, endIndex);

                String idsPlaceholder = String.join("','", batchIds);
                String updateSql = updateSqlTemplate.replace(":ids", "'" + idsPlaceholder + "'");
                log.info("SQL UPDATE for batch {} to {}:\n{}", startIndex + 1, endIndex, updateSql);

                Integer updated = transactionTemplate.execute(status -> {
                    log.info("Updating batch {} to {} of {}", startIndex + 1, endIndex, allIds.size());
                    return partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReason(batchIds);
                });

                totalUpdated += updated;

                log.info("Batch updated {} rows, total updated {}", updated, totalUpdated);
            }

            log.info("=============================================");
            log.info("Finished PARTY_MEMBER_DISCIPLINE migration, total updated {} rows", totalUpdated);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("PARTY_MEMBER_DISCIPLINE migration encountered error: {}", ex.getMessage(), ex);

            throw ex;
        }
    }
}

