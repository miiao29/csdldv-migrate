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

    public void migrateFamilyTypeInBatches(int batchSize) {
        log.info("Starting family type migration with batch size {}", batchSize);

        String selectSql = """
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
        log.info("SQL SELECT to find IDs:\n{}", selectSql);

        List<String> allIds = partyMemberFamilyRepository.findAllIdsForTypeOne();
        log.info("Found {} total records to update", allIds.size());

        String updateTypeOneSqlTemplate = """
                UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
                SET TYPE = 1
                WHERE PARTY_MEMBER_FAMILY_ID IN (:ids)
                """;
        log.info("SQL UPDATE template to set TYPE = 1:\n{}", updateTypeOneSqlTemplate);

        long totalUpdated = 0;

        try {
            for (int i = 0; i < allIds.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, allIds.size());
                List<String> batchIds = allIds.subList(i, endIndex);

                Integer updated = transactionTemplate.execute(status -> {
                    return partyMemberFamilyRepository.bulkSetTypeOne(batchIds);
                });

                totalUpdated += updated;
            }
            Integer setZero = transactionTemplate.execute(status -> partyMemberFamilyRepository.bulkSetTypeZero());

            log.info("=============================================");
            log.info("Finished family type migration, set type=1 {}, set type=0 {}", totalUpdated, setZero);
            log.info("=============================================");
        } catch (Exception ex) {
            log.error("Family type migration encountered error: {}", ex.getMessage(), ex);

            throw ex;
        }
    }
}

