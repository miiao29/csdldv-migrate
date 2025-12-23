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

        long totalUpdated = 0;

        try {
            while (true) {
                List<String> ids = partyMemberFamilyRepository.findIdsForTypeOne(batchSize);

                if (ids.isEmpty()) {
                    break;
                }

                Integer updated = transactionTemplate.execute(status -> partyMemberFamilyRepository.bulkSetTypeOne(ids));

                totalUpdated += updated;

                log.info("Batch updated {} rows, total updated {}", updated, totalUpdated);
            }

            Integer setZero = transactionTemplate.execute(status -> partyMemberFamilyRepository.bulkSetTypeZero());

            log.info("Finished family type migration, set type=1 {}, set type=0 {}", totalUpdated, setZero);
        } catch (Exception ex) {
            log.error("Family type migration encountered error: {}", ex.getMessage(), ex);

            throw ex;
        }
    }
}

