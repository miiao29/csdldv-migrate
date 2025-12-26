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

        List<String> allIds = partyMemberDisciplineRepository.findAllIdsForUpdate();
        log.info("Found {} total records to update", allIds.size());

        long totalUpdated = 0;

        try {
            for (int i = 0; i < allIds.size(); i += batchSize) {
                final int startIndex = i;
                int endIndex = Math.min(i + batchSize, allIds.size());
                List<String> batchIds = allIds.subList(i, endIndex);

                Integer updated = transactionTemplate.execute(status -> {
                    log.info("Updating batch {} to {} of {}", startIndex + 1, endIndex, allIds.size());
                    return partyMemberDisciplineRepository.bulkUpdateDisciplineFormAndReason(batchIds);
                });

                totalUpdated += updated;

                log.info("Batch updated {} rows, total updated {}", updated, totalUpdated);
            }

            log.info("Finished PARTY_MEMBER_DISCIPLINE migration, total updated {} rows", totalUpdated);
        } catch (Exception ex) {
            log.error("PARTY_MEMBER_DISCIPLINE migration encountered error: {}", ex.getMessage(), ex);

            throw ex;
        }
    }
}

