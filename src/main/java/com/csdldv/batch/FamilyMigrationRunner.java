package com.csdldv.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import java.io.Console;
import java.util.Scanner;

@Component
public class FamilyMigrationRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FamilyMigrationRunner.class);

    private final FamilyMigrationService familyMigrationService;
    private final DisciplineMigrationService disciplineMigrationService;
    private final int familyBatchSize;
    private final int disciplineBatchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, DisciplineMigrationService disciplineMigrationService, @Value("${migration.family.batch-size:1000}") int familyBatchSize, @Value("${migration.discipline.batch-size:1000}") int disciplineBatchSize) {
        this.familyMigrationService = familyMigrationService;
        this.disciplineMigrationService = disciplineMigrationService;
        this.familyBatchSize = familyBatchSize;
        this.disciplineBatchSize = disciplineBatchSize;
    }

    @Override
    public void run(String... args) {
        log.info("Preparing migration menu");

        while (true) {
            displayMenu();

            String key = readKey();

            log.info("Received migration key {}", key);

            if ("0".equals(key)) {
                log.info("Exiting migration program");
                System.exit(0);
            } else if ("1".equals(key)) {
                log.info("Starting family type migration with batch size {}", familyBatchSize);

                try {
                    familyMigrationService.migrateFamilyTypeInBatches(familyBatchSize);
                } catch (Exception ex) {
                    log.error("Family type migration failed: {}", ex.getMessage(), ex);
                }

                log.info("Finished family type migration");
                break;
            } else if ("2".equals(key)) {
                log.info("Starting PARTY_MEMBER_DISCIPLINE migration with batch size {}", disciplineBatchSize);

                try {
                    disciplineMigrationService.updateDisciplineFormAndReasonInBatches(disciplineBatchSize);
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_DISCIPLINE migration failed: {}", ex.getMessage(), ex);
                }

                log.info("Finished PARTY_MEMBER_DISCIPLINE migration");
                break;
            } else {
                log.warn("Invalid migration key {}, please enter again", key);
                System.out.println("Invalid option. Please enter 0 to exit, 1 for Family migration, or 2 for PARTY_MEMBER_DISCIPLINE migration.");
            }
        }
    }

    @SuppressWarnings("resource")
    private String readKey() {
        Console console = System.console();

        if (console != null) {
            String line = console.readLine();

            return line == null ? "" : line.trim();
        }

        Scanner scanner = new Scanner(System.in);

        String line = scanner.hasNextLine() ? scanner.nextLine() : "";

        return line.trim();
    }

    private void displayMenu() {
        log.info("migration key options displayed");

        System.out.println("=== Migration Menu ===");
        System.out.println("0: Exit");
        System.out.println("1: Family (batch size " + familyBatchSize + ")");
        System.out.println("2: Update PARTY_MEMBER_DISCIPLINE (MA_KL, LYDO) (batch size " + disciplineBatchSize + ")");
        System.out.print("Enter migration key: ");
    }
}

