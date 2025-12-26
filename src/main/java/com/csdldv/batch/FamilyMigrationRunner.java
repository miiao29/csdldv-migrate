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
    private final int familyMegaBatchSize;
    private final int disciplineBatchSize;
    private final int disciplineMegaBatchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, DisciplineMigrationService disciplineMigrationService, @Value("${migration.family.batch-size:1000}") int familyBatchSize, @Value("${migration.family.mega-batch-size:100000}") int familyMegaBatchSize, @Value("${migration.discipline.batch-size:1000}") int disciplineBatchSize, @Value("${migration.discipline.mega-batch-size:100000}") int disciplineMegaBatchSize) {
        this.familyMigrationService = familyMigrationService;
        this.disciplineMigrationService = disciplineMigrationService;
        this.familyBatchSize = familyBatchSize;
        this.familyMegaBatchSize = familyMegaBatchSize;
        this.disciplineBatchSize = disciplineBatchSize;
        this.disciplineMegaBatchSize = disciplineMegaBatchSize;
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
                log.info("Starting family type migration with batch size {}, mega-batch size {}", familyBatchSize, familyMegaBatchSize);

                try {
                    familyMigrationService.migrateFamilyTypeInBatches(familyBatchSize, familyMegaBatchSize);
                    log.info("Family type migration completed successfully");
                } catch (Exception ex) {
                    log.error("Family type migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else if ("2".equals(key)) {
                log.info("Starting PARTY_MEMBER_DISCIPLINE migration with batch size {}, mega-batch size {}", disciplineBatchSize, disciplineMegaBatchSize);

                try {
                    disciplineMigrationService.updateDisciplineFormAndReasonInBatches(disciplineBatchSize, disciplineMegaBatchSize);
                    log.info("PARTY_MEMBER_DISCIPLINE migration completed successfully");
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_DISCIPLINE migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
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

