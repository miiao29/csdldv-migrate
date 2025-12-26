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
    private final TrainingProcessMigrationService trainingProcessMigrationService;
    private final int familyBatchSize;
    private final int familyMegaBatchSize;
    private final int disciplineBatchSize;
    private final int disciplineMegaBatchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, DisciplineMigrationService disciplineMigrationService, TrainingProcessMigrationService trainingProcessMigrationService, @Value("${migration.family.batch-size:1000}") int familyBatchSize, @Value("${migration.family.mega-batch-size:100000}") int familyMegaBatchSize, @Value("${migration.discipline.batch-size:1000}") int disciplineBatchSize, @Value("${migration.discipline.mega-batch-size:100000}") int disciplineMegaBatchSize) {
        this.familyMigrationService = familyMigrationService;
        this.disciplineMigrationService = disciplineMigrationService;
        this.trainingProcessMigrationService = trainingProcessMigrationService;
        this.familyBatchSize = familyBatchSize;
        this.familyMegaBatchSize = familyMegaBatchSize;
        this.disciplineBatchSize = disciplineBatchSize;
        this.disciplineMegaBatchSize = disciplineMegaBatchSize;
    }

    @Override
    public void run(String... args) {
        log.info("Preparing migration menu");

        while (true) {
            displayMainMenu();

            String key = readKey();

            log.info("Received migration key {}", key);

            if ("0".equals(key)) {
                log.info("Exiting migration program");
                System.exit(0);
            } else if ("1".equals(key)) {
                handleFamilyMigration();
            } else if ("2".equals(key)) {
                handleDisciplineMigration();
            } else if ("3.2".equals(key)) {
                handleTrainingProcessMigration();
            } else {
                log.warn("Invalid migration key {}, please enter again", key);
                System.out.println("Invalid option. Please enter 0 to exit, 1 for Family migration, 2 for PARTY_MEMBER_DISCIPLINE migration, or 3.1 for PARTY_MEMBER_TRAINING_PROCESS migration.");
            }
        }
    }

    private void handleFamilyMigration() {
        while (true) {
            displaySubMenu("Family Migration");

            String subKey = readKey();

            log.info("Received sub-menu key {} for Family migration", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for Family migration");
                familyMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting family type migration with batch size {}, mega-batch size {}", familyBatchSize, familyMegaBatchSize);

                try {
                    familyMigrationService.migrateFamilyTypeInBatches(familyBatchSize, familyMegaBatchSize);
                    log.info("Family type migration completed successfully");
                } catch (Exception ex) {
                    log.error("Family type migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleDisciplineMigration() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_DISCIPLINE Migration");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_DISCIPLINE migration", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_DISCIPLINE migration");
                disciplineMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_DISCIPLINE migration with batch size {}, mega-batch size {}", disciplineBatchSize, disciplineMegaBatchSize);

                try {
                    disciplineMigrationService.updateDisciplineFormAndReasonInBatches(disciplineBatchSize, disciplineMegaBatchSize);
                    log.info("PARTY_MEMBER_DISCIPLINE migration completed successfully");
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_DISCIPLINE migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleTrainingProcessMigration() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_TRAINING_PROCESS Migration");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_TRAINING_PROCESS migration", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_TRAINING_PROCESS migration");
                trainingProcessMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Delete invalid or duplicate records");

                try {
                    trainingProcessMigrationService.deleteInvalidAndDuplicateRecords();
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration completed successfully");
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
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

    private void displayMainMenu() {
        log.info("Main migration menu displayed");

        System.out.println("=== Main Migration Menu ===");
        System.out.println("0: Exit");
        System.out.println("1: Family Migration (batch size " + familyBatchSize + ")");
        System.out.println("2: PARTY_MEMBER_DISCIPLINE Migration (batch size " + disciplineBatchSize + ")");
        System.out.println("3.2: PARTY_MEMBER_TRAINING_PROCESS Migration (Delete invalid or duplicate records)");
        System.out.print("Enter option: ");
    }

    private void displaySubMenu(String functionName) {
        log.info("Sub-menu displayed for {}", functionName);

        System.out.println("=== " + functionName + " ===");
        System.out.println("0: Return to main menu");
        System.out.println("1: View SQL (DELETE + UPDATE + SELECT)");
        System.out.println("2: Execute migration");
        System.out.print("Enter option: ");
    }
}

