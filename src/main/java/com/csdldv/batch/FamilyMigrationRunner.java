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
    private final int trainingProcess31BatchSize;
    private final int trainingProcess31MegaBatchSize;
    private final int trainingProcess32BatchSize;
    private final int trainingProcess32MegaBatchSize;
    private final int trainingProcess33BatchSize;
    private final int trainingProcess33MegaBatchSize;
    private final int trainingProcess34BatchSize;
    private final int trainingProcess34MegaBatchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, DisciplineMigrationService disciplineMigrationService, TrainingProcessMigrationService trainingProcessMigrationService, @Value("${migration.family.batch-size:1000}") int familyBatchSize, @Value("${migration.family.mega-batch-size:100000}") int familyMegaBatchSize, @Value("${migration.discipline.batch-size:1000}") int disciplineBatchSize, @Value("${migration.discipline.mega-batch-size:100000}") int disciplineMegaBatchSize, @Value("${migration.training-process-31.batch-size:1000}") int trainingProcess31BatchSize, @Value("${migration.training-process-31.mega-batch-size:100000}") int trainingProcess31MegaBatchSize, @Value("${migration.training-process-32.batch-size:1000}") int trainingProcess32BatchSize, @Value("${migration.training-process-32.mega-batch-size:100000}") int trainingProcess32MegaBatchSize, @Value("${migration.training-process-33.batch-size:1000}") int trainingProcess33BatchSize, @Value("${migration.training-process-33.mega-batch-size:100000}") int trainingProcess33MegaBatchSize, @Value("${migration.training-process-34.batch-size:1000}") int trainingProcess34BatchSize, @Value("${migration.training-process-34.mega-batch-size:100000}") int trainingProcess34MegaBatchSize) {
        this.familyMigrationService = familyMigrationService;
        this.disciplineMigrationService = disciplineMigrationService;
        this.trainingProcessMigrationService = trainingProcessMigrationService;
        this.familyBatchSize = familyBatchSize;
        this.familyMegaBatchSize = familyMegaBatchSize;
        this.disciplineBatchSize = disciplineBatchSize;
        this.disciplineMegaBatchSize = disciplineMegaBatchSize;
        this.trainingProcess31BatchSize = trainingProcess31BatchSize;
        this.trainingProcess31MegaBatchSize = trainingProcess31MegaBatchSize;
        this.trainingProcess32BatchSize = trainingProcess32BatchSize;
        this.trainingProcess32MegaBatchSize = trainingProcess32MegaBatchSize;
        this.trainingProcess33BatchSize = trainingProcess33BatchSize;
        this.trainingProcess33MegaBatchSize = trainingProcess33MegaBatchSize;
        this.trainingProcess34BatchSize = trainingProcess34BatchSize;
        this.trainingProcess34MegaBatchSize = trainingProcess34MegaBatchSize;
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
            } else if ("4".equals(key)) {
                handleTrainingProcessMigration31();
            } else if ("3".equals(key)) {
                handleTrainingProcessMigration32();
            } else if ("5".equals(key)) {
                handleTrainingProcessMigration33();
            } else if ("6".equals(key)) {
                handleTrainingProcessMigration34();
            } else {
                log.warn("Invalid migration key {}, please enter again", key);
                System.out.println("Invalid option. Please enter 0 to exit, 1 for Family migration, 2 for PARTY_MEMBER_DISCIPLINE migration, 3 for PARTY_MEMBER_TRAINING_PROCESS migration (Delete invalid or duplicate records), 4 for PARTY_MEMBER_TRAINING_PROCESS migration (Insert MA_LLCT), 5 for PARTY_MEMBER_TRAINING_PROCESS migration (Insert MA_BANGDT), or 6 for PARTY_MEMBER_TRAINING_PROCESS migration (Insert MA_BANGNN).");
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
                    log.info("Returning to main menu");
                    break;
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
                    log.info("Returning to main menu");
                    break;
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

    private void handleTrainingProcessMigration31() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_TRAINING_PROCESS Migration - Chức năng 3.1 (Update MA_LLCT)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_TRAINING_PROCESS migration 3.1", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_TRAINING_PROCESS migration 3.1");
                trainingProcessMigrationService.displaySqlFor31();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration 3.1 - Insert MA_LLCT records with batch size {}, mega-batch size {}", trainingProcess31BatchSize, trainingProcess31MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaLLCTRecords(trainingProcess31BatchSize, trainingProcess31MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration 3.1 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration 3.1 failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleTrainingProcessMigration32() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_TRAINING_PROCESS Migration - Chức năng 3.2 (Delete invalid or duplicate records)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_TRAINING_PROCESS migration 3.2", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_TRAINING_PROCESS migration 3.2");
                trainingProcessMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration 3.2 - Delete invalid or duplicate records with batch size {}, mega-batch size {}", trainingProcess32BatchSize, trainingProcess32MegaBatchSize);

                try {
                    trainingProcessMigrationService.deleteInvalidAndDuplicateRecords(trainingProcess32BatchSize, trainingProcess32MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration 3.2 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration 3.2 failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleTrainingProcessMigration33() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_TRAINING_PROCESS Migration - Chức năng 3.3 (Insert MA_BANGDT)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_TRAINING_PROCESS migration 3.3", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_TRAINING_PROCESS migration 3.3");
                trainingProcessMigrationService.displaySqlFor33();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration 3.3 - Insert MA_BANGDT records with batch size {}, mega-batch size {}", trainingProcess33BatchSize, trainingProcess33MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaBANGDTRecords(trainingProcess33BatchSize, trainingProcess33MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration 3.3 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration 3.3 failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleTrainingProcessMigration34() {
        while (true) {
            displaySubMenu("PARTY_MEMBER_TRAINING_PROCESS Migration - Chức năng 3.4 (Insert MA_BANGNN)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for PARTY_MEMBER_TRAINING_PROCESS migration 3.4", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for PARTY_MEMBER_TRAINING_PROCESS migration 3.4");
                trainingProcessMigrationService.displaySqlFor34();
            } else if ("2".equals(subKey)) {
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration 3.4 - Insert MA_BANGNN records with batch size {}, mega-batch size {}", trainingProcess34BatchSize, trainingProcess34MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaBANGNNRecords(trainingProcess34BatchSize, trainingProcess34MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration 3.4 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration 3.4 failed: {}", ex.getMessage(), ex);
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

        System.out.println("=== Main Migration Menu ===\n");
        System.out.println("0: Exit\n");
        System.out.println("1: Family Migration (batch size " + familyBatchSize + ")\n");
        System.out.println("2: PARTY_MEMBER_DISCIPLINE Migration (batch size " + disciplineBatchSize + ")\n");
        System.out.println("3: PARTY_MEMBER_TRAINING_PROCESS Migration (Delete invalid or duplicate records) (batch size " + trainingProcess32BatchSize + ")");
        System.out.println("4: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_LLCT) (batch size " + trainingProcess31BatchSize + ")");
        System.out.println("5: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_BANGDT) (batch size " + trainingProcess33BatchSize + ")");
        System.out.println("6: PARTY_MEMBER_TRAINING_PROCESS Migration (Insert MA_BANGNN) (batch size " + trainingProcess34BatchSize + ")\n");
        System.out.print("Enter option: ");
    }

    private void displaySubMenu(String functionName) {
        log.info("Sub-menu displayed for {}", functionName);

        System.out.println("\n\n\n\n\n\n\n\n=== " + functionName + " ===");
        System.out.println("0: Return to main menu");
        System.out.println("1: View SQL (DELETE + UPDATE + SELECT)");
        System.out.println("2: Execute migration");
        System.out.print("Enter option: ");
    }
}

