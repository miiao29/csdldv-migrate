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
    private final FinancialConditionMigrationService financialConditionMigrationService;
    private final FinancialConditionListMigrationService financialConditionListMigrationService;
    private final int familyBatchSize;
    private final int familyMegaBatchSize;
    private final int disciplineBatchSize;
    private final int disciplineMegaBatchSize;
    private final int trainingProcess31BatchSize;
    private final int trainingProcess31MegaBatchSize;
    private final int trainingProcess33BatchSize;
    private final int trainingProcess33MegaBatchSize;
    private final int trainingProcess34BatchSize;
    private final int trainingProcess34MegaBatchSize;
    private final int financialConditionBatchSize;
    private final int financialConditionMegaBatchSize;
    private final int financialConditionListBatchSize;
    private final int financialConditionListMegaBatchSize;
    private final int financialConditionList2BatchSize;
    private final int financialConditionList2MegaBatchSize;
    private final int financialConditionList3BatchSize;
    private final int financialConditionList3MegaBatchSize;
    private final int financialConditionList4BatchSize;
    private final int financialConditionList4MegaBatchSize;
    private final int financialConditionList5BatchSize;
    private final int financialConditionList5MegaBatchSize;
    private final int financialConditionList6BatchSize;
    private final int financialConditionList6MegaBatchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, DisciplineMigrationService disciplineMigrationService, TrainingProcessMigrationService trainingProcessMigrationService, FinancialConditionMigrationService financialConditionMigrationService, FinancialConditionListMigrationService financialConditionListMigrationService, @Value("${migration.family.batch-size:1000}") int familyBatchSize, @Value("${migration.family.mega-batch-size:100000}") int familyMegaBatchSize, @Value("${migration.discipline.batch-size:1000}") int disciplineBatchSize, @Value("${migration.discipline.mega-batch-size:100000}") int disciplineMegaBatchSize, @Value("${migration.training-process-31.batch-size:1000}") int trainingProcess31BatchSize, @Value("${migration.training-process-31.mega-batch-size:100000}") int trainingProcess31MegaBatchSize, @Value("${migration.training-process-33.batch-size:1000}") int trainingProcess33BatchSize, @Value("${migration.training-process-33.mega-batch-size:100000}") int trainingProcess33MegaBatchSize, @Value("${migration.training-process-34.batch-size:1000}") int trainingProcess34BatchSize, @Value("${migration.training-process-34.mega-batch-size:100000}") int trainingProcess34MegaBatchSize, @Value("${migration.financial-condition.batch-size:1000}") int financialConditionBatchSize, @Value("${migration.financial-condition.mega-batch-size:100000}") int financialConditionMegaBatchSize, @Value("${migration.financial-condition-list.batch-size:1000}") int financialConditionListBatchSize, @Value("${migration.financial-condition-list.mega-batch-size:100000}") int financialConditionListMegaBatchSize, @Value("${migration.financial-condition-list-2.batch-size:1000}") int financialConditionList2BatchSize, @Value("${migration.financial-condition-list-2.mega-batch-size:100000}") int financialConditionList2MegaBatchSize, @Value("${migration.financial-condition-list-3.batch-size:1000}") int financialConditionList3BatchSize, @Value("${migration.financial-condition-list-3.mega-batch-size:100000}") int financialConditionList3MegaBatchSize, @Value("${migration.financial-condition-list-4.batch-size:1000}") int financialConditionList4BatchSize, @Value("${migration.financial-condition-list-4.mega-batch-size:100000}") int financialConditionList4MegaBatchSize, @Value("${migration.financial-condition-list-5.batch-size:1000}") int financialConditionList5BatchSize, @Value("${migration.financial-condition-list-5.mega-batch-size:100000}") int financialConditionList5MegaBatchSize, @Value("${migration.financial-condition-list-6.batch-size:1000}") int financialConditionList6BatchSize, @Value("${migration.financial-condition-list-6.mega-batch-size:100000}") int financialConditionList6MegaBatchSize) {
        this.familyMigrationService = familyMigrationService;
        this.disciplineMigrationService = disciplineMigrationService;
        this.trainingProcessMigrationService = trainingProcessMigrationService;
        this.financialConditionMigrationService = financialConditionMigrationService;
        this.financialConditionListMigrationService = financialConditionListMigrationService;
        this.familyBatchSize = familyBatchSize;
        this.familyMegaBatchSize = familyMegaBatchSize;
        this.disciplineBatchSize = disciplineBatchSize;
        this.disciplineMegaBatchSize = disciplineMegaBatchSize;
        this.trainingProcess31BatchSize = trainingProcess31BatchSize;
        this.trainingProcess31MegaBatchSize = trainingProcess31MegaBatchSize;
        this.trainingProcess33BatchSize = trainingProcess33BatchSize;
        this.trainingProcess33MegaBatchSize = trainingProcess33MegaBatchSize;
        this.trainingProcess34BatchSize = trainingProcess34BatchSize;
        this.trainingProcess34MegaBatchSize = trainingProcess34MegaBatchSize;
        this.financialConditionBatchSize = financialConditionBatchSize;
        this.financialConditionMegaBatchSize = financialConditionMegaBatchSize;
        this.financialConditionListBatchSize = financialConditionListBatchSize;
        this.financialConditionListMegaBatchSize = financialConditionListMegaBatchSize;
        this.financialConditionList2BatchSize = financialConditionList2BatchSize;
        this.financialConditionList2MegaBatchSize = financialConditionList2MegaBatchSize;
        this.financialConditionList3BatchSize = financialConditionList3BatchSize;
        this.financialConditionList3MegaBatchSize = financialConditionList3MegaBatchSize;
        this.financialConditionList4BatchSize = financialConditionList4BatchSize;
        this.financialConditionList4MegaBatchSize = financialConditionList4MegaBatchSize;
        this.financialConditionList5BatchSize = financialConditionList5BatchSize;
        this.financialConditionList5MegaBatchSize = financialConditionList5MegaBatchSize;
        this.financialConditionList6BatchSize = financialConditionList6BatchSize;
        this.financialConditionList6MegaBatchSize = financialConditionList6MegaBatchSize;
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
            } else if ("3".equals(key)) {
                handleTrainingProcessMigration31();
            } else if ("4".equals(key)) {
                handleTrainingProcessMigration33();
            } else if ("5".equals(key)) {
                handleTrainingProcessMigration34();
            } else if ("6".equals(key)) {
                handleFinancialConditionMigration();
            } else if ("7".equals(key)) {
                handleFinancialConditionListMigration();
            } else if ("8".equals(key)) {
                handleFinancialConditionList2Migration();
            } else if ("9".equals(key)) {
                handleFinancialConditionList3Migration();
            } else if ("10".equals(key)) {
                handleFinancialConditionList4Migration();
            } else if ("11".equals(key)) {
                handleFinancialConditionList5Migration();
            } else if ("12".equals(key)) {
                handleFinancialConditionList6Migration();
            } else {
                log.warn("Invalid migration key {}, please enter again", key);
                System.out.println("Invalid option. Please enter 0 to exit, 1 for Family migration, 2 for PARTY_MEMBER_DISCIPLINE migration, 3 for PARTY_MEMBER_TRAINING_PROCESS migration (Merge MA_LLCT), 4 for PARTY_MEMBER_TRAINING_PROCESS migration (Merge MA_BANGDT), 5 for PARTY_MEMBER_TRAINING_PROCESS migration (Merge MA_BANGNN), 6 for FINANCIAL_CONDITION migration, 7 for FINANCIAL_CONDITION_LIST migration (Merge MA_NHADAT, DTNHA), 8 for FINANCIAL_CONDITION_LIST migration (Merge NHA2, DTNHA2), 9 for FINANCIAL_CONDITION_LIST migration (Merge TMP_DATCAP), 10 for FINANCIAL_CONDITION_LIST migration (Merge DATMUA), 11 for FINANCIAL_CONDITION_LIST migration (Merge DATTT), or 12 for FINANCIAL_CONDITION_LIST migration (Merge TSCOGTRI, TSGTRI).");
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
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Menu 3 - Chức năng 3.1 - Insert MA_LLCT records with batch size {}, mega-batch size {}", trainingProcess31BatchSize, trainingProcess31MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaLLCTRecords(trainingProcess31BatchSize, trainingProcess31MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 3 - Chức năng 3.1 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 3 - Chức năng 3.1 failed: {}", ex.getMessage(), ex);
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
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Menu 4 - Chức năng 3.3 - Insert MA_BANGDT records with batch size {}, mega-batch size {}", trainingProcess33BatchSize, trainingProcess33MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaBANGDTRecords(trainingProcess33BatchSize, trainingProcess33MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 4 - Chức năng 3.3 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 4 - Chức năng 3.3 failed: {}", ex.getMessage(), ex);
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
                log.info("Starting PARTY_MEMBER_TRAINING_PROCESS migration - Menu 5 - Chức năng 3.4 - Insert MA_BANGNN records with batch size {}, mega-batch size {}", trainingProcess34BatchSize, trainingProcess34MegaBatchSize);

                try {
                    trainingProcessMigrationService.insertMaBANGNNRecords(trainingProcess34BatchSize, trainingProcess34MegaBatchSize);
                    log.info("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 5 - Chức năng 3.4 completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("PARTY_MEMBER_TRAINING_PROCESS migration - Menu 5 - Chức năng 3.4 failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionMigration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION Migration");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION migration", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION migration");
                financialConditionMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION migration with batch size {}, mega-batch size {}", financialConditionBatchSize, financialConditionMegaBatchSize);

                try {
                    financialConditionMigrationService.mergeFinancialCondition(financialConditionBatchSize, financialConditionMegaBatchSize);
                    log.info("FINANCIAL_CONDITION migration completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionListMigration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge MA_NHADAT, DTNHA)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration");
                financialConditionListMigrationService.displaySql();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration with batch size {}, mega-batch size {}", financialConditionListBatchSize, financialConditionListMegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList(financialConditionListBatchSize, financialConditionListMegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionList2Migration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge NHA2, DTNHA2)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration (Menu 8)", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration (Menu 8)");
                financialConditionListMigrationService.displaySqlFor9();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 8) with batch size {}, mega-batch size {}", financialConditionList2BatchSize, financialConditionList2MegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList2(financialConditionList2BatchSize, financialConditionList2MegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration (Menu 8) completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration (Menu 8) failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionList3Migration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge TMP_DATCAP)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration (Menu 9)", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration (Menu 9)");
                financialConditionListMigrationService.displaySqlFor10();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 9) with batch size {}, mega-batch size {}", financialConditionList3BatchSize, financialConditionList3MegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList3(financialConditionList3BatchSize, financialConditionList3MegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration (Menu 9) completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration (Menu 9) failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionList4Migration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge DATMUA)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration (Menu 10)", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration (Menu 10)");
                financialConditionListMigrationService.displaySqlFor11();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 10) with batch size {}, mega-batch size {}", financialConditionList4BatchSize, financialConditionList4MegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList4(financialConditionList4BatchSize, financialConditionList4MegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration (Menu 10) completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration (Menu 10) failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionList5Migration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge DATTT)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration (Menu 11)", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration (Menu 11)");
                financialConditionListMigrationService.displaySqlFor12();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 11) with batch size {}, mega-batch size {}", financialConditionList5BatchSize, financialConditionList5MegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList5(financialConditionList5BatchSize, financialConditionList5MegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration (Menu 11) completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration (Menu 11) failed: {}", ex.getMessage(), ex);
                    System.exit(1);
                }
            } else {
                log.warn("Invalid sub-menu key {}, please enter again", subKey);
                System.out.println("Invalid option. Please enter 0 to return, 1 to view SQL, or 2 to execute migration.");
            }
        }
    }

    private void handleFinancialConditionList6Migration() {
        while (true) {
            displaySubMenu("FINANCIAL_CONDITION_LIST Migration (Merge TSCOGTRI, TSGTRI)");

            String subKey = readKey();

            log.info("Received sub-menu key {} for FINANCIAL_CONDITION_LIST migration (Menu 12)", subKey);

            if ("0".equals(subKey)) {
                log.info("Returning to main menu");
                break;
            } else if ("1".equals(subKey)) {
                log.info("Displaying SQL for FINANCIAL_CONDITION_LIST migration (Menu 12)");
                financialConditionListMigrationService.displaySqlFor13();
            } else if ("2".equals(subKey)) {
                log.info("Starting FINANCIAL_CONDITION_LIST migration (Menu 12) with batch size {}, mega-batch size {}", financialConditionList6BatchSize, financialConditionList6MegaBatchSize);

                try {
                    financialConditionListMigrationService.mergeFinancialConditionList6(financialConditionList6BatchSize, financialConditionList6MegaBatchSize);
                    log.info("FINANCIAL_CONDITION_LIST migration (Menu 12) completed successfully");
                    log.info("Returning to main menu");
                    break;
                } catch (Exception ex) {
                    log.error("FINANCIAL_CONDITION_LIST migration (Menu 12) failed: {}", ex.getMessage(), ex);
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

        System.out.println("3: PARTY_MEMBER_TRAINING_PROCESS Migration (Merge MA_LLCT) (batch size " + trainingProcess31BatchSize + ")");
        System.out.println("4: PARTY_MEMBER_TRAINING_PROCESS Migration (Merge MA_BANGDT) (batch size " + trainingProcess33BatchSize + ")");
        System.out.println("5: PARTY_MEMBER_TRAINING_PROCESS Migration (Merge MA_BANGNN) (batch size " + trainingProcess34BatchSize + ")\n");

        System.out.println("6: FINANCIAL_CONDITION Migration (batch size " + financialConditionBatchSize + ")\n");

        System.out.println("7: FINANCIAL_CONDITION_LIST Migration (Merge MA_NHADAT, DTNHA) (batch size " + financialConditionListBatchSize + ")");
        System.out.println("8: FINANCIAL_CONDITION_LIST Migration (Merge NHA2, DTNHA2) (batch size " + financialConditionList2BatchSize + ")");
        System.out.println("9: FINANCIAL_CONDITION_LIST Migration (Merge TMP_DATCAP) (batch size " + financialConditionList3BatchSize + ")");
        System.out.println("10: FINANCIAL_CONDITION_LIST Migration (Merge DATMUA) (batch size " + financialConditionList4BatchSize + ")");
        System.out.println("11: FINANCIAL_CONDITION_LIST Migration (Merge DATTT) (batch size " + financialConditionList5BatchSize + ")");
        System.out.println("12: FINANCIAL_CONDITION_LIST Migration (Merge TSCOGTRI, TSGTRI) (batch size " + financialConditionList6BatchSize + ")\n");

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

