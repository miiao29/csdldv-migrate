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
    private final int batchSize;

    public FamilyMigrationRunner(FamilyMigrationService familyMigrationService, @Value("${migration.family.batch-size:1000}") int batchSize) {
        this.familyMigrationService = familyMigrationService;
        this.batchSize = batchSize;
    }

    @Override
    public void run(String... args) {
        log.info("Preparing migration menu");

        displayMenu();

        String key = readKey();

        log.info("Received migration key {}", key);

        if ("1".equals(key)) {
            log.info("Starting family type migration with batch size {}", batchSize);

            try {
                familyMigrationService.migrateFamilyTypeInBatches(batchSize);
            } catch (Exception ex) {
                log.error("Family type migration failed: {}", ex.getMessage(), ex);
            }

            log.info("Finished family type migration");
        } else {
            log.info("No migration executed for key {}", key);
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
        System.out.println("1: Family (batch size " + batchSize + ")");
        System.out.println("Other: Skip migrations");
        System.out.print("Enter migration key: ");
    }
}

