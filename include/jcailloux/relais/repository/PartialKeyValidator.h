#ifndef JCX_RELAIS_PARTIALKEYVALIDATOR_H
#define JCX_RELAIS_PARTIALKEYVALIDATOR_H

#include <string>
#include <vector>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/DbProvider.h"
#include "jcailloux/relais/Log.h"

namespace jcailloux::relais {

/**
 * Validation utilities for partial key repositories.
 *
 * When using a partial key (e.g., just `id` instead of composite `(id, created_at)`),
 * these utilities help validate that the partial key is safe to use:
 *
 * 1. validateKeyUsesSequenceOrUuid() - Checks that the key column uses SEQUENCE or UUID
 * 2. validatePartitionColumns() - Checks that missing PK columns are partition columns
 *
 * Call these at application startup to catch configuration errors early.
 */
class PartialKeyValidator {
public:
    struct ValidationResult {
        bool valid = false;
        std::string reason;
    };

    /**
     * Validates that a key column is guaranteed unique via SEQUENCE or UUID.
     *
     * @param tableName The table name (without schema)
     * @param keyColumn The column name used as partial key
     * @return ValidationResult with valid=true if column uses sequence or is UUID type
     */
    static io::Task<ValidationResult> validateKeyUsesSequenceOrUuid(
        const std::string& tableName,
        const std::string& keyColumn
    ) {
        // Check for sequence default (SERIAL/BIGSERIAL)
        try {
            auto seqResult = co_await DbProvider::queryArgs(R"(
                SELECT pg_get_expr(d.adbin, d.adrelid) as default_expr
                FROM pg_attribute a
                JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE c.relname = $1 AND a.attname = $2
            )", tableName, keyColumn);

            if (seqResult.rows() > 0 && !seqResult[0].isNull(0)) {
                auto defaultExpr = seqResult[0].get<std::string>(0);
                if (defaultExpr.find("nextval(") != std::string::npos) {
                    co_return ValidationResult{true, "Column uses SEQUENCE (globally unique)"};
                }
            }
        } catch (const std::exception& e) {
            RELAIS_LOG_WARN << "PartialKeyValidator: Failed to check sequence for "
                     << tableName << "." << keyColumn << ": " << e.what();
        }

        // Check for UUID type
        try {
            auto typeResult = co_await DbProvider::queryArgs(R"(
                SELECT t.typname
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE c.relname = $1 AND a.attname = $2
            )", tableName, keyColumn);

            if (typeResult.rows() > 0) {
                auto typeName = typeResult[0].get<std::string>(0);
                if (typeName == "uuid") {
                    co_return ValidationResult{true, "Column is UUID type (practically unique)"};
                }
            }
        } catch (const std::exception& e) {
            RELAIS_LOG_WARN << "PartialKeyValidator: Failed to check type for "
                     << tableName << "." << keyColumn << ": " << e.what();
        }

        co_return ValidationResult{
            false,
            "Column '" + keyColumn + "' does not use SEQUENCE or UUID - uniqueness not guaranteed"
        };
    }

    /**
     * Validates that missing PK columns are partition columns.
     *
     * For a partitioned table, the partition key must be part of the PK.
     * This validates that the columns we're omitting from our Key template
     * are exactly the partition columns.
     *
     * @param tableName The table name (without schema)
     * @param templateKeyColumns Columns used in the repository Key template
     * @return ValidationResult indicating if the configuration is valid
     */
    static io::Task<ValidationResult> validatePartitionColumns(
        const std::string& tableName,
        const std::vector<std::string>& templateKeyColumns
    ) {
        // Get partition columns
        std::vector<std::string> partitionCols;
        try {
            auto partResult = co_await DbProvider::queryArgs(R"(
                SELECT a.attname
                FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs)
                WHERE c.relname = $1
            )", tableName);

            for (int i = 0; i < partResult.rows(); ++i) {
                partitionCols.push_back(partResult[i].get<std::string>(0));
            }
        } catch (const std::exception& e) {
            // Table might not be partitioned - that's OK
            co_return ValidationResult{true, "Table is not partitioned"};
        }

        if (partitionCols.empty()) {
            co_return ValidationResult{true, "Table is not partitioned"};
        }

        // Get PK columns
        std::vector<std::string> pkCols;
        try {
            auto pkResult = co_await DbProvider::queryArgs(R"(
                SELECT a.attname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE c.relname = $1 AND i.indisprimary
            )", tableName);

            for (int i = 0; i < pkResult.rows(); ++i) {
                pkCols.push_back(pkResult[i].get<std::string>(0));
            }
        } catch (const std::exception& e) {
            co_return ValidationResult{false, "Failed to get PK columns: " + std::string(e.what())};
        }

        // Compute: missing_cols = pk_cols - template_cols
        std::vector<std::string> missingCols;
        for (const auto& pkCol : pkCols) {
            bool found = false;
            for (const auto& templateCol : templateKeyColumns) {
                if (pkCol == templateCol) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingCols.push_back(pkCol);
            }
        }

        // Check: missing_cols ⊆ partition_cols
        for (const auto& missing : missingCols) {
            bool isPartitionCol = false;
            for (const auto& partCol : partitionCols) {
                if (missing == partCol) {
                    isPartitionCol = true;
                    break;
                }
            }
            if (!isPartitionCol) {
                co_return ValidationResult{
                    false,
                    "PK column '" + missing + "' is not in template and is not a partition column"
                };
            }
        }

        co_return ValidationResult{
            true,
            "All omitted PK columns are partition columns"
        };
    }

    /**
     * Convenience method to run all validations.
     *
     * @param tableName The table name
     * @param keyColumn The column used as partial key
     * @return true if all validations pass
     */
    static io::Task<bool> validateAll(
        const std::string& tableName,
        const std::string& keyColumn
    ) {
        auto seqResult = co_await validateKeyUsesSequenceOrUuid(tableName, keyColumn);
        if (!seqResult.valid) {
            RELAIS_LOG_ERROR << "PartialKeyValidator [" << tableName << "]: " << seqResult.reason;
            co_return false;
        }
        RELAIS_LOG_DEBUG << "PartialKeyValidator [" << tableName << "]: " << seqResult.reason;

        auto partResult = co_await validatePartitionColumns(tableName, {keyColumn});
        if (!partResult.valid) {
            RELAIS_LOG_ERROR << "PartialKeyValidator [" << tableName << "]: " << partResult.reason;
            co_return false;
        }
        RELAIS_LOG_DEBUG << "PartialKeyValidator [" << tableName << "]: " << partResult.reason;

        co_return true;
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_PARTIALKEYVALIDATOR_H