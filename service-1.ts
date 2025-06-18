// This is a test commit
import { Injectable, Logger, HttpException, HttpStatus } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import * as sql from 'mssql';
import { firstValueFrom } from 'rxjs';

// Define your data interface based on external API response
interface ExternalApiRecord {
    id: number;
    name: string;
    value: number;
    status: string;
    lastModified?: string;
}

// Internal record interface for database operations
interface RecordData {
    id: number;
    name: string;
    value: number;
    status: string;
    updatedAt?: Date;
}

@Injectable()
export class BatchDataService {
    private readonly logger = new Logger(BatchDataService.name);

    constructor(
        private readonly connectionPool: sql.ConnectionPool,
        private readonly httpService: HttpService
    ) { }

    /**
     * Update multiple records using stored procedure with table-valued parameter
     * Only updates existing records where isOpen = 1
     */
    async updateOpenRecords(records: RecordData[]): Promise<{
        updated: number;
        skipped: number;
    }> {
        if (!records || records.length === 0) {
            this.logger.warn('No records provided for update');
            return { updated: 0, skipped: 0 };
        }

        try {
            // Log the first few records for debugging
            this.logger.debug(`Updating ${records.length} records. Sample record:`, {
                id: records[0].id,
                name: records[0].name,
                value: records[0].value,
                status: records[0].status,
                updatedAt: records[0].updatedAt
            });

            const request = this.connectionPool.request();

            // Create table-valued parameter with exact column names and types
            const table = new sql.Table('RecordDataType');

            // Add columns - make sure these match your SQL table type exactly
            table.columns.add('Id', sql.Int, { nullable: false });
            table.columns.add('Name', sql.NVarChar(255), { nullable: false });
            table.columns.add('Value', sql.Decimal(10, 2), { nullable: false });
            table.columns.add('Status', sql.NVarChar(50), { nullable: false });
            table.columns.add('UpdatedAt', sql.DateTime2, { nullable: false });

            // Add rows to table parameter with proper data validation
            records.forEach((record, index) => {
                try {
                    // Ensure all values are properly formatted
                    const id = parseInt(record.id?.toString() || '0');
                    const name = (record.name || '').toString().trim();
                    const value = parseFloat(record.value?.toString() || '0');
                    const status = (record.status || '').toString().trim();
                    const updatedAt = record.updatedAt instanceof Date ? record.updatedAt : new Date();

                    // Validate before adding
                    if (id <= 0) {
                        throw new Error(`Invalid ID: ${record.id}`);
                    }
                    if (!name) {
                        throw new Error(`Empty name for record ID: ${id}`);
                    }
                    if (isNaN(value)) {
                        throw new Error(`Invalid value for record ID: ${id}`);
                    }
                    if (!status) {
                        throw new Error(`Empty status for record ID: ${id}`);
                    }

                    table.rows.add(id, name, value, status, updatedAt);
                } catch (error) {
                    this.logger.error(`Error processing record at index ${index}:`, error.message);
                    throw new Error(`Invalid data in record ${index + 1}: ${error.message}`);
                }
            });

            this.logger.debug(`Created table parameter with ${table.rows.length} rows`);

            // Execute stored procedure
            request.input('RecordData', table);

            this.logger.debug('Executing stored procedure sp_UpdateOpenRecords');
            const result = await request.execute('sp_UpdateOpenRecords');

            // Check if result has the expected structure
            if (!result.recordset || result.recordset.length === 0) {
                throw new Error('Stored procedure did not return expected results');
            }

            const stats = result.recordset[0];

            // Ensure the stats object has the expected properties
            const updatedCount = parseInt(stats.UpdatedCount?.toString() || '0');
            const skippedCount = parseInt(stats.SkippedCount?.toString() || '0');

            this.logger.log(`Batch update completed: ${updatedCount} updated, ${skippedCount} skipped`);

            return {
                updated: updatedCount,
                skipped: skippedCount
            };

        } catch (error) {
            this.logger.error('Error in batch update operation:', {
                message: error.message,
                code: error.code,
                number: error.number,
                state: error.state,
                class: error.class,
                serverName: error.serverName,
                procName: error.procName,
                lineNumber: error.lineNumber
            });

            // Provide more specific error messages
            if (error.message?.includes('RecordDataType')) {
                throw new Error(`Table type 'RecordDataType' not found. Please ensure it exists in your database. Original error: ${error.message}`);
            }

            if (error.message?.includes('sp_UpdateOpenRecords')) {
                throw new Error(`Stored procedure 'sp_UpdateOpenRecords' not found or has incorrect parameters. Original error: ${error.message}`);
            }

            throw new Error(`Failed to update records: ${error.message}`);
        }
    }

    /**
     * Verify database objects exist before attempting updates
     */
    async verifyDatabaseObjects(): Promise<{
        tableTypeExists: boolean;
        storedProcExists: boolean;
        issues: string[];
    }> {
        const issues: string[] = [];
        let tableTypeExists = false;
        let storedProcExists = false;

        try {
            const request = this.connectionPool.request();

            // Check if table type exists
            const tableTypeQuery = `
        SELECT COUNT(*) as count 
        FROM sys.table_types 
        WHERE name = 'RecordDataType'
      `;

            const tableTypeResult = await request.query(tableTypeQuery);
            tableTypeExists = tableTypeResult.recordset[0].count > 0;

            if (!tableTypeExists) {
                issues.push('User-defined table type "RecordDataType" does not exist');
            }

            // Check if stored procedure exists
            const spQuery = `
        SELECT COUNT(*) as count 
        FROM sys.procedures 
        WHERE name = 'sp_UpdateOpenRecords'
      `;

            const spResult = await request.query(spQuery);
            storedProcExists = spResult.recordset[0].count > 0;

            if (!storedProcExists) {
                issues.push('Stored procedure "sp_UpdateOpenRecords" does not exist');
            }

            // If stored procedure exists, check its parameters
            if (storedProcExists) {
                const paramQuery = `
          SELECT 
            p.name,
            t.name as type_name,
            p.max_length,
            p.is_readonly
          FROM sys.parameters p
          INNER JOIN sys.types t ON p.user_type_id = t.user_type_id
          WHERE p.object_id = OBJECT_ID('sp_UpdateOpenRecords')
        `;

                const paramResult = await request.query(paramQuery);
                const params = paramResult.recordset;

                const recordDataParam = params.find(p => p.name === '@RecordData');
                if (!recordDataParam) {
                    issues.push('Stored procedure missing @RecordData parameter');
                } else if (recordDataParam.type_name !== 'RecordDataType') {
                    issues.push(`@RecordData parameter type is ${recordDataParam.type_name}, expected RecordDataType`);
                }
            }

        } catch (error) {
            this.logger.error('Error verifying database objects:', error);
            issues.push(`Error during verification: ${error.message}`);
        }

        return {
            tableTypeExists,
            storedProcExists,
            issues
        };
    }

    /**
     * Fetch data from external API and update database records
     */
    async fetchAndUpdateRecords(apiUrl: string, apiHeaders?: Record<string, string>): Promise<{
        fetched: number;
        updated: number;
        skipped: number;
        errors: string[];
    }> {
        const errors: string[] = [];

        try {
            // First verify database objects exist
            const verification = await this.verifyDatabaseObjects();
            if (verification.issues.length > 0) {
                this.logger.error('Database verification failed:', verification.issues);
                errors.push(...verification.issues);
                return {
                    fetched: 0,
                    updated: 0,
                    skipped: 0,
                    errors
                };
            }

            this.logger.log(`Fetching data from external API: ${apiUrl}`);

            // Fetch data from external API
            const response = await firstValueFrom(
                this.httpService.get<ExternalApiRecord[]>(apiUrl, {
                    headers: apiHeaders || {},
                    timeout: 30000,
                })
            );

            const externalRecords = response.data;

            if (!Array.isArray(externalRecords)) {
                throw new HttpException('Invalid API response format', HttpStatus.BAD_REQUEST);
            }

            this.logger.log(`Fetched ${externalRecords.length} records from external API`);

            // Transform external API data to internal format
            const transformedRecords = this.transformExternalData(externalRecords);

            // Validate transformed data
            const validRecords = this.validateRecords(transformedRecords, errors);

            if (validRecords.length === 0) {
                this.logger.warn('No valid records to update');
                return {
                    fetched: externalRecords.length,
                    updated: 0,
                    skipped: externalRecords.length,
                    errors
                };
            }

            // Update database records
            const updateResult = await this.updateOpenRecords(validRecords);

            return {
                fetched: externalRecords.length,
                updated: updateResult.updated,
                skipped: updateResult.skipped + (externalRecords.length - validRecords.length),
                errors
            };

        } catch (error) {
            this.logger.error('Error fetching and updating records', error);

            if (error.code === 'ECONNABORTED') {
                throw new HttpException('API request timeout', HttpStatus.REQUEST_TIMEOUT);
            }

            if (error.response?.status) {
                throw new HttpException(
                    `External API error: ${error.response.status} - ${error.response.statusText}`,
                    HttpStatus.BAD_GATEWAY
                );
            }

            throw new HttpException('Failed to fetch and update records', HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Transform external API data to internal record format
     */
    private transformExternalData(externalRecords: ExternalApiRecord[]): RecordData[] {
        return externalRecords.map(record => ({
            id: record.id,
            name: record.name?.trim() || '',
            value: Number(record.value) || 0,
            status: record.status?.trim() || 'Unknown',
            updatedAt: record.lastModified ? new Date(record.lastModified) : new Date()
        }));
    }

    /**
     * Validate records and collect errors
     */
    private validateRecords(records: RecordData[], errors: string[]): RecordData[] {
        const validRecords: RecordData[] = [];

        records.forEach((record, index) => {
            const recordErrors: string[] = [];

            // Validate required fields
            if (!record.id || record.id <= 0) {
                recordErrors.push('Invalid or missing ID');
            }

            if (!record.name || record.name.length === 0) {
                recordErrors.push('Name is required');
            }

            if (record.name && record.name.length > 255) {
                recordErrors.push('Name exceeds maximum length (255 characters)');
            }

            if (isNaN(record.value)) {
                recordErrors.push('Invalid value format');
            }

            if (!record.status) {
                recordErrors.push('Status is required');
            }

            // Check for duplicate IDs within the batch
            const duplicateIndex = validRecords.findIndex(r => r.id === record.id);
            if (duplicateIndex !== -1) {
                recordErrors.push(`Duplicate ID found at index ${duplicateIndex}`);
            }

            if (recordErrors.length > 0) {
                errors.push(`Record ${index + 1} (ID: ${record.id}): ${recordErrors.join(', ')}`);
            } else {
                validRecords.push(record);
            }
        });

        return validRecords;
    }

    /**
     * Get open records with pagination
     */
    async getOpenRecords(page: number = 1, pageSize: number = 100): Promise<{
        data: RecordData[];
        total: number;
        page: number;
        pageSize: number;
    }> {
        try {
            const request = this.connectionPool.request();
            request.input('Page', sql.Int, page);
            request.input('PageSize', sql.Int, pageSize);

            const result = await request.execute('sp_GetOpenRecords');

            return {
                data: result.recordsets[0],
                total: result.recordsets[1][0].Total,
                page,
                pageSize
            };
        } catch (error) {
            this.logger.error('Error fetching open records', error);
            throw error;
        }
    }
}
