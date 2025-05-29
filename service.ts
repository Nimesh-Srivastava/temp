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
  // Add other fields as per your external API structure
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
  ) {}

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
      this.logger.log(`Fetching data from external API: ${apiUrl}`);
      
      // Fetch data from external API
      const response = await firstValueFrom(
        this.httpService.get<ExternalApiRecord[]>(apiUrl, {
          headers: apiHeaders || {},
          timeout: 30000, // 30 second timeout
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
   * Process records from JSON array (for direct API calls to your endpoint)
   */
  async processJsonRecords(jsonRecords: ExternalApiRecord[]): Promise<{
    processed: number;
    updated: number;
    skipped: number;
    errors: string[];
  }> {
    const errors: string[] = [];
    
    try {
      if (!Array.isArray(jsonRecords)) {
        throw new HttpException('Invalid input format - expected array', HttpStatus.BAD_REQUEST);
      }

      this.logger.log(`Processing ${jsonRecords.length} records from JSON input`);

      // Transform and validate data
      const transformedRecords = this.transformExternalData(jsonRecords);
      const validRecords = this.validateRecords(transformedRecords, errors);
      
      if (validRecords.length === 0) {
        return {
          processed: jsonRecords.length,
          updated: 0,
          skipped: jsonRecords.length,
          errors
        };
      }

      // Update database records
      const updateResult = await this.updateOpenRecords(validRecords);
      
      return {
        processed: jsonRecords.length,
        updated: updateResult.updated,
        skipped: updateResult.skipped + (jsonRecords.length - validRecords.length),
        errors
      };

    } catch (error) {
      this.logger.error('Error processing JSON records', error);
      throw error;
    }
  }

  /**
   * Update multiple records using stored procedure with table-valued parameter
   * Only updates existing records where isOpen = 1
   */
  async updateOpenRecords(records: RecordData[]): Promise<{
    updated: number;
    skipped: number;
  }> {
    try {
      const request = this.connectionPool.request();

      // Create table-valued parameter
      const table = new sql.Table('RecordDataType');
      table.columns.add('Id', sql.Int, { nullable: false });
      table.columns.add('Name', sql.NVarChar(255), { nullable: false });
      table.columns.add('Value', sql.Decimal(10, 2), { nullable: false });
      table.columns.add('Status', sql.NVarChar(50), { nullable: false });
      table.columns.add('UpdatedAt', sql.DateTime2, { nullable: false });

      // Add rows to table parameter
      records.forEach(record => {
        table.rows.add(
          record.id,
          record.name,
          record.value,
          record.status,
          record.updatedAt || new Date()
        );
      });

      // Execute stored procedure
      request.input('RecordData', table);
      const result = await request.execute('sp_UpdateOpenRecords');

      const stats = result.recordset[0];
      
      this.logger.log(`Batch update completed: ${stats.UpdatedCount} updated, ${stats.SkippedCount} skipped (not open or not found)`);
      
      return {
        updated: stats.UpdatedCount,
        skipped: stats.SkippedCount
      };

    } catch (error) {
      this.logger.error('Error in batch update operation', error);
      throw new Error(`Failed to update records: ${error.message}`);
    }
  }

  /**
   * Alternative method using direct UPDATE with JOIN (without stored procedure)
   * Only updates records where isOpen = 1
   */
  async updateOpenRecordsDirect(records: RecordData[]): Promise<number> {
    const transaction = new sql.Transaction(this.connectionPool);
    
    try {
      await transaction.begin();
      
      const request = new sql.Request(transaction);
      
      // Create temporary table
      await request.query(`
        CREATE TABLE #TempRecords (
          Id INT,
          Name NVARCHAR(255),
          Value DECIMAL(10,2),
          Status NVARCHAR(50),
          UpdatedAt DATETIME2
        )
      `);

      // Insert data into temporary table in batches
      const batchSize = 1000;
      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize);
        const values = batch.map(record => 
          `(${record.id}, N'${record.name.replace(/'/g, "''")}', ${record.value}, N'${record.status}', '${(record.updatedAt || new Date()).toISOString()}')`
        ).join(',');

        await request.query(`
          INSERT INTO #TempRecords (Id, Name, Value, Status, UpdatedAt)
          VALUES ${values}
        `);
      }

      // Execute UPDATE operation only for records where isOpen = 1
      const updateResult = await request.query(`
        UPDATE target
        SET 
          Name = source.Name,
          Value = source.Value,
          Status = source.Status,
          UpdatedAt = source.UpdatedAt
        FROM YourTableName AS target
        INNER JOIN #TempRecords AS source ON target.Id = source.Id
        WHERE target.isOpen = 1
      `);

      await transaction.commit();
      
      const updatedRows = updateResult.rowsAffected[0] || 0;
      this.logger.log(`Direct update completed: ${updatedRows} rows updated`);
      
      return updatedRows;

    } catch (error) {
      await transaction.rollback();
      this.logger.error('Error in direct update operation', error);
      throw error;
    }
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

  /**
   * Scheduled method to fetch and update records automatically
   * Can be called by a cron job or scheduler
   */
  async scheduledUpdate(apiUrl: string, apiHeaders?: Record<string, string>): Promise<void> {
    try {
      this.logger.log('Starting scheduled update from external API');
      
      const result = await this.fetchAndUpdateRecords(apiUrl, apiHeaders);
      
      this.logger.log(`Scheduled update completed: 
        - Fetched: ${result.fetched}
        - Updated: ${result.updated}
        - Skipped: ${result.skipped}
        - Errors: ${result.errors.length}`);
        
      if (result.errors.length > 0) {
        this.logger.warn('Errors during scheduled update:', result.errors);
      }
      
    } catch (error) {
      this.logger.error('Scheduled update failed', error);
      throw error;
    }
  }
}

// Controller example
import { Controller, Post, Get, Body, Query, Headers } from '@nestjs/common';

@Controller('records')
export class RecordsController {
  constructor(private readonly batchDataService: BatchDataService) {}

  @Post('fetch-and-update')
  async fetchAndUpdateFromApi(
    @Body() body: { apiUrl: string; apiHeaders?: Record<string, string> }
  ) {
    const { apiUrl, apiHeaders } = body;
    return await this.batchDataService.fetchAndUpdateRecords(apiUrl, apiHeaders);
  }

  @Post('update-from-json')
  async updateFromJson(@Body() records: ExternalApiRecord[]) {
    return await this.batchDataService.processJsonRecords(records);
  }

  @Post('update-direct')
  async updateRecords(@Body() records: RecordData[]) {
    return await this.batchDataService.updateOpenRecords(records);
  }

  @Get()
  async getRecords(
    @Query('page') page: number = 1,
    @Query('pageSize') pageSize: number = 100
  ) {
    return await this.batchDataService.getRecords(page, pageSize);
  }

  @Get('open')
  async getOpenRecords(
    @Query('page') page: number = 1,
    @Query('pageSize') pageSize: number = 100
  ) {
    return await this.batchDataService.getOpenRecords(page, pageSize);
  }
}

// Module configuration
import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';

@Module({
  imports: [
    HttpModule.register({
      timeout: 30000,
      maxRedirects: 5,
    }),
  ],
  providers: [
    BatchDataService,
    {
      provide: 'DATABASE_CONNECTION',
      useFactory: async () => {
        const config: sql.config = {
          server: process.env.DB_SERVER,
          database: process.env.DB_DATABASE,
          user: process.env.DB_USER,
          password: process.env.DB_PASSWORD,
          options: {
            encrypt: true, // Required for Azure SQL
            trustServerCertificate: false,
            enableArithAbort: true,
          },
          pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 30000,
          },
          requestTimeout: 60000, // Increase timeout for batch operations
        };
        
        const pool = new sql.ConnectionPool(config);
        await pool.connect();
        return pool;
      },
    },
  ],
  controllers: [RecordsController],
  exports: [BatchDataService],
})
export class DatabaseModule {}
