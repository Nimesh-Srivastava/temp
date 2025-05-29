-- 1. Create User-Defined Table Type for batch operations
CREATE TYPE RecordDataType AS TABLE (
    Id INT NOT NULL,
    Name NVARCHAR(255) NOT NULL,
    Value DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(50) NOT NULL,
    UpdatedAt DATETIME2 NOT NULL
);
GO

-- 2. Create the main table (adjust columns as needed)
CREATE TABLE YourTableName (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(255) NOT NULL,
    Value DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(50) NOT NULL,
    isOpen BIT NOT NULL DEFAULT 1, -- Added isOpen flag
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Create indexes for better performance
CREATE INDEX IX_YourTableName_Status ON YourTableName(Status);
CREATE INDEX IX_YourTableName_UpdatedAt ON YourTableName(UpdatedAt);
CREATE INDEX IX_YourTableName_IsOpen ON YourTableName(isOpen); -- Index for isOpen filtering
GO

-- 3. Main stored procedure for batch updates (only updates, no inserts/deletes)
-- Only updates records where isOpen = 1
CREATE PROCEDURE sp_UpdateOpenRecords
    @RecordData RecordDataType READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @UpdatedCount INT = 0;
    DECLARE @SkippedCount INT = 0;
    DECLARE @TotalInputRecords INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Get total input records count
        SELECT @TotalInputRecords = COUNT(*) FROM @RecordData;
        
        -- Update only existing records where isOpen = 1
        UPDATE target
        SET 
            Name = source.Name,
            Value = source.Value,
            Status = source.Status,
            UpdatedAt = source.UpdatedAt
        FROM YourTableName AS target
        INNER JOIN @RecordData AS source ON target.Id = source.Id
        WHERE target.isOpen = 1;
        
        SET @UpdatedCount = @@ROWCOUNT;
        SET @SkippedCount = @TotalInputRecords - @UpdatedCount;
        
        COMMIT TRANSACTION;
        
        -- Return operation statistics
        SELECT 
            @UpdatedCount AS UpdatedCount,
            @SkippedCount AS SkippedCount,
            @TotalInputRecords AS TotalInputRecords;
            
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        THROW;
    END CATCH
END;
GO

-- 4. Stored procedure for paginated reading
CREATE PROCEDURE sp_GetRecordsPaged
    @Page INT = 1,
    @PageSize INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Offset INT = (@Page - 1) * @PageSize;
    
    -- Get paginated data
    SELECT 
        Id,
        Name,
        Value,
        Status,
        CreatedAt,
        UpdatedAt
    FROM YourTableName
    ORDER BY Id
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY;
    
    -- Get total count
    SELECT COUNT(*) AS Total FROM YourTableName;
END;
GO

-- 5. Get open records only
CREATE PROCEDURE sp_GetOpenRecords
    @Page INT = 1,
    @PageSize INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Offset INT = (@Page - 1) * @PageSize;
    
    -- Get paginated data for open records only
    SELECT 
        Id,
        Name,
        Value,
        Status,
        isOpen,
        CreatedAt,
        UpdatedAt
    FROM YourTableName
    WHERE isOpen = 1
    ORDER BY Id
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY;
    
    -- Get total count of open records
    SELECT COUNT(*) AS Total FROM YourTableName WHERE isOpen = 1;
END;
GO

-- 6. Procedure to check which records can be updated (isOpen = 1)
CREATE PROCEDURE sp_GetUpdatableRecords
    @RecordIds NVARCHAR(MAX) -- Comma-separated list of IDs
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Convert comma-separated string to table
    WITH RecordIdList AS (
        SELECT CAST(value AS INT) AS Id
        FROM STRING_SPLIT(@RecordIds, ',')
        WHERE RTRIM(value) <> ''
    )
    SELECT 
        r.Id,
        r.Name,
        r.Value,
        r.Status,
        r.isOpen,
        CASE 
            WHEN r.isOpen = 1 THEN 'Can Update'
            WHEN r.isOpen = 0 THEN 'Cannot Update - Record Closed'
            ELSE 'Record Not Found'
        END AS UpdateStatus
    FROM RecordIdList rl
    LEFT JOIN YourTableName r ON rl.Id = r.Id
    ORDER BY r.Id;
END;
GO

-- 7. Procedure to get records by status for filtering
CREATE PROCEDURE sp_GetRecordsByStatus
    @Status NVARCHAR(50),
    @Page INT = 1,
    @PageSize INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Offset INT = (@Page - 1) * @PageSize;
    
    SELECT 
        Id,
        Name,
        Value,
        Status,
        CreatedAt,
        UpdatedAt
    FROM YourTableName
    WHERE Status = @Status
    ORDER BY UpdatedAt DESC
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY;
    
    SELECT COUNT(*) AS Total 
    FROM YourTableName 
    WHERE Status = @Status;
END;
GO

-- 8. Performance monitoring view
CREATE VIEW vw_RecordStats AS
SELECT 
    Status,
    COUNT(*) AS RecordCount,
    AVG(Value) AS AvgValue,
    MIN(UpdatedAt) AS OldestUpdate,
    MAX(UpdatedAt) AS LatestUpdate
FROM YourTableName
GROUP BY Status;
GO

-- Sample usage and testing
/*
-- Test the table type with updates only
DECLARE @testData RecordDataType;
INSERT INTO @testData VALUES 
    (1, 'Updated Record 1', 150.75, 'Active', GETUTCDATE()),
    (2, 'Updated Record 2', 250.25, 'Pending', GETUTCDATE());

-- Execute the update procedure (only updates records where isOpen = 1)
EXEC sp_UpdateOpenRecords @RecordData = @testData;

-- Check which records can be updated
EXEC sp_GetUpdatableRecords @RecordIds = '1,2,3,4,5';

-- Get open records only
EXEC sp_GetOpenRecords @Page = 1, @PageSize = 10;

-- Set a record to closed (for testing)
UPDATE YourTableName SET isOpen = 0 WHERE Id = 2;
*/
