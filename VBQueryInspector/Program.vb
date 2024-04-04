Imports System.Data
Imports System.Data.SqlClient

Module Program
  Sub Main(args As String())
    Dim connectionString = "Server=.\SQLEXPRESS; Database=VBData; Trusted_Connection=True;"

    Dim SCHEMA_OWNER = New SqlParameter() With {
      .ParameterName = "@SchemaOwner",
      .Value = "dbo"
    }

    Dim TABLE_NAME = New SqlParameter() With {
      .ParameterName = "@TableName",
      .Value = "TestTable_People"
    }

    Dim COLUMN_NAME = New SqlParameter() With {
      .ParameterName = "@ColumnName",
      .Value = "LastName"
    }

    Dim CATALOG_NAME = New SqlParameter() With {
      .ParameterName = "@CatalogName",
      .Value = "TestCatalog"
    }

    Dim UNIQUE_INDEX = New SqlParameter() With {
      .ParameterName = "@UniqueIndex",
      .Value = "PK_People"
    }

    Dim CHANGE_TRACKING = New SqlParameter With {
      .ParameterName = "@ChangeTracking",
      .Value = "AUTO"
    }

    Dim NEW_COLUMN_TYPE = New SqlParameter With {
      .ParameterName = "@NewColumnType",
      .Value = "bit"
    }

    Using connection As New SqlConnection(connectionString)
      Dim cmd = New SqlCommand() With {
        .Connection = connection,
        .CommandType = CommandType.Text
      }

      connection.Open()
      EnsureTestTableCreated(cmd)
      SeedTestData(cmd)
      DropExistingFullTextCatalogs(cmd, connectionString)

      PrintDelimiter()
      WriteColoredLine("Start executing program logic...", ConsoleColor.Yellow)
      cmd.Parameters.Add(SCHEMA_OWNER)
      cmd.Parameters.Add(TABLE_NAME)
      cmd.Parameters.Add(COLUMN_NAME)
      cmd.Parameters.Add(CATALOG_NAME)
      cmd.Parameters.Add(UNIQUE_INDEX)
      cmd.Parameters.Add(CHANGE_TRACKING)
      cmd.Parameters.Add(NEW_COLUMN_TYPE)

      ' Create catalog logic
      WriteColoredLine($"Creating FULLTEXT catalog with name: '{CATALOG_NAME.Value}'...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'CREATE FULLTEXT CATALOG ' + QUOTENAME(@CatalogName);
                         EXEC sp_executesql @Sql, N'@CatalogName NVARCHAR(128)', @CatalogName"
      cmd.ExecuteNonQuery()
      WriteColoredLine($"Catalog '{CATALOG_NAME.Value}' is created successfully.", ConsoleColor.Green)

      ' Create fulltext index logic
      WriteColoredLine($"Creating FULLTEXT index...", ConsoleColor.Cyan)
      cmd.CommandText = $"DECLARE @Sql NVARCHAR(MAX);
                   SET @Sql = 'CREATE FULLTEXT INDEX ON {TABLE_NAME.Value}([FirstName]) KEY INDEX PK_People ON {CATALOG_NAME.Value} WITH CHANGE_TRACKING AUTO';
                   EXEC sp_executesql @Sql"
      cmd.ExecuteNonQuery()
      WriteColoredLine($"FULLTEXT index is created successfully.", ConsoleColor.Green)

      ' Add column in fulltext index logic
      WriteColoredLine("Adding column 'LastName' to fulltext index...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' ADD (' + QUOTENAME(@ColumnName) + ')';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128), @ColumnName NVARCHAR(128)', @TableName, @ColumnName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine($"Column 'LastName' is added successfully.", ConsoleColor.Green)

      ' Set change tracking = 'AUTO' logic
      WriteColoredLine("Setting change tracking value to be 'AUTO'...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' SET CHANGE_TRACKING AUTO';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128)', @TableName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Change tracking is set to 'AUTO' successfully.", ConsoleColor.Green)

      ' Enable fulltext index logic
      WriteColoredLine("Enabling fulltext index...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' ENABLE';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128)', @TableName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Fulltext index is enabled successfully.", ConsoleColor.Green)

      ' Start update population logic
      WriteColoredLine("Starting update population...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' START UPDATE POPULATION';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128)', @TableName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Update population is started successfully.", ConsoleColor.Green)

      ' Add column in fulltext index with schema in query logic
      WriteColoredLine("Adding column to fulltext index with schema in query...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                        SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@SchemaOwner) + '.' + QUOTENAME(@TableName) + ' ADD (' + QUOTENAME(@ColumnName) + ')';
                        EXEC sp_executesql @Sql, N'@SchemaOwner NVARCHAR(128), @TableName NVARCHAR(128), @ColumnName NVARCHAR(128)', @SchemaOwner, @TableName, @ColumnName;"
      WriteColoredLine($"Column is added successfully.", ConsoleColor.Green)

      ' Enable fulltext index with schema in query logic
      WriteColoredLine("Enabling fulltext index with schema in query...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                        SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@SchemaOwner) + '.' + QUOTENAME(@TableName) + ' ENABLE';
                        EXEC sp_executesql @Sql, N'@SchemaOwner NVARCHAR(128), @TableName NVARCHAR(128)', @SchemaOwner, @TableName"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Fulltext index is enabled successfully.", ConsoleColor.Green)

      ' Alter table logic
      COLUMN_NAME.Value = "Age"
      WriteColoredLine("Altering column 'Age'...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                        SET @Sql = 'ALTER TABLE ' + QUOTENAME(@TableName) + ' ALTER COLUMN ' + QUOTENAME(@ColumnName) + ' ' + QUOTENAME(@NewColumnType);
                        EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128), @ColumnName NVARCHAR(128), @NewColumnType NVARCHAR(128)', @TableName, @ColumnName, @NewColumnType;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Column 'Age' is altered successfully.", ConsoleColor.Green)

      ' Set change tracking = 'OFF' logic
      WriteColoredLine("Setting change tracking value to be 'OFF'...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' SET CHANGE_TRACKING OFF';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128)', @TableName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Change tracking is set to 'OFF' successfully.", ConsoleColor.Green)

      ' Disable fulltext index logic
      WriteColoredLine("Disabling fulltext index...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' DISABLE';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128)', @TableName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine("Fulltext index is disabled successfully.", ConsoleColor.Green)

      ' Drop column from fulltext index logic
      COLUMN_NAME.Value = "LastName"
      WriteColoredLine($"Dropping column '{COLUMN_NAME.Value}' from fulltext index...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                         SET @Sql = 'ALTER FULLTEXT INDEX ON ' + QUOTENAME(@TableName) + ' DROP (' + QUOTENAME(@ColumnName) + ') WITH NO POPULATION';
                         EXEC sp_executesql @Sql, N'@TableName NVARCHAR(128), @ColumnName NVARCHAR(128)', @TableName, @ColumnName;"
      cmd.ExecuteNonQuery()
      WriteColoredLine($"Column {COLUMN_NAME.Value} is dropped successfully.", ConsoleColor.Green)

      ' Drop fulltext index to test drop catalog logic. Catalog can not be dropped if contains fulltext index
      cmd.CommandText = $"DROP FULLTEXT INDEX ON {TABLE_NAME.Value}"
      cmd.ExecuteNonQuery()

      ' Dropping fulltext catalog logic
      WriteColoredLine($"Dropping catalog '{COLUMN_NAME.Value}'...", ConsoleColor.Cyan)
      cmd.CommandText = "DECLARE @Sql NVARCHAR(MAX);
                          SET @Sql = 'DROP FULLTEXT CATALOG ' + QUOTENAME(@CatalogName);
                          EXEC sp_executesql @Sql, N'@CatalogName NVARCHAR(128)', @CatalogName"
      cmd.ExecuteNonQuery()
      WriteColoredLine($"Fulltext catalog '{CATALOG_NAME.Value}' is dropped successfully.", ConsoleColor.Green)

      connection.Close()
    End Using
    Console.ForegroundColor = ConsoleColor.White
  End Sub

  Public Sub ShowConsoleInfo(executedSQL As String, parameters As IDataParameterCollection)
    Console.WriteLine($"Executed query: {executedSQL}")
    Console.Write("Parameters: ")
    For Each parameter In parameters
      Console.Write($"{CType(parameter, IDbDataParameter).ParameterName} = {CType(parameter, IDbDataParameter).Value}, ")
    Next
    Console.WriteLine("")
    Console.WriteLine("")
  End Sub

  Public Sub PrintRow(rowCounter As Integer, id As Integer, name As String, age As Integer)
    Console.WriteLine($"Row {rowCounter}: {id} {name} {age}")
  End Sub

  Public Sub PrintDelimiter()
    Console.ForegroundColor = ConsoleColor.White
    Console.WriteLine("")
    Console.WriteLine("--------------------------------------------")
    Console.WriteLine("")
  End Sub

  Public Sub DropTestTableIfExists(cmd As SqlCommand)
    WriteColoredLine("Deleting table 'TestTable_People'...", ConsoleColor.Cyan)
    cmd.CommandText = "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'TestTable_People'))
                       BEGIN
                         DROP TABLE TestTable_People
                       END"
    cmd.ExecuteNonQuery()
    WriteColoredLine("Table TestTable_People is deleted successfully.", ConsoleColor.Green)
  End Sub

  Public Sub CreateTestTable(cmd As SqlCommand)
    WriteColoredLine("Creating table 'TestTable_People'...", ConsoleColor.Cyan)
    cmd.CommandText = "CREATE TABLE TestTable_People
                       (
                         Id INT NOT NULL IDENTITY,
                         [FirstName] NVARCHAR(MAX),
                         [LastName] NVARCHAR(MAX),
                         [Age] INT,
                         CONSTRAINT PK_People PRIMARY KEY (Id)
                       )"
    cmd.ExecuteNonQuery()
    WriteColoredLine("Table 'TestTable_People' is created successfully.", ConsoleColor.Green)
  End Sub

  Public Sub DropExistingFullTextCatalogs(cmd As SqlCommand, connectionString As String)
    cmd.CommandText = "SELECT [name] FROM sys.fulltext_catalogs"
    Dim reader = cmd.ExecuteReader()

    While reader.Read()
      Dim catalogName = reader.GetValue(0)

      WriteColoredLine("Deleting existing fulltext catalogs...", ConsoleColor.Cyan)
      Using connection As New SqlConnection(connectionString)
        Dim deleteCatalogCommand = New SqlCommand With {
        .Connection = connection,
        .CommandType = CommandType.Text,
        .CommandText = $"DROP FULLTEXT CATALOG {catalogName}"
        }

        connection.Open()
        deleteCatalogCommand.ExecuteNonQuery()
        WriteColoredLine($"Fulltext catalog '{catalogName}' is deleted successfully", ConsoleColor.Yellow)
        connection.Close()
      End Using
    End While

    reader.Close()
  End Sub

  Public Sub EnsureTestTableCreated(cmd As SqlCommand)
    DropTestTableIfExists(cmd)
    CreateTestTable(cmd)
  End Sub

  Public Sub SeedTestData(cmd As SqlCommand)
    WriteColoredLine("Seeding test data into table 'TestTable_People'...", ConsoleColor.Cyan)
    cmd.CommandText = "INSERT INTO TestTable_People VALUES
                       ('Daniel', 'Dianov', 18),
                       ('Marian', 'Marinov', 19),
                       ('Joro', 'Paspalev', 20)"
    cmd.ExecuteNonQuery()
    WriteColoredLine("Table TestTable_People is seeded successfully.", ConsoleColor.Green)
  End Sub

  Public Sub WriteColoredLine(message As String, color As ConsoleColor)
    Console.ForegroundColor = color
    Console.WriteLine(message)
  End Sub
End Module
