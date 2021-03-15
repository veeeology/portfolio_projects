CREATE PROCEDURE [dbo].[sp_create_pivoted_view]
	@src_table NVARCHAR(255)
	,@val_col NVARCHAR(255)
	,@category_col NVARCHAR(255)
	,@axis_col NVARCHAR(255)
	,@static_table BIT

/* ============================================================================
STORED PROCEDURE: sp_create_pivoted_view
 
DESCRIPTION:  Given a table with a categories to be used as columns in a pivot
table, automatically constructs a view that contains the correct column names.
For example, if I have a table with 3 columns: [axis_col], [category_col],
and [value_col], and three values for [category col] ('group1', 'group2', group3')
it would construct this query and create a view.  This view will be updated if
the categories present in the data change; it does not require the user to know
ahead of time which categories are present.  If the @static_table=1 option is
selected, it will create a table and populate it with the results of the pivoted
view.

SELECT * FROM
(
       SELECT [axis_col]
              ,[category_col]
              ,[value_col]
       FROM [src_table]
) src_table
 
PIVOT
(      AVG([value])
       FOR [category_col] IN
       ([group1], [group2], group3])
) pivoted
 
Note: This current implementation assumes that we are doing all operations
within the dbo schema - if needed, an additional input variable can be added
to specify a different one.  Also, this assumes that you will want to aggregate
using AVG - again, it would be relatively easy to add a @aggmethod input to 
allow for other aggregation methods to be specified.
===============================================================================*/

AS
BEGIN
	DECLARE @pivoted_view NVARCHAR(255)
	-- Create the name for the pivoted view

	IF LEFT(@src_table, 3) = 'VW_'
		SET @pivoted_view = @src_table + '_pivoted'
	ELSE
		SET @pivoted_view = 'VW_' + @src_table + '_pivoted'

	-- Create the list by creating a temporary view to get the distinct values
	-- in the category column
	DECLARE @list NVARCHAR(MAX)
	DECLARE @createview NVARCHAR(MAX) 

	-- If the temporary view already exists, drop it
	IF 'tmp_attr_list' in (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables)
		DROP VIEW [dbo].[tmp_attr_list]

	-- Create the temporary view
	SET @createview = 'CREATE VIEW [dbo].[tmp_attr_list] AS WITH tmp AS(SELECT DISTINCT ['
					+ @category_col + '] as [col], 1 as [grp] FROM [' + @src_table
					+ ']) SELECT STRING_AGG([col], ''],['') as [list] FROM [tmp] GROUP BY [grp]'
	EXEC(@createview)

	-- Retrieve the value from the the temporary view
	SET @list = '[' + (SELECT TOP 1 [list] FROM [dbo].[tmp_attr_list]) + ']'

	-- Clean up by removing the temporary view
	DROP VIEW [dbo].[tmp_attr_list]

	-- Construct the pivot statement
	DECLARE @statement NVARCHAR(MAX)
	SET @statement = N'SELECT * FROM (SELECT [' + @category_col + '], [' + @axis_col + '], [' + @val_col + '] FROM [' + @src_table + ']) src_table'
	SET @statement = @statement + ' PIVOT (AVG([' + @val_col + ']) FOR [' + @category_col + '] IN (' + @list + ')) pivot_table'
	SET @statement = 'CREATE VIEW [' + @pivoted_view + '] AS ' + @statement
	IF @pivoted_view in (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables)
		BEGIN
		DECLARE @stmt NVARCHAR(MAX)
		SET @stmt = 'DROP VIEW ' + @pivoted_view
		EXEC(@stmt)
		END

	EXEC(@statement)

	-- IF the option to create the static table is selected, create the table and insert the data
	IF @static_table = 1
	BEGIN
		DECLARE @tbl_name NVARCHAR(MAX)
		DECLARE @createtbl NVARCHAR(MAX)
		DECLARE @axistype NVARCHAR(50)
		DECLARE @valuetype NVARCHAR(50)
		DECLARE @createlist NVARCHAR(MAX)
		DECLARE @insertinto NVARCHAR(MAX)

		-- IF the table already exists, drop it to ensure that the schema
		-- is consistent with the current view
		SET @tbl_name = REPLACE(@pivoted_view, 'VW_', '')
		IF @tbl_name IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES)
		BEGIN
			DECLARE @droptext NVARCHAR(500)
			SET @droptext = 'DROP TABLE [' + @tbl_name + ']'
			EXEC(@droptext)
		END

		-- Get the types from the source table to construct the create table statement
		SET @axistype = (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @src_table AND COLUMN_NAME = @axis_col)
		SET @valuetype = (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @src_table AND COLUMN_NAME = @val_col)

		-- Construct the create table statement
		SET @createtbl = 'CREATE TABLE [' + @tbl_name + '] ([' + @axis_col + '] [' + @axistype + '], '
		SET @createlist = REPLACE(@list, ']', '] [' + @valuetype + ']')
		SET @createtbl = @createtbl + @createlist + ', PRIMARY KEY ([' + @axis_col + ']))'
		EXEC(@createtbl)

		-- Populate the table with the output of the view
		SET @insertinto = 'INSERT INTO [' + @tbl_name + '] SELECT * FROM [' + @pivoted_view + ']'
		EXEC(@insertinto)

	END
END
GO