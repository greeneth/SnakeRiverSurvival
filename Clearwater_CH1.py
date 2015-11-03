#-------------------------------------------------------------------------------
# Clearwater_CH1_PIT_Survival.py
# Purpose: Automation of SQL queries and exporting of final detection data
# Author: Ethan Green, PNNL
# Date: October 2015
# Versions: MS SQL SERVER 2012 - Python 2.7
#-------------------------------------------------------------------------------

# Import libraries
import sys
import csv
import pyodbc
import time

# Create connection to database
conn = pyodbc.connect('TRUSTED_CONNECTION=YES;DRIVER={SQL Server Native client 11.0};SERVER=WE27751\SQLEXPRESS')

# Initiate a cursor
cursor = conn.cursor()

#Set variables
#inputFile = raw_input("Full path to the input file (ex. C:/Temp/infile.csv):  ")
#outfileInput = raw_input("Full path to the output file (ex. C:/Temp/outfile.csv):  ")


#Define SQL queries

#LGR Skippers
Above_LGRSkippersAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS last_LGS FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
						WHERE LGR = 0 AND Below_LGR = 1)
AND [Site Name] IN ('GOJ - Little Goose Dam Juvenile'/*,
					'LMJ - Lower Monumental Dam Juvenile',
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(last_LGS) = 4 AND DAY(last_LGS) BETWEEN 24 AND 31)
		OR (MONTH(last_LGS) = 5 AND DAY(last_LGS) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS])"""

Above_LGRPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_LGS FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
						WHERE LGR = 0 AND Below_LGR = 1)
AND [Site Name] IN ('GOJ - Little Goose Dam Juvenile'/*,
					'LMJ - Lower Monumental Dam Juvenile',
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.LGS_Pass_Date = c.last_LGS;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET LGR = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT [Tag Code]/*, MAX([Obs Time Value]) AS last_LGS*/ FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
													WHERE LGR = 0 AND Below_LGR = 1)
							AND [Site Name] IN ('GOJ - Little Goose Dam Juvenile'/*,
												'LMJ - Lower Monumental Dam Juvenile',
												'MCJ - McNary Dam Juvenile',
												'MCX - MCNARY JUVENILE EXPERIMENTAL'*/))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('GOJ - Little Goose Dam Juvenile')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LGS = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('GOJ - Little Goose Dam Juvenile')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
							WHERE Below_LGR = 1)) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LGS = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN ('LMJ - Lower Monumental Dam Juvenile',
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_LGS = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET Below_LGS = 0
WHERE LGS = 2

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET Below_LGS = 0
WHERE Below_LGS IS NULL"""

LGRSkippers_LMNAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS Last_LMN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
						WHERE Below_LGS = 1)
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN])
AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
					'LMJ - Lower Monumental Dam Juvenile'/*,
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(Last_LMN) = 4 AND DAY(Last_LMN) BETWEEN 24 AND 31)
		OR (MONTH(Last_LMN) = 5 AND DAY(Last_LMN) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN])"""

LGRSkippers_LMNPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_LMN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
						WHERE Below_LGS = 1)
		AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
							'LMJ - Lower Monumental Dam Juvenile'/*,
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.LMN_Pass_Date = c.last_LMN;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET LGS = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
						WHERE Below_LGS = 1)
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
												'LMJ - Lower Monumental Dam Juvenile'/*,
												'MCJ - McNary Dam Juvenile',
												'MCX - MCNARY JUVENILE EXPERIMENTAL'*/))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('LMJ - Lower Monumental Dam Juvenile')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LMN = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('LMJ - Lower Monumental Dam Juvenile')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LMN = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',*/
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_LMN = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Below_LMN = 0
WHERE LMN = 2

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Below_LMN = 0
WHERE Below_LMN IS NULL"""

LGRSkippers_MCNAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS Last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])
AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
					'LMJ - Lower Monumental Dam Juvenile',*/
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL')
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(Last_MCN) = 4 AND DAY(Last_MCN) BETWEEN 24 AND 31)
		OR (MONTH(Last_MCN) = 5 AND DAY(Last_MCN) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])"""

LGRSkippers_MCNPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
		AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
							'LMJ - Lower Monumental Dam Juvenile',*/
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.MCN_Pass_Date = c.last_MCN;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET LMN = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
												'LMJ - Lower Monumental Dam Juvenile',*/
												'MCJ - McNary Dam Juvenile',
												'MCX - MCNARY JUVENILE EXPERIMENTAL'))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',*/
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_MCN = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Below_MCN = 0
WHERE MCN = 2"""

#LGS Skippers
LGR_LGSSkippersAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS last_LMN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
						WHERE Below_LGR = 1)
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
						WHERE [Site Name] = 'GOJ - Little Goose Dam Juvenile')
AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
					'LMJ - Lower Monumental Dam Juvenile'/*,
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(last_LMN) = 4 AND DAY(last_LMN) BETWEEN 24 AND 31)
		OR (MONTH(last_LMN) = 5 AND DAY(last_LMN) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN])"""

LGR_LGSSkippersPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_LMN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
								WHERE Below_LGR = 1)
								AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
													'LMJ - Lower Monumental Dam Juvenile'/*,
													'MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL'*/)
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.LMN_Pass_Date = c.last_LMN;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];


UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET LGS = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
								WHERE Below_LGR = 1)
								AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',*/
													'LMJ - Lower Monumental Dam Juvenile'/*,
													'MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL'*/))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('LMJ - Lower Monumental Dam Juvenile')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LMN = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('LMJ - Lower Monumental Dam Juvenile')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]
							WHERE Below_LGR = 1)) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET LMN = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',*/
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_LMN = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Below_LMN = 0
WHERE LMN = 2

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Below_LMN = 0
WHERE Below_LMN IS NULL"""

LGSSkippers_MCNAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS Last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])
AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
					'LMJ - Lower Monumental Dam Juvenile',*/
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL')
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(Last_MCN) = 4 AND DAY(Last_MCN) BETWEEN 24 AND 31)
		OR (MONTH(Last_MCN) = 5 AND DAY(Last_MCN) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])"""

LGSSkippers_MCNPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
		AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
							'LMJ - Lower Monumental Dam Juvenile',*/
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.MCN_Pass_Date = c.last_MCN;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET LMN = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
						WHERE Below_LMN = 1)
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
												'LMJ - Lower Monumental Dam Juvenile',*/
												'MCJ - McNary Dam Juvenile',
												'MCX - MCNARY JUVENILE EXPERIMENTAL'))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',*/
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_MCN = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Below_MCN = 0
WHERE MCN = 2"""

LGSSkippers101AddIns = """WITH a AS (
SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE ([Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR]))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS])
AND ([Site Name] IN ('LMJ - Lower Monumental Dam Juvenile',
					'ICH - Ice Harbor Dam (Combined)',
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL',
					'JDJ - John Day Dam Juvenile',
					'B2J - Bonneville PH2 Juvenile',
					'B1J - BONNEVILLE PH1 JUVENILE',
					'BVX - Bonneville PH1 Juvenile (Exp.)',
					'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])))

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] ([Tag Code])
SELECT [Tag Code] FROM a

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
SET LGR = 1, LGS = 0, Below_LGS = 1
WHERE (LGR IS NULL
		AND LGS IS NULL
		AND Below_LGS IS NULL)"""

#LMN Skippers
LMN_MCNSkippersAddIn = """WITH a AS
(SELECT [Tag Code], MAX([Obs Time Value]) AS last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
						WHERE Below_LGS = 1
						AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]))
AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
					'LMJ - Lower Monumental Dam Juvenile',*/
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL')
GROUP BY [Tag Code])

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] ([Tag Code])
SELECT [Tag Code] FROM a
WHERE ((MONTH(last_MCN) = 4 AND DAY(last_MCN) BETWEEN 24 AND 31)
		OR (MONTH(last_MCN) = 5 AND DAY(last_MCN) BETWEEN 1 AND 25))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])"""


LMN_MCNSkippersPopulateNew = """UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING (SELECT [Tag Code], MAX([Obs Time Value]) AS last_MCN FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
								WHERE Below_LGS = 1)
		AND [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
							'LMJ - Lower Monumental Dam Juvenile',*/
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		GROUP BY [Tag Code]) AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.MCN_Pass_Date = c.last_MCN;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET LMN = 1

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT  DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
								WHERE Below_LGS = 1)
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
							WHERE [Site Name] IN (/*'GOJ - Little Goose Dam Juvenile',
													'LMJ - Lower Monumental Dam Juvenile',*/
													'MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL'))
		AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
								WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
													'MCX - MCNARY JUVENILE EXPERIMENTAL')
								AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%'))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 1;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE [Site Name] IN ('MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL')
		AND ([Antenna Group Name] LIKE '%SAMPLE%' OR [Antenna Group Name] LIKE '%RACEWAY%')
		AND [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]
							WHERE Below_LGS = 1)) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET MCN = 2;

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS a
USING (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
		WHERE ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',
							'ICH - Ice Harbor Dam (Combined)',
							'MCJ - McNary Dam Juvenile',
							'MCX - MCNARY JUVENILE EXPERIMENTAL',*/
							'JDJ - John Day Dam Juvenile',
							'B2J - Bonneville PH2 Juvenile',
							'B1J - BONNEVILLE PH1 JUVENILE',
							'BVX - Bonneville PH1 Juvenile (Exp.)',
							'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY]))) AS b
ON a.[Tag Code] = b.[Tag Code]
WHEN MATCHED THEN UPDATE
SET a.Below_MCN = 1;

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Below_MCN = 0
WHERE MCN = 2

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Below_MCN = 0
WHERE Below_MCN IS NULL"""

LMNSkippers101AddIn = """WITH b AS (
SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE ([Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR])
OR [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS]))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN])
AND ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',
					'ICH - Ice Harbor Dam (Combined)',*/
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL',
					'JDJ - John Day Dam Juvenile',
					'B2J - Bonneville PH2 Juvenile',
					'B1J - BONNEVILLE PH1 JUVENILE',
					'BVX - Bonneville PH1 Juvenile (Exp.)',
					'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])))

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] ([Tag Code])
SELECT [Tag Code] FROM b

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]
SET LGS = 1, LMN = 0, Below_LMN = 1
WHERE (LGS IS NULL
		AND LMN IS NULL
		AND Below_LMN IS NULL)"""

MCNSkippers101AddIn = """WITH c AS (
SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[CHK_Clean_Detections]
WHERE ([Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGR])
OR [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LGS])
OR [Tag Code] IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_LMN]))
AND [Tag Code] NOT IN (SELECT DISTINCT [Tag Code] FROM [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN])
AND ([Site Name] IN (/*'LMJ - Lower Monumental Dam Juvenile',
					'ICH - Ice Harbor Dam (Combined)',
					'MCJ - McNary Dam Juvenile',
					'MCX - MCNARY JUVENILE EXPERIMENTAL',*/
					'JDJ - John Day Dam Juvenile',
					'B2J - Bonneville PH2 Juvenile',
					'B1J - BONNEVILLE PH1 JUVENILE',
					'BVX - Bonneville PH1 Juvenile (Exp.)',
					'ESX - Estuary Saltwater Experiment')
		OR ([Site Name] LIKE '%adult%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])
		OR ([Site Name] LIKE '%ladder%' AND YEAR([Obs Time Value]) > [Migration Year YYYY])))

INSERT INTO [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] ([Tag Code])
SELECT [Tag Code] FROM c

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET Spp = 'Chinook'
WHERE Spp IS NULL

MERGE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN] AS b
USING [LGR_IHR_PSE_1].[dbo].[Tagging Data] AS c
ON b.[Tag Code] = c.[Tag Code]
WHEN MATCHED THEN UPDATE
SET b.Run_type = c.[Run Name], b.Release_Date = c.[Release Date MMDDYYYY];

UPDATE [LGR_IHR_PSE_3].[dbo].[Clearwater_CH1_MCN]
SET LMN = 1, MCN = 0, Below_MCN = 1
WHERE (LMN IS NULL
		AND MCN IS NULL
		AND Below_MCN IS NULL)"""

#Execute SQL queries in order
#Start execution timer
start_all = time.clock()

#LGR Skippers
print "Starting LGR Skippers..."
start_LGR = time.clock()
cursor.execute(Above_LGRSkippersAddIn)
cursor.execute(Above_LGRPopulateNew)
cursor.execute(LGRSkippers_LMNAddIn)
cursor.execute(LGRSkippers_LMNPopulateNew)
cursor.execute(LGRSkippers_MCNAddIn)
cursor.execute(LGRSkippers_MCNPopulateNew)
print "Done. Time elapsed: ",((time.clock() - start_LGR)/60)," minutes."

#LGS Skippers
print "Starting LGS Skippers..."
start_LGS = time.clock()
cursor.execute(LGR_LGSSkippersAddIn)
cursor.execute(LGR_LGSSkippersPopulateNew)
cursor.execute(LGSSkippers_MCNAddIn)
cursor.execute(LGSSkippers_MCNPopulateNew)
cursor.execute(LGSSkippers101AddIns)
print "Done. Time elapsed: ",((time.clock() - start_LGS)/60)," minutes."

#LMN Skippers
print "Starting LMN Skippers..."
start_LMN = time.clock()
cursor.execute(LMN_MCNSkippersAddIn)
cursor.execute(LMN_MCNSkippersPopulateNew)
cursor.execute(LMNSkippers101AddIn)
print "Done. Time elapsed: ",((time.clock() - start_LMN)/60)," minutes."

#MCN Skippers
print "Starting MCN Skippers..."
start_MCN = time.clock()
cursor.execute(MCNSkippers101AddIn)
print "Done. Time elapsed: ",((time.clock() - start_MCN)/60)," minutes."

#Cleanup
print "Cleaning up..."
conn.commit()
conn.close()

#End
print "Execution completed in ", ((time.clock() - start_all)/60), " minutes."