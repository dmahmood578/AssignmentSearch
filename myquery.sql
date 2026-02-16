-- Example SQL query for patent assignment data
-- This query finds patents with specific criteria
-- Run with: python queries.py < myquery.sql

SELECT 
    "Patent Number",
    Inventors,
    Assignees,
    "Correspondent Address",
    "Attorney Name",
    "Attorney Address"
FROM all_assignments
WHERE 
    "Application Status" LIKE '%Patented Case%'
    AND ("Entity Status" = 'Micro' OR "Entity Status" = 'Small')
    AND Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'
