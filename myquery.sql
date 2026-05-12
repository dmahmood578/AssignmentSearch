-- Example SQL query for patent assignment + text summary data
-- Run with: python queries.py < myquery.sql

SELECT 
    a."Patent Number",
    t."Input Identifier",
    t."Identifier Type",
    a.Assignees,
    t."Patent Title",
    t."Technology Field",
    t."CPC Primary",
    t."Claim Count"
FROM all_assignments AS a
JOIN patent_text AS t ON a."Patent Number" = t."Patent Number"
WHERE a.Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'
  AND t."Technology Field" NOT LIKE 'Unavailable%'
ORDER BY a."Patent Number";
