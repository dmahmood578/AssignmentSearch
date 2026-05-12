-- Example SQL query for claim-level review on current schema
-- Run with: python queries.py < myquery2.sql

SELECT
    c."Patent Number",
    c."Input Identifier",
    c."Identifier Type",
    c."Claim Number",
    c."Is Dependent",
    c."Claim Text",
    t."Patent Title",
    t."Technology Field"
FROM patent_claims AS c
LEFT JOIN patent_text AS t ON c."Patent Number" = t."Patent Number"
WHERE CAST(c."Claim Number" AS INTEGER) IN (1, 2, 3)
ORDER BY c."Patent Number", CAST(c."Claim Number" AS INTEGER);