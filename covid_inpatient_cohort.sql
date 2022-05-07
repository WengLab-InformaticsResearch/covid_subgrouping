CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

-- Codeset 0: SARS-CoV-2 conditions
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from dbo.CONCEPT where concept_id in (37311061)
UNION  select c.concept_id
  from dbo.CONCEPT c
  join dbo.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (37311061)
  and c.invalid_reason is null

) I
) C;

-- Codeset 1: 2019 novel coronavirus detected (measurement)
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from dbo.CONCEPT where concept_id in (37310282)
UNION  select c.concept_id
  from dbo.CONCEPT c
  join dbo.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (37310282)
  and c.invalid_reason is null

) I
) C;

-- Codeset 2: SARS-CoV-2 measurements
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from dbo.CONCEPT where concept_id in (756055)
UNION  select c.concept_id
  from dbo.CONCEPT c
  join dbo.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (756055)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from dbo.CONCEPT where concept_id in (37310282,37310281,37310258)
UNION  select c.concept_id
  from dbo.CONCEPT c
  join dbo.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (37310282,37310281,37310258)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C;

-- Codeset 3: "Other coronavirus as the cause of diseases classified elsewhere" (condition)
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from dbo.CONCEPT where concept_id in (710158,710155,710156,710159,710160,45756093,42501115,586414,45600471,586415)

) I
LEFT JOIN
(
  select concept_id from dbo.CONCEPT where concept_id in (45542411,710157)

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C;

-- Codeset 4: Inpatient Visit
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from dbo.CONCEPT where concept_id in (262,9201)
UNION  select c.concept_id
  from dbo.CONCEPT c
  join dbo.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (262,9201)
  and c.invalid_reason is null

) I
) C;


with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
	-- Begin Primary Events
	select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
	FROM
	(
	  select E.person_id, E.start_date, E.end_date,
			 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
			 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
	  FROM 
	  (
		-- Begin Visit Occurrence Criteria: all inpatient visits starting after 2020-03-01
		select C.person_id, C.visit_occurrence_id as event_id, C.visit_start_date as start_date, C.visit_end_date as end_date,
			   C.visit_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.visit_start_date as sort_date
		from 
			(
				-- All inpatient visits
			  select vo.* 
			  FROM dbo.VISIT_OCCURRENCE vo
			JOIN #Codesets codesets on ((vo.visit_concept_id = codesets.concept_id and codesets.codeset_id = 4))
			) C

		WHERE C.visit_start_date > DATEFROMPARTS(2020, 03, 01)
		-- End Visit Occurrence Criteria

	  ) E
	  -- Option to require obsevation period prior to visit start date (in this case, 0 days required)
	  JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
	  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
	) P

	-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM primary_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
	SELECT 0 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
		-- Begin Condition Occurrence Criteria
		SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
			   C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.condition_start_date as sort_date
		FROM 
		(
			  SELECT co.* 
			  FROM dbo.CONDITION_OCCURRENCE co
			  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 0))
		) C
		-- End Condition Occurrence Criteria

	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

	UNION ALL
	-- Begin Correlated Criteria
	SELECT 1 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
	    -- Begin Condition Occurrence Criteria
		SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
			   C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.condition_start_date as sort_date
		FROM 
		(
			  SELECT co.* 
			  FROM dbo.CONDITION_OCCURRENCE co
			  JOIN #Codesets codesets on ((co.condition_source_concept_id = codesets.concept_id and codesets.codeset_id = 3))
		) C
		-- End Condition Occurrence Criteria

	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

	UNION ALL

	-- Begin Correlated Criteria
	SELECT 2 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
		-- Begin Measurement Criteria
		select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
			   C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.measurement_date as sort_date
		from 
		(
		  select m.* 
		  FROM dbo.MEASUREMENT m
		JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 1))
		) C
		-- End Measurement Criteria
	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

	UNION ALL
	-- Begin Correlated Criteria
	SELECT 3 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
	    -- Begin Measurement Criteria
		select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
			   C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.measurement_date as sort_date
		from 
		(
		  select m.* 
		  FROM dbo.MEASUREMENT m
		JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 2))
		) C

		WHERE C.value_as_concept_id in (4126681,45877985,9191,4181412,45879438,45884084)
		-- End Measurement Criteria

	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

	UNION ALL

	-- Begin Correlated Criteria
	SELECT 4 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
		-- Begin Observation Criteria
		select C.person_id, C.observation_id as event_id, C.observation_date as start_date, DATEADD(d,1,C.observation_date) as END_DATE,
			   C.observation_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.observation_date as sort_date
		from 
		(
		  select o.* 
		  FROM dbo.OBSERVATION o
		JOIN #Codesets codesets on ((o.observation_concept_id = codesets.concept_id and codesets.codeset_id = 2))
		) C

		WHERE C.value_as_concept_id in (4126681,45877985,9191,45884084,4181412,45879438)
		-- End Observation Criteria

	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

	UNION ALL
	-- Begin Correlated Criteria
	SELECT 5 as index_id, p.person_id, p.event_id
	FROM primary_events P
	INNER JOIN
	(
		-- Begin Observation Criteria
		select C.person_id, C.observation_id as event_id, C.observation_date as start_date, DATEADD(d,1,C.observation_date) as END_DATE,
			   C.observation_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
			   C.observation_date as sort_date
		from 
		(
		  select o.* 
		  FROM dbo.OBSERVATION o
		JOIN #Codesets codesets on ((o.observation_source_concept_id = codesets.concept_id and codesets.codeset_id = 3))
		) C
		-- End Observation Criteria

	) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-21,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.END_DATE)
	GROUP BY p.person_id, p.event_id
	HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
	-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id

) QE

;


-- Save #qualified_events 
SELECT * 
INTO covid19_table_name
FROM #qualified_events;