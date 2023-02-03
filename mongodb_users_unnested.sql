WITH
	t AS (
		SELECT
			created_at,
			referral_code,
			is_active,
			isEmployee,
			stripe,
			therapists,
			first_name,
			number_of_subscriptions,
			id,
			updated_at,
			v,
			last_name,
			emitted_at,
			partners,
			is_paused,
			events_log,
			is_finished,
			email,
			phoneNumber,
			subscriptionService,
			bookings,
			role,
			normalized_at
			JSON_QUERY(credit, '$.date') AS date,
			JSON_QUERY(credit, '$.add') AS add,
			JSON_QUERY(credit, '$.amount') AS amount,
			JSON_QUERY(credit, '$.description') AS description,
			JSON_QUERY(credit, '$.type') AS type
		FROM {{ ref('mongodb_users_latest') }},
		UNNEST(JSON_EXTRACT_ARRAY(credits_log)) as credit)
SELECT * FROM t