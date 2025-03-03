--разбиение на 30 минутные сессии
--Last Non-Direct Click (direct учитывается только если весь путь состоял из direct)
--окно атрибуции 90 дней
--GA4 traffic acqusition report https://tanelytics.com/ga4-bigquery-session-traffic_source/

with prepared_sessions
AS
(
	with
	sources_union
	AS
	(
		with non_direct_sessions
		AS
		(
			SELECT * EXCEPT(source_datetime)
				FROM (
					SELECT * 
					,count(DISTINCT session_source) OVER (PARTITION by appmetrica_device_id,session_datetime_minute) as source_datetime
					FROM (
						SELECT DISTINCT appmetrica_device_id
						,os_name
						,session_id,
						CASE
							WHEN utm_source like '%mail%' OR utm_medium like '%mail%' THEN 'Email'
							WHEN utm_source = 'sms' OR utm_medium = 'sms' OR path = '/s/' THEN 'Sms'
							WHEN utm_medium IN ('smm','sm','social','social-network','social-media','social network','social media') OR
							utm_source IN ('inst','tg','telegram','ig','telegram_socialmedia','vk','vkontakte','vkontakte_socialmedia','WhatsApp_socialmedia') 
							OR utm_source like '%socialmedia%'
							THEN 'Social'
							WHEN utm_medium like '_cp' OR utm_medium like 'cp_' OR utm_medium like 'paid%' OR utm_medium IN ('ppc','retargeting') OR utm_medium like 'cpm%'
							THEN 'Paid Search'
							WHEN utm_source like '%maps%' OR utm_source = '2gis' THEN 'Maps'
							WHEN ysclid is not null THEN 'Yandex Organic Search'
							ELSE 'Вход с нераспознанными метками'
							END AS session_source
						,session_date
						,session_datetime
						,session_timestamp
						,session_datetime_minute
						FROM (
							SELECT appmetrica_device_id
							,os_name
							,toStartOfInterval(event_datetime, INTERVAL 30 minute) as session_id --айди сессии как начало 30 минутного интервала
							,toDate(toDateTime(event_datetime,'UTC')) as session_date
							,event_datetime as session_datetime
							,event_timestamp as session_timestamp
							,toStartOfMinute(event_datetime) as session_datetime_minute
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.path')),'') as path
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.utm_source')),'') as utm_source
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.utm_medium')),'') as utm_medium
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.utm_campaign')),'') as utm_campaign
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.ysclid')),'') as ysclid
							,NULLIF(TRIM(JSON_VALUE(event_json, '$.parameters')),'') as parameters
							FROM appmetrica_events
							where event_name = 'WebLinkOpen'
							and event_date BETWEEN date_sub(quarter, 1, {{interval_from}}) and {{interval_to}}
						)
						UNION ALL
						SELECT DISTINCT appmetrica_device_id
						,os_name
						,toStartOfInterval(event_datetime, INTERVAL 30 minute) as session_id
						,'Push' as session_source
						,toDate(toDateTime(event_datetime,'UTC')) as session_date
						,event_datetime as session_datetime
						,event_timestamp as session_timestamp
						,toStartOfMinute(event_datetime) as session_datetime_minute --[1]
						FROM appmetrica_events
						where event_name = 'PushOpen'
						and event_date BETWEEN date_sub(quarter, 1, {{interval_from}}) and {{interval_to}}
					)
				)
				WHERE source_datetime = 1 OR session_source = 'Push' --убираем WebLinkOpen если ссылка была открыта в пуше
		)
		,direct_sessions
		AS
		(
			SELECT DISTINCT appmetrica_device_id
			,os_name
			,toStartOfInterval(session_start_datetime, INTERVAL 30 minute) as session_id
			,'Direct' as session_source
			,toDate(toDateTime(session_start_datetime,'UTC')) as session_date
			,session_start_datetime as session_datetime
			,session_start_timestamp as session_timestamp
			,toStartOfMinute(session_start_datetime) as session_datetime_minute
			FROM appmetrica_sessions
			where appmetrica_sessions.session_start_date between date_sub(quarter, 1, {{interval_from}}) and {{interval_to}}
		)
		SELECT DISTINCT * EXCEPT (session_datetime_minute,count_distinct_session_source_date)
		FROM (
			SELECT DISTINCT *
			,count(DISTINCT session_source) OVER (partition by appmetrica_device_id,session_datetime_minute) as count_distinct_session_source_date
			FROM 
			(
				SELECT DISTINCT * FROM non_direct_sessions
				UNION ALL
				SELECT DISTINCT * FROM direct_sessions
			)
		)
		WHERE count_distinct_session_source_date = 1 OR session_source != 'Direct' --исключаем Direct, если в той же минуте присутствуют события, т.к. session_start отправляется при старте любой сессии
	)
	,attributed_sessions
	AS
	(
		SELECT
		appmetrica_device_id,
		os_name,
		attributed_session_date,
		session_id,
		attributed_session_datetime,
		attributed_session_source,
		MAX(appmetrica_device_id) as distinct_field1 --техническое поле из-за особенностей при расчете DISTINCT с оконными функциями
		FROM (
			SELECT
		      *
		      ,IFNULL(ifnull(
		          session_first_traffic_source,
		          last_value(session_last_traffic_source) over(
		            partition by appmetrica_device_id
		            order by
		              attributed_session_timestamp asc range between 7776000 preceding
		              and 1 preceding 
		          )),'Direct') attributed_session_source --ищем Non-direct клик за последние 90 дней, иначе - Direct
	          FROM (
				SELECT
				appmetrica_device_id,
				os_name,
				session_date as attributed_session_date,
				session_id,
				attributed_session_datetime,
				attributed_session_timestamp,
				NULLIF(session_first_traffic_source,'Direct') as session_first_traffic_source,
				NULLIF(session_last_traffic_source,'Direct') as session_last_traffic_source,
				MAX(appmetrica_device_id) as distinct_field --техническое поле из-за особенностей при расчете DISTINCT с оконными функциями
				FROM (
					SELECT *,
					last_value(session_source) OVER (partition by appmetrica_device_id,session_id ORDER BY session_timestamp asc rows between unbounded preceding and unbounded following) as session_last_traffic_source
					FROM (
						SELECT
						appmetrica_device_id,
						os_name,
						session_date,
						session_id,
						session_timestamp,
						MIN(session_timestamp) OVER (partition by appmetrica_device_id,session_id)  as attributed_session_timestamp ,
						session_source,
						MIN(session_datetime) OVER (partition by appmetrica_device_id,session_id)  as attributed_session_datetime,
						first_value(session_source)  OVER (partition by appmetrica_device_id,session_id ORDER BY session_timestamp asc ) as session_first_traffic_source
				        FROM sources_union
					)
				)
				group by 1,2,3,4,5,6,7,8
			)
		)
		group by 1,2,3,4,5,6
	)
	,profiles --crm user_id из таблицы пользовательских профилей
	AS
	(
		SELECT
		DISTINCT
		appmetrica_device_id 
		,client_id
		FROM appmetrica_profiles
	)
	SELECT
	DISTINCT
	appmetrica_device_id
	,os_name
	,attributed_session_source
	,attributed_session_date
	,attributed_session_datetime
	,client_id
	FROM attributed_sessions
	LEFT JOIN profiles USING (appmetrica_device_id)
	SETTINGS join_use_nulls=1
)
,orders_crm --таблица с заказами
AS
(
	SELECT DISTINCT 
	crm_user_id
	,crm_transaction_id
	,transaction_date
	,transaction_created
	,order_revenue
	FROM orders
	WHERE transaction_date  between {{interval_from}} and {{interval_to}}
)
SELECT
attributed_session_source as "Источник трафика"
,count(DISTINCT appmetrica_device_id) as "Пользователи"
,COUNT(DISTINCT attributed_bitrix_order_id) as "Заказы"
,count(DISTINCT attributed_crm_user_id) / count(DISTINCT appmetrica_device_id) as "Конверсия"
,IFNULL(sum(attributed_bitrix_revenue),0) as "Выручка"
FROM (
	SELECT 
	DISTINCT
	attributed_session_source,
	appmetrica_device_id
	,attributed_crm_transaction_id
	,attributed_order_revenue
	,attributed_crm_user_id
	,count(DISTINCT appmetrica_device_id) OVER () as total_users
	FROM (
		SELECT DISTINCT * EXCEPT(sessions_cor_after_orders_join)
		FROM (
			SELECT
			DISTINCT 
			appmetrica_device_id,
			attributed_session_source,
			attributed_session_date,
			attributed_session_datetime,
			os_name,
			attributed_crm_transaction_id,
			attributed_order_revenue,
			attributed_crm_user_id,
			count() over (PARTITION by appmetrica_device_id,attributed_session_datetime) as sessions_cor_after_orders_join
			FROM (
				SELECT
				DISTINCT
				appmetrica_device_id,
				attributed_session_source,
				attributed_session_date,
				attributed_session_datetime,
				os_name,
				CASE WHEN order_session_diff = min_order_session_diff THEN crm_transaction_id END AS attributed_crm_transaction_id,
				CASE WHEN order_session_diff = min_order_session_diff THEN order_revenue END AS attributed_order_revenue,
				CASE WHEN order_session_diff = min_order_session_diff THEN crm_user_id END AS attributed_crm_user_id
				FROM (
					SELECT
					*,
					MIN(order_session_diff) OVER (PARTITION by client_id,crm_transaction_id)  AS min_order_session_diff --находим ближайший заказ к сессии
					FROM (
						SELECT DISTINCT *
						,CASE WHEN transaction_created > attributed_session_datetime THEN date_diff('second',attributed_session_datetime,transaction_created) END AS order_session_diff
						FROM prepared_sessions  LEFT JOIN orders
						ON prepared_sessions.client_id = orders.crm_user_id
						SETTINGS join_use_nulls=1
						)
				)
			)
		)
		WHERE sessions_cor_after_orders_join = 1 or attributed_bitrix_order_id IS NOT NULL --убираем дубли сессий после джоина заказов (несколько заказов в один день)
	)

)
group by 1,2






