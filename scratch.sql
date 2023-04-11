SELECT e."index" , e.eventDate, e.id  FROM events e -- where e.id = 124574834675218693;

--SELECT count(*)  FROM events e WHERE e.eventDate >  1680094087000;

SELECT count(*) FROM events e WHERE e.eventDate>1681166545001

DROP table events ;

PRAGMA table_info(events);


SELECT count(*) FROM events e ;

SELECT * FROM act_codes ac ;

SELECT * FROM users u;

SELECT * FROM workspaces w ;

SELECT * FROM models m ;

SELECT * from actions a ;

SELECT * FROM cloudworks c ;

SELECT * FROM files f ;

SELECT 
	printf("%.0f", e.eventDate / 1000) || printf("%09d", e."index"  ) as LOAD_ID ,
	e.id as AUDIT_ID , 
	datetime(e.eventDate/1000 , 'unixepoch') as EVENT_DATE ,
	e.eventTimeZone as EVENT_TIMEZONE , 
	datetime(e.createdDate/1000 , 'unixepoch') as CREATED_DATE ,
	e.createdTimeZone as CREATE_TIMEZONE , 
	e.eventTypeId as EVENT_ID , 
	ac.[Event Message] as EVENT_MESSAGE , 
	ac.[Associated Object Id] as ASSOCIATED_OBJECT_ID, 
	ac.Notes as NOTES , 
	e.userId as USER_ID , 
	u.userName as USER_NAME , 
	u.displayName as DISPLAY_NAME , 
	e.tenantId as TENANT_ID , 
	"{{tenant_name}}" as TENANT_NAME , 
	e."additionalAttributes.workspaceId" as WORKSPACE_ID , 
	w.name as WORKSPACE_NAME ,
	CASE 
		WHEN e."additionalAttributes.modelId" IS NOT NULL THEN e."additionalAttributes.modelId"
		WHEN e.objectId = cw.integrationId THEN cw.modelId
	END as MODEL_ID ,
	CASE 
		WHEN e."additionalAttributes.modelId" IS NOT NULL THEN m.name
		WHEN e.objectId = cw.integrationId THEN (SELECT  m3.name FROM cloudworks c INNER JOIN models m3 ON c.modelId = m3.id)  
	END as MODEL_NAME ,
	e.objectId as OBJECT_ID ,
	CASE 
		WHEN e.objectId = m2.id THEN "Model"
		WHEN e.objectId = cw.integrationId THEN "CloudWorks Integration"
		WHEN e.objectId = u2.id  THEN "User"
	END as OBJECT_TYPE ,
	CASE 
		WHEN e.objectId = m2.id THEN m2.name
		WHEN e.objectId = cw.integrationId THEN cw.name 
		WHEN e.objectId = u2.id  THEN u2.userName 
	END as OBJECT_NAME ,
	e.message as MESSAGE , 
	e.success as SUCCESS, 
	e.errorNumber as ERROR_NUMBER , 
	e.ipAddress as IP_ADDRESS , 
	e.userAgent as USER_AGENT , 
	e.sessionId as SESSION_ID , 
	e.hostName as HOST_NAME , 
	e.serviceVersion as SERVICE_VERSION , 
	e.objectTypeId as OBJECT_TYPE_ID , 
	e.objectTenantId as OBJECT_TENANT_ID , 
	e."additionalAttributes.actionId" AS ACTION_ID ,
	CASE 
		WHEN e."additionalAttributes.actionId" IS "-1" THEN "Unsaved Action" 
		WHEN a.name IS NULL AND e."additionalAttributes.actionId" IS NOT NULL THEN "<Object has been Deleted>"
		ELSE a.name 
	END AS ACTION_NAME ,
	e."additionalAttributes.name" as ADDITIONAL_ATTRIBUTES_NAME , 
	e."additionalAttributes.type" as ADDITIONAL_ATTRIBUTES_TYPE , 
	e."additionalAttributes.auth_id" as ADDITIONAL_ATTRIBUTES_AUTH_ID ,
	e."additionalAttributes.modelRoleName" as MODEL_ROLE_NAME ,
	e."additionalAttributes.modelRoleId" as MODEL_ROLE_ID ,
	e."additionalAttributes.objectTypeId" as ADDITIONAL_ATTRIBUTES_OBJECT_TYPE_ID , 
	e."additionalAttributes.roleId" as ADDITIONAL_ATTRIBUTES_ROLE_ID , 
	e."additionalAttributes.roleName" as ADDITIONAL_ATTRIBUTES_ROLE_NAME ,
	e."additionalAttributes.objectTenantId" as ADDITIONAL_ATTRIBUTES_OBJECT_TENANT_ID ,
	e."additionalAttributes.objectId" as ADDITIONAL_ATTRIBUTES_OBJECT_ID ,
	e."additionalAttributes.active" as ADDITIONAL_ATTRIBUTES_ACTIVE ,
	e.checksum as CHECKSUM
FROM events e 
LEFT JOIN users u ON e.userId = u.id
LEFT JOIN users u2 ON e.objectId = u2.id 
LEFT JOIN workspaces w ON e."additionalAttributes.workspaceId" = w.id 
LEFT JOIN models m ON e."additionalAttributes.modelId" = m.id 
LEFT JOIN models m2 ON e.objectId = m2.id
LEFT JOIN cloudworks cw on e.objectId = cw.integrationId 
LEFT JOIN act_codes ac on e.eventTypeId = ac.[Event Code]
LEFT JOIN actions a on e."additionalAttributes.actionId" || e.objectId  = a.id || a.model_id ;
WHERE e.eventDate > 1680000031000
LIMIT 15000 OFFSET 0;

