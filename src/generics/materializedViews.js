'use strict'
const crypto = require('crypto')
const entityTypeQueries = require('@database/queries/entityType')
const schedulerRequests = require('@requests/scheduler')
const { sequelize } = require('@database/models/index')
const models = require('@database/models/index')
const { Op } = require('sequelize')
const utils = require('@generics/utils')
const searchConfig = require('@configs/search.json')
const getIndexQueries = require('@generics/mViewsIndexQueries')
const { getDefaults } = require('@helpers/getDefaultOrgId')
const { elevateLog } = require('elevate-logger')
const logger = elevateLog.init()
const responses = require('@helpers/responses')
const httpStatusCode = require('@generics/http-status')
const userExtensionQueries = require('@database/queries/userExtension')

let refreshInterval

const ensureTransformJsonbToTextArrayFunction = async () => {
	const createFunctionSQL = `
		CREATE OR REPLACE FUNCTION public.transform_jsonb_to_text_array(input_jsonb jsonb)
		RETURNS text[]
		AS $$
		DECLARE
			result text[] := ARRAY[]::text[];
			element text;
		BEGIN
			IF input_jsonb IS NULL THEN
				RETURN result;
			ELSIF jsonb_typeof(input_jsonb) = 'object' THEN
				FOR element IN SELECT jsonb_object_keys(input_jsonb)
				LOOP
					result := array_append(result, element);
				END LOOP;
			ELSIF jsonb_typeof(input_jsonb) = 'array' THEN
				FOR element IN SELECT jsonb_array_elements_text(input_jsonb)
				LOOP
					result := array_append(result, element);
				END LOOP;
			END IF;
			RETURN result;
		END;
		$$ LANGUAGE plpgsql;
	`

	try {
		await sequelize.query(createFunctionSQL)
		logger.info('Ensured public.transform_jsonb_to_text_array exists')
	} catch (error) {
		logger.error(`Error ensuring transform_jsonb_to_text_array function: ${error.message}`)
		throw error
	}
}

const groupByModelNames = async (entityTypes) => {
	const groupedData = new Map()
	entityTypes.forEach((item) => {
		item.model_names.forEach((modelName) => {
			if (groupedData.has(modelName)) {
				groupedData.get(modelName).entityTypes.push(item)
				groupedData.get(modelName).entityTypeValueList.push(item.value)
			} else
				groupedData.set(modelName, {
					modelName: modelName,
					entityTypes: [item],
					entityTypeValueList: [item.value],
				})
		})
	})

	return [...groupedData.values()]
}

const filterConcreteAndMetaAttributes = async (modelAttributes, attributesList) => {
	try {
		const concreteAttributes = []
		const metaAttributes = []
		attributesList.forEach((attribute) => {
			if (modelAttributes.includes(attribute)) concreteAttributes.push(attribute)
			else metaAttributes.push(attribute)
		})
		return { concreteAttributes, metaAttributes }
	} catch (err) {
		console.log(err)
	}
}

const rawAttributesTypeModifier = async (rawAttributes) => {
	try {
		const outputArray = []
		for (const key in rawAttributes) {
			const columnInfo = rawAttributes[key]
			const type = columnInfo.type.key
			const subField = columnInfo.type.options?.type?.key
			const typeMap = {
				ARRAY: {
					JSON: 'json[]',
					STRING: 'character varying[]',
					INTEGER: 'integer[]',
				},
				INTEGER: 'integer',
				DATE: 'timestamp with time zone',
				BOOLEAN: 'boolean',
				JSONB: 'jsonb',
				JSON: 'json',
				STRING: 'character varying',
				BIGINT: 'bigint',
				TEXT: 'text',
			}
			const conversion = typeMap[type]
			if (conversion) {
				if (type === 'DATE' && (key === 'createdAt' || key === 'updatedAt')) {
					continue
				}
				outputArray.push({
					key: key,
					type: subField ? typeMap[type][subField] : conversion,
				})
			}
		}
		return outputArray
	} catch (err) {
		console.log(err)
	}
}
const metaAttributesTypeModifier = (data) => {
	try {
		const typeMap = {
			'ARRAY[STRING]': 'character varying[]',
			'ARRAY[INTEGER]': 'integer[]',
			'ARRAY[TEXT]': 'text[]',
			INTEGER: 'integer',
			DATE: 'timestamp with time zone',
			BOOLEAN: 'boolean',
			JSONB: 'jsonb',
			JSON: 'json',
			STRING: 'character varying',
			BIGINT: 'bigint',
			TEXT: 'text',
		}

		const outputArray = data.map((field) => {
			const { data_type, model_names, ...rest } = field
			const convertedDataType = typeMap[data_type]

			return convertedDataType
				? {
						...rest,
						data_type: convertedDataType,
						model_names: Array.isArray(model_names)
							? model_names.map((modelName) => `'${modelName}'`).join(', ')
							: model_names,
				  }
				: field
		})

		return outputArray
	} catch (err) {
		console.log(err)
	}
}

const generateRandomCode = (length) => {
	const charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
	let result = ''
	for (let i = 0; i < length; i++) {
		const randomIndex = Math.floor(Math.random() * charset.length)
		result += charset[randomIndex]
	}
	return result
}

const materializedViewQueryBuilder = async (model, concreteFields, metaFields, tenantCode) => {
	try {
		const tableName = model.tableName
		const temporaryMaterializedViewName = `${utils.getTenantViewName(tenantCode, tableName)}_${generateRandomCode(
			8
		)}`
		const concreteFieldsQuery = await concreteFields
			.map((data) => {
				return `${data.key}::${data.type} as ${data.key}`
			})
			.join(',\n')
		const metaFieldsQuery =
			metaFields.length > 0
				? await metaFields
						.map((data) => {
							if (data.data_type == 'character varying[]') {
								return `transform_jsonb_to_text_array(meta->'${data.value}')::${data.data_type} as ${data.value}`
							} else {
								return `(meta->>'${data.value}')::${data.data_type} as ${data.value}`
							}
						})
						.join(',\n')
				: '' // Empty string if there are no meta fields

		const whereClause = utils.generateWhereClause(tableName)
		// Add tenant-specific filtering to the WHERE clause - validate tenantCode for safety
		if (!tenantCode || typeof tenantCode !== 'string' || !/^[a-zA-Z0-9_-]+$/.test(tenantCode)) {
			throw new Error('Invalid tenant code for materialized view creation')
		}
		const tenantWhereClause = `${whereClause} AND tenant_code = '${tenantCode.replace(/'/g, "''")}'`

		const materializedViewGenerationQuery = `CREATE MATERIALIZED VIEW ${temporaryMaterializedViewName} AS
		  SELECT 
			  ${concreteFieldsQuery}${metaFieldsQuery && `,`}${metaFieldsQuery}
		  FROM public."${tableName}"
		  WHERE ${tenantWhereClause};`

		return { materializedViewGenerationQuery, temporaryMaterializedViewName }
	} catch (err) {
		console.log(err)
	}
}

const createIndexesOnAllowFilteringFields = async (model, modelEntityTypes, fieldsWithDatatype, tenantCode) => {
	try {
		const uniqueEntityTypeValueList = [...new Set(modelEntityTypes.entityTypeValueList)]
		const viewName = utils.getTenantViewName(tenantCode, model.tableName)

		await Promise.all(
			uniqueEntityTypeValueList.map(async (attribute) => {
				const item = fieldsWithDatatype.find(
					(element) => element.key === attribute || element.value === attribute
				)

				// Retrieve the type
				const type = item ? item.type || item.data_type : undefined

				if (!type) return false
				// Determine the query based on the type
				let query
				if (type === 'character varying' || type === 'character text') {
					query = `CREATE INDEX IF NOT EXISTS ${tenantCode}_idx_${model.tableName}_${attribute} ON ${viewName} USING gin (${attribute} gin_trgm_ops);`
				} else {
					query = `CREATE INDEX IF NOT EXISTS ${tenantCode}_idx_${model.tableName}_${attribute} ON ${viewName} USING gin (${attribute});`
				}

				return await sequelize.query(query)
			})
		)
	} catch (err) {
		console.log(err)
	}
}
const createViewGINIndexOnSearch = async (model, config, fields, tenantCode) => {
	try {
		const modelName = model.name
		const searchType =
			modelName === 'Session'
				? 'session'
				: modelName === 'UserExtension' || modelName === 'MentorExtension'
				? 'mentor'
				: null

		if (!searchType) {
			return
		}

		const fieldsConfig = config.search[searchType].fields
		const fieldsForIndex = fieldsConfig.filter((field) => !field.isAnEntityType).map((field) => field.name)

		if (fieldsForIndex.length === 0) {
			return
		}

		const viewName = utils.getTenantViewName(tenantCode, model.tableName)

		for (const field of fieldsForIndex) {
			try {
				await sequelize.query(`
                    CREATE INDEX IF NOT EXISTS ${tenantCode}_gin_index_${model.tableName}_${field}
                    ON ${viewName}
                    USING gin(${field} gin_trgm_ops);
                `)
			} catch (err) {}
		}
	} catch (err) {
		console.log(err)
	}
}
// Function to execute index queries for a specific model
const executeIndexQueries = async (modelName, tenantCode) => {
	// Find the index queries for the specified model
	const modelQueries = getIndexQueries(tenantCode).find((item) => item.modelName === modelName)

	if (modelQueries) {
		console.log(`Executing index queries for ${modelName}`)
		for (const query of modelQueries.queries) {
			try {
				await sequelize.query(query)
			} catch (error) {
				console.error(`Error executing query for ${modelName}: ${query}`, error)
			}
		}
	} else {
		console.log(`No index queries found for model: ${modelName}`)
	}
}
const deleteMaterializedView = async (viewName) => {
	try {
		await sequelize.query(`DROP MATERIALIZED VIEW ${viewName};`)
	} catch (err) {
		console.log(err)
	}
}

const renameMaterializedView = async (temporaryMaterializedViewName, tableName, tenantCode) => {
	const t = await sequelize.transaction()
	try {
		const finalViewName = utils.getTenantViewName(tenantCode, tableName)
		let randomViewName = `${finalViewName}_${generateRandomCode(8)}`

		const checkOriginalViewQuery = `SELECT COUNT(*) from pg_matviews where matviewname = '${finalViewName}';`
		const renameOriginalViewQuery = `ALTER MATERIALIZED VIEW ${finalViewName} RENAME TO ${randomViewName};`
		const renameNewViewQuery = `ALTER MATERIALIZED VIEW ${temporaryMaterializedViewName} RENAME TO ${finalViewName};`

		const temp = await sequelize.query(checkOriginalViewQuery)

		if (temp[0][0].count > 0) await sequelize.query(renameOriginalViewQuery, { transaction: t })
		else randomViewName = null
		await sequelize.query(renameNewViewQuery, { transaction: t })
		await t.commit()

		return randomViewName
	} catch (error) {
		await t.rollback()
		console.error('Error executing transaction:', error)
	}
}

const createViewUniqueIndexOnPK = async (model, tenantCode) => {
	try {
		const primaryKeys = model.primaryKeyAttributes
		const viewName = utils.getTenantViewName(tenantCode, model.tableName)

		const result = await sequelize.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS ${tenantCode}_unique_index_${model.tableName}_${primaryKeys.map(
			(key) => `_${key}`
		)} 
            ON ${viewName} (${primaryKeys.map((key) => `${key}`).join(', ')});`)
	} catch (err) {
		console.log(err)
	}
}

const generateMaterializedView = async (modelEntityTypes, tenantCode) => {
	try {
		const model = models[modelEntityTypes.modelName]

		const { concreteAttributes, metaAttributes } = await filterConcreteAndMetaAttributes(
			Object.keys(model.rawAttributes),
			modelEntityTypes.entityTypeValueList
		)

		const concreteFields = await rawAttributesTypeModifier(model.rawAttributes)

		const metaFields = await modelEntityTypes.entityTypes
			.map((entity) => {
				if (metaAttributes.includes(entity.value)) return entity
				else null
			})
			.filter(Boolean)

		const modifiedMetaFields = await metaAttributesTypeModifier(metaFields)

		const { materializedViewGenerationQuery, temporaryMaterializedViewName } = await materializedViewQueryBuilder(
			model,
			concreteFields,
			modifiedMetaFields,
			tenantCode
		)

		await sequelize.query(materializedViewGenerationQuery)
		const allFields = [...modifiedMetaFields, ...concreteFields]
		const randomViewName = await renameMaterializedView(temporaryMaterializedViewName, model.tableName, tenantCode)
		if (randomViewName) await deleteMaterializedView(randomViewName)
		await createIndexesOnAllowFilteringFields(model, modelEntityTypes, allFields, tenantCode)
		await createViewUniqueIndexOnPK(model, tenantCode)
		await createViewGINIndexOnSearch(model, searchConfig, allFields, tenantCode)
		await executeIndexQueries(model.name, tenantCode)
	} catch (err) {
		console.log(err)
	}
}

const getAllowFilteringEntityTypes = async (tenantCode) => {
	try {
		// Validate tenantCode parameter
		if (!tenantCode || tenantCode === 'undefined') {
			logger.error(`Invalid tenantCode provided: ${tenantCode}`)
			return []
		}

		// Fetch allow_filtering entity types across all orgs for this tenant.
		// All orgs can define filterable entity types; including them ensures
		// materialized view columns cover non-default org fields too.
		const entities = await entityTypeQueries.findAllEntityTypes(
			null, // no org filter — include all orgs under the tenant
			tenantCode,
			['id', 'value', 'label', 'data_type', 'organization_id', 'has_entities', 'model_names'],
			{
				allow_filtering: true,
			}
		)

		return entities
	} catch (err) {
		logger.error(`Error in getAllowFilteringEntityTypes: ${err.message}`)
		return []
	}
}

const triggerViewBuild = async (tenantCode) => {
	try {
		await ensureTransformJsonbToTextArrayFunction()
		const allowFilteringEntityTypes = await getAllowFilteringEntityTypes(tenantCode)
		const entityTypesGroupedByModel = await groupByModelNames(allowFilteringEntityTypes)

		await Promise.all(
			entityTypesGroupedByModel.map(async (modelEntityTypes) => {
				return generateMaterializedView(modelEntityTypes, tenantCode)
			})
		)

		return entityTypesGroupedByModel
	} catch (err) {
		console.log(err)
	}
}

//Refresh Flow

const modelNameCollector = async (entityTypes) => {
	try {
		const modelSet = new Set()
		await Promise.all(
			entityTypes.map(async ({ model_names }) => {
				if (model_names && Array.isArray(model_names))
					await Promise.all(
						model_names.map((model) => {
							if (!modelSet.has(model)) modelSet.add(model)
						})
					)
			})
		)
		return [...modelSet.values()]
	} catch (err) {
		console.log(err)
	}
}

const refreshMaterializedView = async (modelName, tenantCode) => {
	try {
		if (!modelName || !tenantCode) {
			logger.warn(
				`refreshMaterializedView: Missing parameters - modelName: ${modelName}, tenantCode: ${tenantCode}`
			)
			return { success: false, message: 'Missing required parameters' }
		}
		const model = models[modelName]
		if (!model) {
			logger.warn(`refreshMaterializedView: Model not found - modelName: ${modelName}`)
			return { success: false, message: `Model ${modelName} not found` }
		}
		const viewName = utils.getTenantViewName(tenantCode, model.tableName)

		// Check if a REFRESH MATERIALIZED VIEW query is already running
		const [activeQueries] = await sequelize.query(`
		SELECT * FROM pg_stat_activity
		WHERE query LIKE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ${viewName}%'
		  AND state = 'active';
	  `)

		// If there are active refresh queries, skip refreshing the materialized view
		if (activeQueries.length > 0) {
			logger.info(`refreshMaterializedView: Skipping refresh for ${viewName} - refresh already in progress`)
			return { success: true, message: 'Refresh already in progress', skipped: true }
		}

		// If no active refresh queries, proceed with refreshing the materialized view
		const [result, metadata] = await sequelize.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${viewName}`)
		logger.info(`refreshMaterializedView: Successfully refreshed ${viewName} for tenant ${tenantCode}`)
		return {
			success: true,
			message: 'Materialized view refreshed successfully',
			result,
			metadata,
			rowCount: metadata?.rowCount,
		}
	} catch (err) {
		logger.error(
			`refreshMaterializedView: Error refreshing view for model ${modelName}, tenant ${tenantCode}:`,
			err.message || err
		)
		return { success: false, message: err.message || 'Failed to refresh materialized view', error: err }
	}
}

const refreshNextView = (currentIndex, modelNames, tenantCode) => {
	try {
		if (currentIndex < modelNames.length) {
			refreshMaterializedView(modelNames[currentIndex], tenantCode)
			currentIndex++
		} else {
			currentIndex = 0 // Reset to start over for next cycle
		}
		return currentIndex
	} catch (err) {
		console.log(err)
	}
}

const triggerPeriodicViewRefresh = async (tenantCode) => {
	try {
		const allowFilteringEntityTypes = await getAllowFilteringEntityTypes(tenantCode)
		const modelNames = await modelNameCollector(allowFilteringEntityTypes)
		const interval = process.env.REFRESH_VIEW_INTERVAL
		let currentIndex = 0

		// Using the mockSetInterval function to simulate setInterval
		refreshInterval = setInterval(() => {
			currentIndex = refreshNextView(currentIndex, modelNames, tenantCode)
		}, interval / modelNames.length)

		// Immediately trigger the first refresh
		currentIndex = refreshNextView(currentIndex, modelNames, tenantCode)
	} catch (err) {
		console.log(err)
	}
}

const checkAndCreateMaterializedViews = async () => {
	try {
		await ensureTransformJsonbToTextArrayFunction()
		await sequelize.query('CREATE EXTENSION IF NOT EXISTS pg_trgm;', {
			type: sequelize.QueryTypes.SELECT,
		})

		// Get all existing materialized views
		const query = 'select matviewname from pg_matviews;'
		const [result, metadata] = await sequelize.query(query)

		// Convert existing view names for comparison (normalize naming)
		const existingViewNames = result.map(({ matviewname }) => matviewname.toLowerCase().trim())

		// Get distinct tenant codes from org_extension table
		const orgExtensionQuery =
			"SELECT DISTINCT tenant_code FROM organization_extension WHERE tenant_code IS NOT NULL AND tenant_code != '' AND tenant_code != 'undefined' ORDER BY tenant_code ASC"
		const [orgTenants] = await sequelize.query(orgExtensionQuery)

		const tenantsNeedingViews = []

		// Check each tenant from org_extension
		for (const tenant of orgTenants) {
			const tenantCode = tenant.tenant_code

			if (!tenantCode || tenantCode === 'undefined') {
				continue
			}

			// Use the original logic: get entity types and group by models
			const allowFilteringEntityTypes = await getAllowFilteringEntityTypes(tenantCode)
			const entityTypesGroupedByModel = await groupByModelNames(allowFilteringEntityTypes)

			// Check if views exist for this tenant
			let tenantViewsMissing = false
			for (const modelEntityTypes of entityTypesGroupedByModel) {
				const model = models[modelEntityTypes.modelName]
				const expectedViewName = utils.getTenantViewName(tenantCode, model.tableName)

				// Convert expected view name for comparison (normalize naming)
				const normalizedExpectedName = expectedViewName.toLowerCase().trim()

				const mViewExists = existingViewNames.includes(normalizedExpectedName)
				if (!mViewExists) {
					tenantViewsMissing = true
					break
				}
			}

			// If this tenant is missing views, add to list
			if (tenantViewsMissing) {
				tenantsNeedingViews.push(tenantCode)
			}
		}

		// Create views only for tenants that need them
		if (tenantsNeedingViews.length > 0) {
			logger.info(`Materialized views missing for tenants: ${tenantsNeedingViews.join(', ')}. Building views...`)

			const results = []
			for (const tenantCode of tenantsNeedingViews) {
				logger.info(`Building materialized views for tenant: ${tenantCode}`)
				const result = await triggerViewBuild(tenantCode)
				results.push({
					tenantCode,
					result: result || 'Success',
				})
			}

			return {
				success: true,
				message: `Built materialized views for ${results.length} tenants`,
				results,
			}
		} else {
			logger.info('All materialized views already exist for all org_extension tenants')
			return { success: true, message: 'All materialized views already exist' }
		}
	} catch (error) {
		logger.error(`Error in checkAndCreateMaterializedViews: ${error.message}`)
		// If there's an error checking, fall back to building all views
		return await triggerViewBuildForAllTenants()
	}
}

const triggerViewBuildForAllTenants = async () => {
	try {
		const tenants = await userExtensionQueries.getDistinctTenantCodes()
		const results = []

		for (const tenant of tenants) {
			const tenantCode = tenant.code

			// Skip tenants with undefined or empty tenant codes
			if (!tenantCode || tenantCode === 'undefined') {
				logger.warn(`Skipping tenant with invalid code: ${tenant}`)
				continue
			}

			logger.info(`Building materialized views for tenant: ${tenantCode}`)
			const result = await triggerViewBuild(tenantCode)
			results.push({
				tenantCode,
				result: result || 'Success',
			})
		}

		return {
			success: true,
			message: `Built materialized views for ${results.length} tenants`,
			results,
		}
	} catch (err) {
		logger.error(`Error in triggerViewBuildForAllTenants: ${err.message}`)
		return {
			success: false,
			message: 'Failed to build views for all tenants',
			error: err.message,
		}
	}
}

const triggerPeriodicViewRefreshForAllTenants = async (modelName = null) => {
	try {
		const tenants = await userExtensionQueries.getDistinctTenantCodes()
		const results = []

		for (const tenant of tenants) {
			const tenantCode = tenant.code

			// Skip tenants with undefined or empty tenant codes
			if (!tenantCode || tenantCode === 'undefined') {
				logger.warn(`Skipping tenant with invalid code: ${tenant}`)
				continue
			}

			if (modelName) {
				// Refresh specific model for this tenant
				logger.info(`Refreshing model ${modelName} for tenant: ${tenantCode}`)
				const result = await refreshMaterializedView(modelName, tenantCode)
				results.push({
					tenantCode,
					model: modelName,
					result: result || 'Success',
				})
			} else {
				// Refresh all models for this tenant
				logger.info(`Starting periodic refresh for tenant: ${tenantCode}`)
				const result = await triggerPeriodicViewRefresh(tenantCode)
				results.push({
					tenantCode,
					result: result || 'Success',
				})
			}
		}

		return {
			success: true,
			message: `Started periodic refresh for ${results.length} tenants`,
			results,
		}
	} catch (err) {
		logger.error(`Error in triggerPeriodicViewRefreshForAllTenants: ${err.message}`)
		return {
			success: false,
			message: 'Failed to start refresh for all tenants',
			error: err.message,
		}
	}
}

const getStableOffset = (tenantCode, modelName, interval) => {
	if (!tenantCode || !modelName || !interval || Number(interval) <= 0) {
		logger.error(
			`[getStableOffset] Invalid params - tenantCode: ${tenantCode}, modelName: ${modelName}, interval: ${interval}`
		)
		return 0
	}
	const hash = crypto.createHash('md5').update(`${tenantCode}_${modelName}`).digest('hex')
	return parseInt(hash.substring(0, 8), 16) % Number(interval)
}

const scheduleViewRefreshJob = (tenantCode, modelName, interval) => {
	if (!tenantCode || !modelName) {
		logger.error(
			`[scheduleViewRefreshJob] Missing required params - tenantCode: ${tenantCode}, modelName: ${modelName}`
		)
		return
	}
	if (!interval || Number(interval) <= 0) {
		logger.error(
			`[scheduleViewRefreshJob] Invalid interval: ${interval} for tenant: ${tenantCode}, model: ${modelName}`
		)
		return
	}

	const jobId = `repeatable_view_job_${tenantCode}_${modelName}`
	const jobName = `repeatable_view_job_${tenantCode}_${modelName}`
	const encodedParams = encodeURIComponent(`${tenantCode}|${modelName}`)
	const urlEndpoint = `/mentoring/v1/admin/triggerPeriodicViewRefreshInternal/${encodedParams}`
	const offset = getStableOffset(tenantCode, modelName, interval)
	schedulerRequests.createSchedulerJob(jobId, null, jobName, {}, urlEndpoint, 'get', {
		jobId: jobId,
		repeat: { every: Number(interval), offset },
		removeOnComplete: 50,
		removeOnFail: 200,
	})
}

const adminService = {
	triggerViewBuild,
	triggerPeriodicViewRefresh,
	refreshMaterializedView,
	checkAndCreateMaterializedViews,
	triggerViewBuildForAllTenants,
	triggerPeriodicViewRefreshForAllTenants,
	scheduleViewRefreshJob,
}

module.exports = adminService
