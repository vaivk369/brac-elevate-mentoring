const Session = require('@database/models/index').Session
const { Op, literal, QueryTypes } = require('sequelize')
const common = require('@constants/common')
const utils = require('@generics/utils')
const sequelize = require('sequelize')
const moment = require('moment')
const Sequelize = require('@database/models/index').sequelize
const SessionAttendee = require('@database/models/index').SessionAttendee
const { attendeeData } = require('@dtos/attendee')

exports.getColumns = async () => {
	try {
		return await Object.keys(Session.rawAttributes)
	} catch (error) {
		return error
	}
}

exports.getModelName = async () => {
	try {
		return await Session.name
	} catch (error) {
		return error
	}
}

exports.create = async (data, tenantCode) => {
	try {
		data.tenant_code = tenantCode
		const session = await Session.create(data)
		return session
	} catch (error) {
		return error
	}
}

exports.findOne = async (filter, tenantCode, options = {}) => {
	try {
		const whereClause = {
			...filter,
			tenant_code: tenantCode,
		}

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		const res = await Session.findOne({
			where: {
				...optionsWhere, // Allow additional where conditions (or undefined if empty)
				...whereClause, // But tenant filtering takes priority
			},
			...otherOptions, // Other options like attributes, order, etc.
			raw: true,
		})
		return res
	} catch (error) {
		return error
	}
}

exports.findById = async (id, tenantCode) => {
	try {
		return await Session.findOne({ where: { id, tenant_code: tenantCode }, raw: true })
	} catch (error) {
		return error
	}
}

exports.findSessionWithAttendee = async (sessionId, userId, tenantCode) => {
	try {
		const result = await Session.findOne({
			where: {
				id: sessionId,
				tenant_code: tenantCode,
			},
			include: [
				{
					model: SessionAttendee,
					as: 'attendees', // correct alias
					required: false,
					where: {
						mentee_id: userId,
						tenant_code: tenantCode,
					},
					attributes: ['id', 'type', 'meeting_info', 'joined_at', 'mentee_id'],
				},
			],
		})

		const attendee = result?.attendees?.[0]

		return result
			? {
					...result.get(),
					...attendeeData(attendee),
			  }
			: null
	} catch (error) {
		return error
	}
}

exports.findByIdWithMentorDetails = async (id, tenantCode) => {
	try {
		// Optimized: Get session with mentor details in single query for un-enrollment flow
		return await Session.findOne({
			where: { id, tenant_code: tenantCode },
			include: [
				{
					model: Session.sequelize.models.MentorExtension,
					as: 'mentor_extension',
					attributes: ['name'],
					where: { tenant_code: tenantCode },
				},
			],
		})
	} catch (error) {
		return error
	}
}

exports.updateOne = async (filter, update, tenantCode, options = {}) => {
	try {
		filter.tenant_code = tenantCode

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		const result = await Session.update(update, {
			where: {
				...optionsWhere, // Allow additional where conditions
				...filter, // But tenant filtering takes priority
			},
			...otherOptions,
			individualHooks: true,
		})
		const [rowsAffected, updatedRows] = result

		return options.returning ? { rowsAffected, updatedRows } : rowsAffected
	} catch (error) {
		return error
	}
}

/**
 * Update Session table rows with provided data and conditions
 * @param {Object} data - Fields to update (e.g., { deleted_at: new Date() })
 * @param {Object} where - WHERE condition (e.g., { id: sessionIds })
 * @returns {Promise<number>} Number of affected rows
 */
exports.updateRecords = async (data, options = {}) => {
	try {
		if (!options.where || Object.keys(options.where).length === 0) {
			throw new Error('updateRecords: "where" condition is required')
		}
		const result = await Session.update(data, options)
		return Array.isArray(result) ? result[0] : result // Sequelize returns [number of affected rows]
	} catch (error) {
		return error
	}
}

exports.findAll = async (filter, tenantCode, options = {}) => {
	try {
		filter.tenant_code = tenantCode

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		return await Session.findAll({
			where: {
				...optionsWhere, // Allow additional where conditions
				...filter, // But tenant filtering takes priority
			},
			...otherOptions,
			raw: true,
		})
	} catch (error) {
		return error
	}
}

exports.updateEnrollmentCount = async (sessionId, increment = true, tenantCode) => {
	try {
		const options = increment ? { by: 1 } : { by: -1 }
		const result = this.incrementOrDecrement(
			{
				where: { id: sessionId, tenant_code: tenantCode },
				...options,
			},
			'seats_remaining'
		)
		return result
	} catch (error) {
		return error
	}
}

exports.incrementOrDecrement = async (filterWithOptions, incrementFields = []) => {
	try {
		// Note: tenant_code filtering should already be included in filterWithOptions.where
		return await Session.increment(incrementFields, filterWithOptions)
	} catch (error) {
		return error
	}
}

exports.getSessionByUserIdAndTime = async (userId, startDate, endDate, sessionId, tenantCode) => {
	try {
		let startDateResponse, endDateResponse
		const query = {
			mentor_id: userId,
			status: { [Op.ne]: common.COMPLETED_STATUS },
		}

		if (startDate) {
			query.start_date = {
				[Op.lte]: startDate,
			}
			query.end_date = {
				[Op.gte]: startDate,
			}

			if (sessionId) {
				// check if sessionId is truthy (i.e. not undefined or empty)
				query.id = { [Op.ne]: sessionId }
			}

			startDateResponse = await this.findAll(query, tenantCode)
		}
		if (endDate) {
			query.start_date = {
				[Op.lte]: endDate,
			}
			query.end_date = {
				[Op.gte]: endDate,
			}

			if (sessionId) {
				// check if sessionId is truthy (i.e. not undefined or empty)
				query.id = { [Op.ne]: sessionId }
			}

			endDateResponse = await this.findAll(query, tenantCode)
		}

		return {
			startDateResponse: startDateResponse,
			endDateResponse: endDateResponse,
		}
	} catch (error) {
		return error
	}
}

exports.deleteSession = async (filter, tenantCode) => {
	try {
		filter.tenant_code = tenantCode
		return await Session.destroy({
			where: filter,
		})
	} catch (error) {
		return error
	}
}

exports.findSessionForPublicEndpoint = async (sessionId, tenantCode) => {
	try {
		return await Session.findOne({
			where: {
				id: sessionId,
				tenant_code: tenantCode,
			},
			attributes: ['id', 'tenant_code', 'title', 'status'],
			raw: true,
		})
	} catch (error) {
		return error
	}
}

// Special method for BBB callbacks to get tenant_code safely
exports.getSessionTenantCode = async (sessionId) => {
	try {
		return await Session.findOne({
			where: { id: sessionId },
			attributes: ['id', 'tenant_code', 'mentor_organization_id'],
			raw: true,
		})
	} catch (error) {
		return error
	}
}

exports.updateSession = async (filter, update, tenantCode, options = {}) => {
	try {
		filter.tenant_code = tenantCode

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		return await Session.update(update, {
			where: {
				...optionsWhere, // Allow additional where conditions
				...filter, // But tenant filtering takes priority
			},
			...otherOptions,
		})
	} catch (error) {
		return error
	}
}
exports.removeAndReturnMentorSessions = async (userId, tenantCode) => {
	try {
		const currentEpochTime = moment().unix()
		const currentDate = moment()
		const currentDateTime = moment().format('YYYY-MM-DD HH:mm:ssZ')

		const foundSessions = await Session.findAll({
			where: {
				// Op.and ensures both [Op.or] blocks are applied; duplicate object keys silently overwrite
				[Op.and]: [
					{ [Op.or]: [{ mentor_id: userId }, { created_by: userId }] },
					{ [Op.or]: [{ start_date: { [Op.gt]: currentEpochTime } }, { status: common.PUBLISHED_STATUS }] },
				],
				tenant_code: tenantCode,
			},
			raw: true,
		})

		const upcomingSessionIds = foundSessions.map((session) => session.id)

		const updatedSessions = await Session.update(
			{
				deleted_at: currentDateTime,
				mentor_name: common.USER_NOT_FOUND,
				mentor_id: null,
			},
			{
				where: {
					mentor_id: userId,
					created_by: userId,
					id: { [Op.in]: upcomingSessionIds },
					tenant_code: tenantCode,
				},
			}
		)

		// Only return sessions that were actually soft-deleted (mentor is both creator and assigned mentor).
		// Session-manager-created sessions (created_by != userId) are NOT deleted and must NOT be included
		// here, or their attendees would be incorrectly unenrolled.
		const deletedSessionIdAndTitle = foundSessions
			.filter((s) => String(s.mentor_id) === String(userId) && String(s.created_by) === String(userId))
			.map((session) => ({ id: session.id, title: session.title, start_date: session.start_date }))

		const removedSessions = updatedSessions[0] > 0 ? deletedSessionIdAndTitle : []
		return removedSessions
	} catch (error) {
		return error
	}
}

// Duplicate findAllSessions function removed - improved version exists at end of file
exports.getAllUpcomingSessions = async (paranoid, tenantCode) => {
	const currentEpochTime = moment().unix()
	//const currentEpochTime = moment().format('YYYY-MM-DD HH:mm:ssZ')

	try {
		return await Session.findAll({
			paranoid: paranoid,
			where: {
				start_date: {
					[Op.gt]: currentEpochTime,
				},
				status: {
					[Op.not]: common.INACTIVE_STATUS,
				},
				tenant_code: tenantCode,
			},
			raw: true,
		})
	} catch (err) {
		throw err
	}
}

exports.updateEnrollmentCount = async (sessionId, increment = true, tenantCode) => {
	try {
		const updateFields = increment
			? { seats_remaining: literal('"seats_remaining" + 1') }
			: { seats_remaining: literal('"seats_remaining" - 1') }

		return await Session.update(updateFields, {
			where: {
				id: sessionId,
				tenant_code: tenantCode,
			},
		})
	} catch (error) {
		return error
	}
}
exports.countHostedSessions = async (id, tenantCode) => {
	try {
		const whereClause = {
			mentor_id: id,
			status: 'COMPLETED',
			started_at: {
				[Op.not]: null,
			},
		}

		if (tenantCode) {
			whereClause.tenant_code = tenantCode
		}

		const count = await Session.count({
			where: whereClause,
		})
		return count
	} catch (error) {
		return error
	}
}

exports.getCreatedSessionsCountInDateRange = async (mentorId, startDate, endDate, tenantCode) => {
	try {
		const count = await Session.count({
			where: {
				created_at: {
					[Op.between]: [startDate, endDate],
				},
				created_by: mentorId, // Sessions created by this user
				tenant_code: tenantCode,
			},
		})
		return count
	} catch (error) {
		return error
	}
}

/**
 * Get the count of mentoring sessions within a date range for a specific mentor.
 * @param {number} mentorId 	- The ID of the mentor.
 * @param {Date} startDate 		- The start date of the date range.
 * @param {Date} endDate 		- The end date of the date range.
 * @returns {Promise<number>} 	- The count of mentoring sessions.
 * @throws {Error} 				- If an error occurs during the process.
 */

exports.getAssignedSessionsCountInDateRange = async (mentorId, startDate, endDate, tenantCode) => {
	try {
		const count = await Session.count({
			where: {
				created_at: {
					[Op.between]: [startDate, endDate],
				},
				mentor_id: mentorId,
				created_by: { [Op.ne]: mentorId }, // Sessions assigned to mentor but not created by them
				tenant_code: tenantCode,
			},
		})
		return count
	} catch (error) {
		return error
	}
}

exports.getHostedSessionsCountInDateRange = async (mentorId, startDate, endDate, tenantCode) => {
	try {
		const count = await Session.count({
			where: {
				mentor_id: mentorId, // Sessions where this user is the mentor
				status: 'COMPLETED',
				start_date: {
					[Op.between]: [startDate, endDate],
				},
				started_at: {
					[Op.not]: null,
				},
				tenant_code: tenantCode,
			},
		})
		return count
	} catch (error) {
		return error
	}
}

exports.getMentorsUpcomingSessions = async (page, limit, search = '', mentorId, tenantCode) => {
	try {
		const currentEpochTime = moment().unix()

		const sessionAttendeesData = await Session.findAndCountAll({
			where: {
				[Op.and]: [
					{
						[Op.or]: [{ mentor_id: mentorId }, { created_by: mentorId }],
						status: 'PUBLISHED',
						start_date: {
							[Op.gt]: currentEpochTime,
						},
						started_at: {
							[Op.eq]: null,
						},
						tenant_code: tenantCode,
					},
					{
						[Op.or]: [
							sequelize.where(
								sequelize.fn('LOWER', sequelize.col('title')),
								'LIKE',
								`%${search.toLowerCase()}%`
							),
						],
					},
				],
			},
			order: [['start_date', 'ASC']],
			attributes: [
				'id',
				'title',
				'description',
				'start_date',
				'end_date',
				'status',
				'image',
				'mentor_id',
				'meeting_info',
				/* 				[(sequelize.json('meeting_info.platform'), 'meeting_info.platform')],
				[sequelize.json('meeting_info.value'), 'meeting_info.value'], */
			],
			offset: limit * (page - 1),
			limit: limit,
			raw: true,
		})

		return {
			data: sessionAttendeesData.rows,
			count: sessionAttendeesData.count,
		}
	} catch (error) {
		return error
	}
}

exports.getUpcomingSessions = async (page, limit, search, userId, startDate, endDate, tenantCode) => {
	try {
		const currentEpochTime = moment().unix()
		let whereCondition = {
			[Op.or]: [{ title: { [Op.iLike]: `%${search}%` } }],
			mentor_id: {
				[Op.or]: [{ [Op.ne]: userId }, { [Op.is]: null }],
			},
			end_date: {
				[Op.gt]: currentEpochTime,
			},
			status: {
				[Op.in]: [common.PUBLISHED_STATUS, common.LIVE_STATUS],
			},
			tenant_code: tenantCode,
		}

		if (startDate && endDate) {
			const startEpoch = startDate
			const endEpoch = endDate

			// Log to debug

			whereCondition.start_date = {
				[Op.gte]: startEpoch,
				[Op.lte]: endEpoch,
			}
		}

		const sessionData = await Session.findAndCountAll({
			where: whereCondition,
			order: [['start_date', 'ASC']],
			attributes: [
				'id',
				'title',
				'description',
				'start_date',
				'end_date',
				'status',
				'image',
				'mentor_id',
				'created_at',
				'meeting_info',
				'visibility',
				'mentor_organization_id',
				/* ['meetingInfo.platform', 'meetingInfo.platform'],
				['meetingInfo.value', 'meetingInfo.value'], */
			],
			offset: limit * (page - 1),
			limit: limit,
			raw: true,
		})
		return sessionData
	} catch (error) {
		return error
	}
}

exports.getEnrolledSessions = async (page, limit, search, userId, startDate, endDate, tenantCode) => {
	try {
		const query = `
		SELECT 
			s.*,
			sa.type AS enrolled_type,
			sa.is_feedback_skipped
		FROM session_attendees sa
		INNER JOIN sessions s ON sa.session_id = s.id AND sa.tenant_code = s.tenant_code
		WHERE 
			sa.mentee_id = :userId
			AND s.status IN (:statusList)
			AND s.end_date > :currentEpoch
			AND s.deleted_at IS NULL
			AND sa.deleted_at IS NULL
			AND sa.tenant_code = :tenantCode
			${search ? 'AND s.title ILIKE :search' : ''}
			${startDate && endDate ? 'AND s.start_date BETWEEN :startEpoch AND :endEpoch' : ''}
		ORDER BY s.start_date ASC
		OFFSET :offset
		LIMIT :limit
		`

		const replacements = {
			userId,
			statusList: [common.PUBLISHED_STATUS, common.LIVE_STATUS],
			currentEpoch: moment().unix(),
			search: `%${search}%`,
			startEpoch: startDate,
			endEpoch: endDate,
			offset: limit * (page - 1),
			limit,
			tenantCode,
		}

		const sessionDetails = await Sequelize.query(query, {
			replacements,
			type: QueryTypes.SELECT,
		})

		const countQuery = `
		SELECT COUNT(DISTINCT s.id) AS "count"
		FROM session_attendees sa
		INNER JOIN sessions s ON sa.session_id = s.id AND sa.tenant_code = s.tenant_code
		WHERE 
			sa.mentee_id = :userId
			AND s.status IN (:statusList)
			AND s.end_date > :currentEpoch
			AND s.deleted_at IS NULL
			AND sa.deleted_at IS NULL
			AND sa.tenant_code = :tenantCode
			${search ? 'AND s.title ILIKE :search' : ''}
			${startDate && endDate ? 'AND s.start_date BETWEEN :startEpoch AND :endEpoch' : ''}
		`
		const count = await Sequelize.query(countQuery, {
			type: QueryTypes.SELECT,
			replacements: replacements,
		})

		let rows = []
		if (sessionDetails.length > 0) {
			rows = sessionDetails.map(({ mentee_password, mentor_password, ...rest }) => rest)
		}

		return { rows, count: Number(count[0].count) }
	} catch (error) {
		console.error(error)
		throw error
	}
}

exports.findAndCountAll = async (filter, tenantCode, options = {}, attributes = {}) => {
	try {
		filter.tenant_code = tenantCode

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		const { rows, count } = await Session.findAndCountAll({
			where: {
				...optionsWhere, // Allow additional where conditions
				...filter, // But tenant filtering takes priority
			},
			...otherOptions,
			...attributes,
			raw: true,
		})
		return { rows, count }
	} catch (error) {
		return error
	}
}
exports.mentorsSessionWithPendingFeedback = async (mentorId, tenantCode, options = {}, completedSessionIds) => {
	try {
		const whereClause = {
			id: { [Op.notIn]: completedSessionIds },
			status: common.COMPLETED_STATUS,
			started_at: {
				[Op.not]: null,
			},
			is_feedback_skipped: false,
			mentor_id: mentorId,
			tenant_code: tenantCode,
		}

		// Safe merge: tenant filtering cannot be overridden by options.where
		const { where: optionsWhere, ...otherOptions } = options

		return await Session.findAll({
			where: {
				...optionsWhere, // Allow additional where conditions
				...whereClause, // But tenant filtering takes priority
			},
			...otherOptions,
			raw: true,
		})
	} catch (error) {
		return error
	}
}

exports.getUpcomingSessionsFromView = async (
	page,
	limit,
	searchFilter,
	userId,
	filter,
	tenantCode,
	saasFilter = '',
	additionalProjectionclause = '',
	searchText,
	defaultFilter = ''
) => {
	try {
		const currentEpochTime = Math.floor(Date.now() / 1000)

		const filterClause = filter?.query.length > 0 ? `AND (${filter.query})` : ''

		const saasFilterClause = saasFilter != '' ? saasFilter : ''
		const defaultFilterClause = defaultFilter != '' ? 'AND ' + defaultFilter : ''
		// No longer need tenant filtering since we use tenant-specific views
		let publicSessionFilter = " AND type = '" + common.SESSION_TYPE.PUBLIC + "'"

		// Create selection clause
		let projectionClause = [
			'id',
			'title',
			'description',
			'start_date',
			'end_date',
			'meta',
			'recommended_for',
			'medium',
			'categories',
			'status',
			'image',
			'mentor_id',
			'visibility',
			'mentor_organization_id',
			'created_at',
			'mentor_name',
			'seats_limit',
			'seats_remaining',
			"(meeting_info - 'link') AS meeting_info",
		]

		if (additionalProjectionclause !== '') {
			projectionClause.push(additionalProjectionclause)
		}

		//if (searchFilter.positionQuery !== '') {
		//projectionClause.push(searchFilter.positionQuery)
		//}

		projectionClause = projectionClause.join(',')

		let orderClause = []
		if (searchFilter.sortQuery !== '') {
			orderClause.push(searchFilter.sortQuery)
		}
		orderClause.push('start_date ASC')
		orderClause = orderClause.join(',')

		const query = `
		SELECT 
			${projectionClause}
		FROM
			${utils.getTenantViewName(tenantCode, Session.tableName)}
		WHERE
			mentor_id != :userId
				${saasFilterClause}
			${filterClause}
			AND status IN ('${common.PUBLISHED_STATUS}', '${common.LIVE_STATUS}')
			${publicSessionFilter}
			${searchFilter.whereClause}
			${defaultFilterClause}
		ORDER BY
			${orderClause}
		OFFSET
			:offset
		LIMIT
			:limit;
	`

		const replacements = {
			search: `%${searchText}%`,
			userId: userId,
			currentEpochTime: currentEpochTime,
			offset: limit * (page - 1),
			limit: limit,
			tenantCode: tenantCode,
			...filter.replacements,
		}

		if (filter && typeof filter === 'object') {
			for (const key in filter) {
				if (Array.isArray(filter[key])) {
					replacements[key] = filter[key]
				}
			}
		}

		const sessionIds = await Sequelize.query(query, {
			type: QueryTypes.SELECT,
			replacements: replacements,
		})
		const countQuery = `
		SELECT count(*) AS "count"
		FROM
			${utils.getTenantViewName(tenantCode, Session.tableName)}
		WHERE
			mentor_id != :userId
				${saasFilterClause}
			${filterClause}
			AND status IN ('${common.PUBLISHED_STATUS}', '${common.LIVE_STATUS}')
			${publicSessionFilter}
			${searchFilter.whereClause}
			${defaultFilterClause}
		;
	`
		const count = await Sequelize.query(countQuery, {
			type: QueryTypes.SELECT,
			replacements: replacements,
		})

		return {
			rows: sessionIds,
			count: Number(count[0].count),
		}
	} catch (error) {
		return error
	}
}

exports.findAllByIds = async (ids, tenantCode) => {
	try {
		return await Session.findAll({
			where: {
				id: ids,
				tenant_code: tenantCode,
			},
			raw: true,
			order: [['created_at', 'DESC']],
		})
	} catch (error) {
		return error
	}
}

exports.getMentorsUpcomingSessionsFromView = async (
	page,
	limit,
	search = '',
	mentorId,
	filter,
	tenantCode,
	saasFilter = '',
	defaultFilter = '',
	menteeUserId
) => {
	try {
		const currentEpochTime = Math.floor(Date.now() / 1000)

		const filterClause = filter?.query.length > 0 ? `AND ${filter.query}` : ''

		const saasFilterClause = saasFilter != '' ? saasFilter : ''
		// No longer need tenant filtering since we use tenant-specific views

		const defaultFilterClause = defaultFilter != '' ? 'AND ' + defaultFilter : ''

		const query = `
		SELECT
		Sessions.id,
		Sessions.title,
		Sessions.description,
			Sessions.start_date,
			Sessions.end_date,
			Sessions.status,
			Sessions.image,
			Sessions.mentor_id,
			Sessions.meeting_info,
			Sessions.visibility,
			Sessions.mentor_organization_id,
			Sessions.type,
			CASE WHEN sa.id IS NOT NULL THEN true ELSE false END AS is_enrolled,
			COALESCE(sa.type, NULL) AS enrolment_type
		FROM
				${utils.getTenantViewName(tenantCode, Session.tableName)} as Sessions
				LEFT JOIN session_attendees AS sa
				ON Sessions.id = sa.session_id AND sa.mentee_id = :menteeUserId AND sa.tenant_code = :tenantCode
		WHERE
			mentor_id = :mentorId
				AND status = 'PUBLISHED'
			AND start_date > :currentEpochTime
			AND started_at IS NULL
			AND (
				LOWER(title) LIKE :search
			)
			AND (
				Sessions.type = 'PUBLIC'
				OR (Sessions.type = 'PRIVATE' AND sa.id IS NOT NULL)
			)
			${filterClause}
			${saasFilterClause}
			${defaultFilterClause}
			
		ORDER BY
			Sessions.start_date ASC
		OFFSET
			:offset
		LIMIT
			:limit;
	`

		const replacements = {
			mentorId: mentorId,
			currentEpochTime: currentEpochTime,
			search: `%${search.toLowerCase()}%`,
			offset: limit * (page - 1),
			limit: limit,
			tenantCode: tenantCode,
			menteeUserId,
			...filter.replacements, // Add filter parameters to replacements
		}

		const sessionAttendeesData = await Sequelize.query(query, {
			type: QueryTypes.SELECT,
			replacements: replacements,
		})

		const countQuery = `
		SELECT count(*) AS "count"
		FROM
		${utils.getTenantViewName(tenantCode, Session.tableName)} as Sessions
		LEFT JOIN session_attendees AS sa
				ON Sessions.id = sa.session_id AND sa.mentee_id = :menteeUserId  AND sa.tenant_code = :tenantCode
		WHERE
			mentor_id = :mentorId
				AND status = 'PUBLISHED'
			AND start_date > :currentEpochTime
			AND started_at IS NULL
			AND (
				LOWER(title) LIKE :search
			)
			AND (
				Sessions.type = 'PUBLIC'
				OR (Sessions.type = 'PRIVATE' AND sa.id IS NOT NULL)
			)
			${filterClause}
			${saasFilterClause}
			${defaultFilterClause};
	`
		const count = await Sequelize.query(countQuery, {
			type: QueryTypes.SELECT,
			replacements: replacements,
		})
		return {
			data: sessionAttendeesData,
			count: Number(count[0].count),
		}
	} catch (error) {
		return error
	}
}

exports.deactivateAndReturnMentorSessions = async (userId, tenantCode) => {
	try {
		const currentEpochTime = moment().unix()
		const currentDateTime = moment().format('YYYY-MM-DD HH:mm:ssZ')

		const foundSessions = await Session.findAll({
			where: {
				[Op.or]: [{ mentor_id: userId }, { created_by: userId }],
				[Op.or]: [{ start_date: { [Op.gt]: currentEpochTime } }, { status: common.PUBLISHED_STATUS }],
				tenant_code: tenantCode,
			},
			raw: true,
		})

		const sessionIdAndTitle = foundSessions.map((session) => {
			return { id: session.id, title: session.title }
		})
		const upcomingSessionIds = foundSessions.map((session) => session.id)

		const updatedSessions = await Session.update(
			{
				status: common.INACTIVE_STATUS,
			},
			{
				where: {
					id: { [Op.in]: upcomingSessionIds },
					tenant_code: tenantCode,
				},
			}
		)
		const removedSessions = updatedSessions[0] > 0 ? sessionIdAndTitle : []
		return removedSessions
	} catch (error) {
		return error
	}
}

exports.getUpcomingSessionsOfMentee = async (menteeUserId, sessionType, tenantCode) => {
	try {
		// Get private sessions where the deleted mentee was enrolled and session is in future
		const query = `
			SELECT s.id, s.title, s.mentor_id, s.start_date, s.end_date, s.type, s.created_by
			FROM sessions s
            LEFT JOIN session_attendees sa ON s.id = sa.session_id 
			AND s.tenant_code = sa.tenant_code
			WHERE sa.mentee_id = :menteeUserId
			AND s.type = :sessionType
			AND s.start_date > :currentTime
			AND s.deleted_at IS NULL
			AND s.tenant_code = :tenantCode
			AND sa.tenant_code = :tenantCode
		`

		const privateSessions = await Sequelize.query(query, {
			type: QueryTypes.SELECT,
			replacements: {
				menteeUserId,
				sessionType,
				currentTime: Math.floor(Date.now() / 1000),
				tenantCode,
			},
		})

		return privateSessions || []
	} catch (error) {
		console.error('Error in getUpcomingSessionsOfMentee:', error)
		return []
	}
}

exports.getUpcomingSessionsForMentor = async (mentorUserId, tenantCode) => {
	try {
		const currentTime = Math.floor(Date.now() / 1000)

		const upcomingSessions = await Session.findAll({
			where: {
				mentor_id: mentorUserId,
				tenant_code: tenantCode,
				start_date: { [Op.gt]: currentTime },
				deleted_at: null,
				created_by: {
					[Op.and]: [{ [Op.ne]: null }, { [Op.ne]: mentorUserId }],
				},
			},
			raw: true,
		})

		return upcomingSessions || []
	} catch (error) {
		return error
	}
}

exports.getSessionsAssignedToMentor = async (mentorUserId, tenantCode) => {
	try {
		// Citus requires tenant_code in JOIN ON clause for co-located distributed tables
		const query = `
				SELECT s.*, sa.mentee_id
				FROM ${Session.tableName} s
				LEFT JOIN session_attendees sa ON s.id = sa.session_id
				AND s.tenant_code = sa.tenant_code
				WHERE s.mentor_id = :mentorUserId
				AND s.start_date > :currentTime
				AND s.deleted_at IS NULL
				AND s.tenant_code = :tenantCode
			`

		const sessionsToDelete = await Sequelize.query(query, {
			type: QueryTypes.SELECT,
			replacements: {
				mentorUserId,
				currentTime: Math.floor(Date.now() / 1000),
				tenantCode,
			},
		})

		return sessionsToDelete
	} catch (error) {
		throw error
	}
}

exports.addOwnership = async (sessionId, mentorId) => {
	try {
		// Update session to assign mentor directly
		await Session.update({ mentor_id: mentorId }, { where: { id: sessionId } })
		return true
	} catch (error) {
		return error
	}
}

/**
 * Find all sessions with pagination and search filtering
 * Simple database query without business logic enrichment
 * @method
 * @name findAllSessions
 * @param {Number} page - pagination page number
 * @param {Number} limit - pagination limit
 * @param {String} search - search term
 * @param {Object} filters - filter conditions
 * @param {String} tenantCode - tenant code for isolation
 * @returns {Object} - sessions data without enrichment
 */
exports.findAllSessions = async (page, limit, search, filters, tenantCode) => {
	try {
		// Apply tenant isolation
		filters.tenant_code = tenantCode

		let filterQuery = {
			where: filters,
			raw: true,
			attributes: [
				'id',
				'title',
				'mentor_id',
				'description',
				'status',
				'start_date',
				'end_date',
				'image',
				'created_at',
				'meeting_info',
				'created_by',
				'mentor_organization_id',
				'meta',
				'seats_remaining',
				'seats_limit',
			],
			offset: parseInt((page - 1) * limit, 10),
			limit: parseInt(limit, 10),
			order: [['created_at', 'DESC']],
		}

		// Add search conditions if provided
		if (search) {
			filterQuery.where[Op.or] = [
				{ title: { [Op.iLike]: `%${search}%` } },
				{ description: { [Op.iLike]: `%${search}%` } },
			]
		}

		return await Session.findAndCountAll(filterQuery)
	} catch (err) {
		throw err
	}
}
