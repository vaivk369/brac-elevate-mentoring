const requestSession = require('@database/models/index').RequestSession
const sequelize = require('@database/models/index').sequelize
const { Op } = require('sequelize')
const common = require('@constants/common')

exports.addSessionRequest = async (requesteeId, requestId) => {
	try {
		// This method is no longer needed since data is directly stored in session_request table
		// Keeping for backward compatibility
		return { success: true, message: 'Session request mapping no longer required' }
	} catch (error) {
		throw error
	}
}

exports.getSessionsMapping = async (userId, status, tenantCode) => {
	try {
		const strUserId = String(userId)
		const escapedUserId = sequelize.escape(strUserId)

		let statusList = []
		if (!status || status.length == 0) {
			statusList = [
				common.CONNECTIONS_STATUS.ACCEPTED,
				common.CONNECTIONS_STATUS.REQUESTED,
				common.CONNECTIONS_STATUS.REJECTED,
				common.CONNECTIONS_STATUS.EXPIRED,
			]
		} else {
			statusList = status
		}

		const statusConditions = []
		// Scenario A: REQUESTED status
		// 1. PUBLIC or GROUP and status is REQUESTED and userId is not in rejected_requestees
		// 2. SPECIFIC and status is REQUESTED and requestee_id is userId
		if (statusList.includes(common.CONNECTIONS_STATUS.REQUESTED)) {
			statusConditions.push({
				status: common.CONNECTIONS_STATUS.REQUESTED,
				//requestor_id: { [Op.ne]: strUserId },
				[Op.or]: [
					{
						assignment_type: { [Op.in]: ['PUBLIC', 'GROUP'] },
						[Op.and]: [
							sequelize.literal(
								`NOT (ARRAY[${escapedUserId}]::text[] && COALESCE(rejected_requestees, '{}')::text[])`
							),
						],
					},
					{
						assignment_type: 'SPECIFIC',
						requestee_id: strUserId,
					},
				],
			})
		}

		// Scenario B: ACCEPTED status
		// status = ACCEPTED and requestee_id is userId (irrespective of assignment_type)
		if (statusList.includes(common.CONNECTIONS_STATUS.ACCEPTED)) {
			statusConditions.push({
				status: common.CONNECTIONS_STATUS.ACCEPTED,
				requestee_id: strUserId,
			})
		}

		// Scenario C: REJECTED status
		// status = REJECTED && requestee_id = userId OR userId IN rejected_requestees
		if (statusList.includes(common.CONNECTIONS_STATUS.REJECTED)) {
			statusConditions.push({
				[Op.or]: [
					{
						status: common.CONNECTIONS_STATUS.REJECTED,
						requestee_id: strUserId,
					},
					sequelize.literal(`ARRAY[${escapedUserId}]::text[] && COALESCE(rejected_requestees, '{}')::text[]`),
				],
			})
		}

		if (statusList.includes(common.CONNECTIONS_STATUS.EXPIRED)) {
			statusConditions.push({
				[Op.or]: [
					{
						status: common.CONNECTIONS_STATUS.EXPIRED,
						requestee_id: strUserId,
					},
				],
			})
		}

		return await requestSession.findAll({
			where: {
				tenant_code: tenantCode,
				[Op.or]: statusConditions,
			},
			raw: true,
			order: [['created_at', 'DESC']],
		})
	} catch (error) {
		throw error
	}
}
