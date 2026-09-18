const sessionApprovalRequest = require('@database/models/index').SessionApprovalRequest
const { Op } = require('sequelize')
const common = require('@constants/common')

exports.create = async (requestorId, supervisorId, organizationId, organizationCode, payload, tenantCode) => {
	try {
		const data = {
			requestor_id: requestorId,
			supervisor_id: supervisorId,
			organization_id: organizationId,
			organization_code: organizationCode,
			status: common.CONNECTIONS_STATUS.REQUESTED,
			payload,
			created_by: requestorId,
			updated_by: requestorId,
			tenant_code: tenantCode,
		}

		const request = await sessionApprovalRequest.create(data)
		return request.get({ plain: true })
	} catch (error) {
		throw error
	}
}

exports.findPendingByRequestor = async (requestorId, tenantCode) => {
	try {
		return await sessionApprovalRequest.findOne({
			where: {
				requestor_id: requestorId,
				status: common.CONNECTIONS_STATUS.REQUESTED,
				tenant_code: tenantCode,
			},
			raw: true,
		})
	} catch (error) {
		throw error
	}
}

exports.findOneRequest = async (id, tenantCode) => {
	try {
		return await sessionApprovalRequest.findOne({
			where: {
				id,
				tenant_code: tenantCode,
			},
			raw: true,
		})
	} catch (error) {
		throw error
	}
}

exports.list = async (supervisorId, status, page, pageSize, tenantCode) => {
	try {
		const currentPage = page ? page : 1
		const limit = pageSize ? pageSize : 10
		const offset = (currentPage - 1) * limit

		const statusFilter = status
			? status
			: {
					[Op.in]: [
						common.CONNECTIONS_STATUS.REQUESTED,
						common.CONNECTIONS_STATUS.ACCEPTED,
						common.CONNECTIONS_STATUS.REJECTED,
					],
			  }

		return await sessionApprovalRequest.findAndCountAll({
			where: {
				supervisor_id: supervisorId,
				status: statusFilter,
				tenant_code: tenantCode,
			},
			raw: true,
			limit,
			offset,
			order: [['created_at', 'DESC']],
		})
	} catch (error) {
		throw error
	}
}

exports.approve = async (id, userId, sessionId, tenantCode) => {
	try {
		const updateData = {
			status: common.CONNECTIONS_STATUS.ACCEPTED,
			session_id: sessionId ? String(sessionId) : null,
			updated_by: userId,
		}

		const result = await sessionApprovalRequest.update(updateData, {
			where: {
				id,
				status: common.CONNECTIONS_STATUS.REQUESTED,
				tenant_code: tenantCode,
			},
			individualHooks: true,
		})

		return result[1]
	} catch (error) {
		throw error
	}
}

exports.reject = async (id, userId, rejectReason, tenantCode) => {
	try {
		const updateData = {
			status: common.CONNECTIONS_STATUS.REJECTED,
			updated_by: userId,
			reject_reason: rejectReason || null,
		}

		const result = await sessionApprovalRequest.update(updateData, {
			where: {
				id,
				status: common.CONNECTIONS_STATUS.REQUESTED,
				tenant_code: tenantCode,
			},
			individualHooks: true,
		})

		return result[1]
	} catch (error) {
		throw error
	}
}

module.exports = exports
