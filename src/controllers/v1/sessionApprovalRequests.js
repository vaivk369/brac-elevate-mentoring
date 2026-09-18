const sessionApprovalRequestsService = require('@services/sessionApprovalRequests')
const { isAMentor } = require('@generics/utils')
const common = require('@constants/common')

module.exports = class sessionApprovalRequests {
	/**
	 * Submit a session-approval request. Called internally by sessions.js's
	 * approval gate - not intended to be invoked directly by clients.
	 */
	async create(req) {
		try {
			return await sessionApprovalRequestsService.create(
				req.body,
				req.decodedToken.id,
				req.decodedToken.organization_id,
				req.decodedToken.organization_code,
				isAMentor(req.decodedToken.roles),
				true,
				req.decodedToken.tenant_code
			)
		} catch (error) {
			return error
		}
	}

	/**
	 * List session-approval requests assigned to the logged-in supervisor.
	 */
	async list(req) {
		try {
			return await sessionApprovalRequestsService.list(
				req.decodedToken.id,
				req.query.status,
				req.query.pageNo,
				req.query.pageSize,
				req.decodedToken.tenant_code
			)
		} catch (error) {
			return error
		}
	}

	/**
	 * Approve or reject a pending session-approval request.
	 */
	async decision(req) {
		try {
			const decision =
				req.body.decision === common.CONNECTIONS_STATUS.REJECTED
					? common.CONNECTIONS_STATUS.REJECTED
					: common.CONNECTIONS_STATUS.ACCEPTED

			return await sessionApprovalRequestsService.decision(
				req.body.id,
				req.decodedToken.id,
				decision,
				req.body.reason,
				req.decodedToken.tenant_code
			)
		} catch (error) {
			return error
		}
	}
}
