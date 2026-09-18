/**
 * name : services/sessionApprovalRequests.js
 * Description : Supervisor-approval workflow for LC (org_admin) requested sessions
 *                whose support_offering_type is 'asset'. An LC's session request is
 *                held as a pending SessionApprovalRequest until their resolved
 *                supervisor (tenant_admin) approves or rejects it. On approval, the
 *                original session-creation call is replayed (bypassing the gate).
 */

const sessionApprovalRequestQueries = require('@database/queries/sessionApprovalRequests')
const userExtensionQueries = require('@database/queries/userExtension')
const common = require('@constants/common')
const responses = require('@helpers/responses')
const httpStatusCode = require('@generics/http-status')
const projectRequests = require('@requests/project')
const kafkaCommunication = require('@generics/kafka-communication')
const utils = require('@generics/utils')
const cacheHelper = require('@generics/cacheHelper')

module.exports = class sessionApprovalRequestsHelper {
	/**
	 * Submit a session-approval request on behalf of an LC, resolving their
	 * supervisor and storing the original session-creation call for replay.
	 */
	static async create(bodyData, loggedInUserId, orgId, orgCode, isAMentor, notifyUser, tenantCode) {
		try {
			const pendingRequest = await sessionApprovalRequestQueries.findPendingByRequestor(
				loggedInUserId,
				tenantCode
			)
			if (pendingRequest) {
				return responses.failureResponse({
					statusCode: httpStatusCode.bad_request,
					responseCode: 'CLIENT_ERROR',
					message: 'SESSION_APPROVAL_REQUEST_ALREADY_PENDING',
				})
			}

			let supervisorId = null
			try {
				supervisorId = await projectRequests.resolveSupervisor(loggedInUserId, tenantCode)
			} catch (error) {
				supervisorId = null
			}

			if (!supervisorId) {
				return responses.failureResponse({
					statusCode: httpStatusCode.bad_request,
					responseCode: 'CLIENT_ERROR',
					message: 'SUPERVISOR_NOT_FOUND',
				})
			}

			const payload = {
				bodyData,
				loggedInUserId,
				orgId,
				orgCode,
				isAMentor,
				notifyUser,
				tenantCode,
			}

			const request = await sessionApprovalRequestQueries.create(
				loggedInUserId,
				supervisorId,
				orgId,
				orgCode,
				payload,
				tenantCode
			)

			// Best-effort notification; must not fail the request submission itself.
			try {
				await notifySupervisor(request, orgCode, tenantCode)
			} catch (error) {
				console.log('SESSION_APPROVAL_REQUEST supervisor notification failed', error)
			}

			return responses.successResponse({
				statusCode: httpStatusCode.created,
				message: 'SESSION_APPROVAL_REQUEST_SUBMITTED',
				result: request,
			})
		} catch (error) {
			throw error
		}
	}

	/**
	 * List session-approval requests assigned to a supervisor.
	 */
	static async list(supervisorId, status, page, pageSize, tenantCode) {
		try {
			const requests = await sessionApprovalRequestQueries.list(supervisorId, status, page, pageSize, tenantCode)
			return responses.successResponse({
				statusCode: httpStatusCode.ok,
				message: 'SESSION_APPROVAL_REQUESTS_LIST',
				result: {
					data: requests.rows,
					count: requests.count,
				},
			})
		} catch (error) {
			throw error
		}
	}

	/**
	 * Approve or reject a pending session-approval request. Only the resolved
	 * supervisor for that request may decide it. Approval replays the original
	 * session-creation call, bypassing the gate.
	 */
	static async decision(id, decidedBy, decision, reason, tenantCode) {
		try {
			const request = await sessionApprovalRequestQueries.findOneRequest(id, tenantCode)
			if (!request) {
				return responses.failureResponse({
					statusCode: httpStatusCode.not_found,
					responseCode: 'CLIENT_ERROR',
					message: 'SESSION_APPROVAL_REQUEST_NOT_FOUND',
				})
			}

			if (request.status !== common.CONNECTIONS_STATUS.REQUESTED) {
				return responses.failureResponse({
					statusCode: httpStatusCode.bad_request,
					responseCode: 'CLIENT_ERROR',
					message: 'SESSION_APPROVAL_REQUEST_ALREADY_PROCESSED',
				})
			}

			if (String(request.supervisor_id) !== String(decidedBy)) {
				return responses.failureResponse({
					statusCode: httpStatusCode.forbidden,
					responseCode: 'CLIENT_ERROR',
					message: 'NOT_AUTHORIZED_FOR_THIS_REQUEST',
				})
			}

			if (decision === common.CONNECTIONS_STATUS.REJECTED) {
				await sessionApprovalRequestQueries.reject(id, decidedBy, reason, tenantCode)

				try {
					await notifyRequestor(request, false, reason, tenantCode)
				} catch (error) {
					console.log('SESSION_APPROVAL_REQUEST requestor notification failed', error)
				}

				return responses.successResponse({
					statusCode: httpStatusCode.ok,
					message: 'SESSION_APPROVAL_REQUEST_REJECTED',
				})
			}

			// Approved: replay the original session-creation call, bypassing the gate.
			const sessionService = require('@services/sessions')
			const {
				bodyData,
				loggedInUserId,
				orgId,
				orgCode,
				isAMentor,
				notifyUser,
				tenantCode: payloadTenantCode,
			} = request.payload

			const sessionCreation = await sessionService.create(
				bodyData,
				loggedInUserId,
				orgId,
				orgCode,
				isAMentor,
				notifyUser,
				payloadTenantCode,
				[],
				true
			)

			if (sessionCreation.statusCode !== httpStatusCode.created) {
				return responses.failureResponse({
					statusCode: sessionCreation.statusCode || httpStatusCode.bad_request,
					message: sessionCreation.message || 'SESSION_CREATION_FAILED',
					result: sessionCreation.data || [],
				})
			}

			await sessionApprovalRequestQueries.approve(id, decidedBy, sessionCreation.result.id, tenantCode)

			try {
				await notifyRequestor(request, true, null, tenantCode)
			} catch (error) {
				console.log('SESSION_APPROVAL_REQUEST requestor notification failed', error)
			}

			return responses.successResponse({
				statusCode: httpStatusCode.created,
				message: 'SESSION_APPROVAL_REQUEST_APPROVED',
				result: sessionCreation.result,
			})
		} catch (error) {
			throw error
		}
	}
}

/**
 * Best-effort email notification to the supervisor on request creation. Skips
 * silently if no email template is configured, or if the supervisor has no
 * local user-extension row (e.g. a tenant_admin who has never acted as a
 * mentor/mentee in this app) to resolve an email address for.
 */
async function notifySupervisor(request, orgCode, tenantCode) {
	const templateCode = process.env.SESSION_APPROVAL_REQUEST_EMAIL_TEMPLATE
	if (!templateCode) return

	const [supervisorDetails, requestorDetails] = await Promise.all([
		userExtensionQueries.getUsersByUserIds(
			[request.supervisor_id],
			{ attributes: ['name', 'email'] },
			tenantCode,
			true
		),
		userExtensionQueries.getUsersByUserIds([request.requestor_id], { attributes: ['name'] }, tenantCode, true),
	])

	if (!supervisorDetails?.[0]?.email) return

	const templateData = await cacheHelper.notificationTemplates.get(tenantCode, orgCode, templateCode)
	if (!templateData) return

	const payload = {
		type: 'email',
		email: {
			to: supervisorDetails[0].email,
			subject: templateData.subject,
			body: utils.composeEmailBody(templateData.body, {
				name: supervisorDetails[0].name,
				requestorName: requestorDetails?.[0]?.name || '',
			}),
		},
	}
	await kafkaCommunication.pushEmailToKafka(payload)
}

/**
 * Best-effort email notification to the requestor (LC) on approve/reject.
 */
async function notifyRequestor(request, approved, reason, tenantCode) {
	const templateCode = approved
		? process.env.SESSION_APPROVAL_REQUEST_APPROVED_EMAIL_TEMPLATE
		: process.env.SESSION_APPROVAL_REQUEST_REJECTED_EMAIL_TEMPLATE
	if (!templateCode) return

	const requestorDetails = await userExtensionQueries.getUsersByUserIds(
		[request.requestor_id],
		{ attributes: ['name', 'email'] },
		tenantCode,
		true
	)

	if (!requestorDetails?.[0]?.email) return

	const templateData = await cacheHelper.notificationTemplates.get(
		tenantCode,
		request.organization_code,
		templateCode
	)
	if (!templateData) return

	const payload = {
		type: 'email',
		email: {
			to: requestorDetails[0].email,
			subject: templateData.subject,
			body: utils.composeEmailBody(templateData.body, {
				name: requestorDetails[0].name,
				reason: reason || '',
			}),
		},
	}
	await kafkaCommunication.pushEmailToKafka(payload)
}
