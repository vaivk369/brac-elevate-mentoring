/**
 * name : project.js
 * author : Claude
 * created-date : 25-Aug-2026
 * Description : Internal calls to sl-project-service.
 *
 * NOTE: sl-project-service's authenticator checks the `internal-access-token`
 * (hyphenated) request header, whereas the shared `@generics/requests` helper
 * sends `internal_access_token` (underscored) - which sl-project-service does
 * not recognize. To avoid breaking existing integrations that rely on the
 * shared helper's underscored header, this module talks to sl-project-service
 * directly using the `request` package with the correct hyphenated header.
 */

// Dependencies
const request = require('request')
const endpoints = require('@constants/endpoints')

const projectBaseUrl = process.env.PROJECT_SERVICE_HOST + process.env.PROJECT_SERVICE_BASE_URL

/**
 * Resolve the supervisor (hierarchy level 0) for a user within a program, via
 * sl-project-service's internal `changeRequests/resolveSupervisor` endpoint.
 * @method
 * @name resolveSupervisor
 * @param {String} userId - user id (LC) to resolve the supervisor for.
 * @param {String} tenantId - tenant id.
 * @param {String} [programId] - program id. Defaults to process.env.SUPERVISOR_RESOLUTION_PROGRAM_ID.
 * @returns {Promise<String|null>} - resolved supervisor id, or null if not found.
 */
const resolveSupervisor = function (userId, tenantId, programId) {
	return new Promise((resolve, reject) => {
		const apiUrl = projectBaseUrl + endpoints.RESOLVE_SUPERVISOR

		const options = {
			headers: {
				'Content-Type': 'application/json',
				'internal-access-token': process.env.INTERNAL_ACCESS_TOKEN,
			},
			json: {
				userId,
				tenantId,
				programId: programId || process.env.SUPERVISOR_RESOLUTION_PROGRAM_ID,
			},
		}

		try {
			request.post(apiUrl, options, (err, data) => {
				if (err) {
					return reject({
						message: 'PROJECT_SERVICE_DOWN',
						error: err,
					})
				}

				const supervisorId = data?.body?.result?.supervisorId || null
				return resolve(supervisorId)
			})
		} catch (error) {
			return reject(error)
		}
	})
}

module.exports = {
	resolveSupervisor,
}
