'use strict'

/** @type {import('sequelize-cli').Migration} */
module.exports = {
	async up(queryInterface, Sequelize) {
		try {
			const permissionsData = [
				{
					code: 'session_approval_requests_create_permission',
					module: 'sessionApprovalRequests',
					request_type: ['POST'],
					api_path: '/mentoring/v1/sessionApprovalRequests/create',
					status: 'ACTIVE',
					created_at: new Date(),
					updated_at: new Date(),
				},
				{
					code: 'session_approval_requests_list_permission',
					module: 'sessionApprovalRequests',
					request_type: ['GET'],
					api_path: '/mentoring/v1/sessionApprovalRequests/list',
					status: 'ACTIVE',
					created_at: new Date(),
					updated_at: new Date(),
				},
				{
					code: 'session_approval_requests_decision_permission',
					module: 'sessionApprovalRequests',
					request_type: ['POST'],
					api_path: '/mentoring/v1/sessionApprovalRequests/decision',
					status: 'ACTIVE',
					created_at: new Date(),
					updated_at: new Date(),
				},
			]
			await queryInterface.bulkInsert('permissions', permissionsData)
		} catch (error) {
			console.log(error)
		}
	},

	down: async (queryInterface, Sequelize) => {
		await queryInterface.bulkDelete('permissions', { module: 'sessionApprovalRequests' }, {})
	},
}
