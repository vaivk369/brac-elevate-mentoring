'use strict'

require('module-alias/register')
require('dotenv').config()
const common = require('@constants/common')
const Permissions = require('@database/models/index').Permission

const POSTPermissionId = async (module, request_type, api_path) => {
	try {
		const permission = await Permissions.findOne({
			where: { module, request_type, api_path },
		})
		if (!permission) {
			throw new Error(`Permission not found for ${module} ${request_type} ${api_path}`)
		}
		return permission.id
	} catch (error) {
		throw error
	}
}

module.exports = {
	async up(queryInterface, Sequelize) {
		try {
			const rolePermissionsData = [
				{
					role_title: common.ORG_ADMIN_ROLE,
					permission_id: await POSTPermissionId(
						'sessionApprovalRequests',
						['POST'],
						'/mentoring/v1/sessionApprovalRequests/create'
					),
					module: 'sessionApprovalRequests',
					request_type: ['POST'],
					api_path: '/mentoring/v1/sessionApprovalRequests/create',
					created_at: new Date(),
					updated_at: new Date(),
					created_by: 0,
				},
				{
					role_title: common.TENANT_ADMIN_ROLE,
					permission_id: await POSTPermissionId(
						'sessionApprovalRequests',
						['GET'],
						'/mentoring/v1/sessionApprovalRequests/list'
					),
					module: 'sessionApprovalRequests',
					request_type: ['GET'],
					api_path: '/mentoring/v1/sessionApprovalRequests/list',
					created_at: new Date(),
					updated_at: new Date(),
					created_by: 0,
				},
				{
					role_title: common.TENANT_ADMIN_ROLE,
					permission_id: await POSTPermissionId(
						'sessionApprovalRequests',
						['POST'],
						'/mentoring/v1/sessionApprovalRequests/decision'
					),
					module: 'sessionApprovalRequests',
					request_type: ['POST'],
					api_path: '/mentoring/v1/sessionApprovalRequests/decision',
					created_at: new Date(),
					updated_at: new Date(),
					created_by: 0,
				},
			]
			await queryInterface.bulkInsert('role_permission_mapping', rolePermissionsData)
		} catch (error) {
			console.error(error)
		}
	},

	down: async (queryInterface, Sequelize) => {
		await queryInterface.bulkDelete('role_permission_mapping', { module: 'sessionApprovalRequests' }, {})
	},
}
