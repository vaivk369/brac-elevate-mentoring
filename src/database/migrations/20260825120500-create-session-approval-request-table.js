'use strict'
/** @type {import('sequelize-cli').Migration} */
module.exports = {
	async up(queryInterface, Sequelize) {
		await queryInterface.createTable('session_approval_request', {
			id: {
				type: Sequelize.INTEGER,
				autoIncrement: true,
				primaryKey: true,
			},
			requestor_id: {
				type: Sequelize.STRING,
				allowNull: false,
			},
			supervisor_id: {
				type: Sequelize.STRING,
				allowNull: false,
			},
			organization_id: {
				type: Sequelize.STRING,
			},
			organization_code: {
				type: Sequelize.STRING,
			},
			status: {
				type: Sequelize.STRING,
				allowNull: false,
			},
			payload: {
				type: Sequelize.JSON,
				allowNull: false,
			},
			session_id: {
				type: Sequelize.STRING,
			},
			reject_reason: {
				type: Sequelize.STRING,
			},
			tenant_code: {
				type: Sequelize.STRING,
				allowNull: false,
			},
			created_by: {
				type: Sequelize.STRING,
				allowNull: false,
			},
			updated_by: {
				type: Sequelize.STRING,
			},
			created_at: {
				type: Sequelize.DATE,
				allowNull: false,
			},
			updated_at: {
				type: Sequelize.DATE,
				allowNull: false,
			},
			deleted_at: {
				type: Sequelize.DATE,
				allowNull: true,
			},
		})
		await queryInterface.addIndex('session_approval_request', ['requestor_id'], {
			name: 'index_requestor_id_session_approval_request',
		})
		await queryInterface.addIndex('session_approval_request', ['supervisor_id'], {
			name: 'index_supervisor_id_session_approval_request',
		})
		await queryInterface.addIndex('session_approval_request', ['status'], {
			name: 'index_status_session_approval_request',
		})
	},

	async down(queryInterface, Sequelize) {
		await queryInterface.removeIndex('session_approval_request', 'index_status_session_approval_request')
		await queryInterface.removeIndex('session_approval_request', 'index_supervisor_id_session_approval_request')
		await queryInterface.removeIndex('session_approval_request', 'index_requestor_id_session_approval_request')

		await queryInterface.dropTable('session_approval_request')
	},
}
