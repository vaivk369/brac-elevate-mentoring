'use strict'

/** @type {import('sequelize-cli').Migration} */
module.exports = {
	async up(queryInterface, Sequelize) {
		const modulesData = [
			{ code: 'sessionApprovalRequests', status: 'ACTIVE', created_at: new Date(), updated_at: new Date() },
		]

		// Insert the data into the 'modules' table
		await queryInterface.bulkInsert('modules', modulesData)
	},

	async down(queryInterface, Sequelize) {
		await queryInterface.bulkDelete('modules', { code: 'sessionApprovalRequests' }, {})
	},
}
