'use strict'
module.exports = (sequelize, DataTypes) => {
	const SessionApprovalRequest = sequelize.define(
		'SessionApprovalRequest',
		{
			id: {
				allowNull: false,
				autoIncrement: true,
				primaryKey: true,
				type: DataTypes.INTEGER,
			},
			requestor_id: {
				type: DataTypes.STRING,
				allowNull: false,
			},
			supervisor_id: {
				type: DataTypes.STRING,
				allowNull: false,
			},
			organization_id: {
				type: DataTypes.STRING,
			},
			organization_code: {
				type: DataTypes.STRING,
			},
			status: {
				type: DataTypes.STRING,
				allowNull: false,
			},
			payload: {
				type: DataTypes.JSON,
				allowNull: false,
			},
			session_id: {
				type: DataTypes.STRING,
			},
			reject_reason: {
				type: DataTypes.STRING,
			},
			tenant_code: {
				type: DataTypes.STRING,
				allowNull: false,
			},
			created_by: {
				type: DataTypes.STRING,
				allowNull: false,
			},
			updated_by: {
				type: DataTypes.STRING,
			},
			created_at: {
				allowNull: false,
				type: DataTypes.DATE,
				defaultValue: DataTypes.NOW,
			},
			updated_at: {
				allowNull: false,
				type: DataTypes.DATE,
				defaultValue: DataTypes.NOW,
			},
			deleted_at: {
				type: DataTypes.DATE,
			},
		},
		{
			sequelize,
			modelName: 'SessionApprovalRequest',
			tableName: 'session_approval_request',
			freezeTableName: true,
			paranoid: true,
			indexes: [
				{
					fields: ['requestor_id'],
					name: 'index_requestor_id_session_approval_request',
				},
				{
					fields: ['supervisor_id'],
					name: 'index_supervisor_id_session_approval_request',
				},
				{
					fields: ['status'],
					name: 'index_status_session_approval_request',
				},
			],
		}
	)

	return SessionApprovalRequest
}
