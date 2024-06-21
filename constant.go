package bqwt

const (
	TableInfoStandardSQL = "SELECT table_schema AS datasetId, table_name As tableId, creation_time AS created, storage_last_modified_time AS lastModified " +
		"FROM `%s.INFORMATION_SCHEMA.TABLE_STORAGE` " +
		"WHERE storage_last_modified_time > TIMESTAMP_MILLIS(%v) " +
		"AND table_schema = '%s' " +
		"ORDER BY storage_last_modified_time DESC"

	LastModifiedTableStandardSQL = "SELECT table_schema AS datasetId, table_name As tableId, creation_time AS created, storage_last_modified_time AS lastModified " +
		"FROM `%s.INFORMATION_SCHEMA.TABLE_STORAGE` " +
		"WHERE table_schema = '%s' " +
		"ORDER BY storage_last_modified_time DESC LIMIT 1"

	TableInfoLegacySQL = `SELECT dataset_id AS datasetId, table_id As tableId, creation_time AS created, last_modified_time AS lastModified 
		 	 FROM [%s.__TABLES__] 
			 WHERE last_modified_time > %v
			 ORDER BY last_modified_time DESC`

	LastModifiedTableLegacySQL = `SELECT dataset_id AS datasetId, table_id As tableId, creation_time AS created, last_modified_time AS lastModified 
			FROM [%s.__TABLES__] 
			ORDER BY last_modified_time DESC
			LIMIT 1`
)
