const drill = require('./drill.js')
const { formatSchemaQueryResults } = require('../utils')

const id = 'drill'
const name = 'Apache Drill'

function getDrillSchemaSql(connection) {
  /*const drillConfig = {
    host: connection.host,
    port: connection.port,
    user: connection.username,
    password: connection.password,
    defaultPlugin: connection.database,
    defaultSchema: connection.schema,
    ssl: connection.ssl || false
  }
  const client = new drill.Client(drillConfig)

  let pt = client.getPluginType(connection.database)

  if( pt == 'file'){
    console.log("Victory")
  } else {
    console.log( 'Other:' + pt + " " + connection.database)
  }*/

  let db = connection.database
  if (connection.schema.length > 0) {
    db = db + '.' + connection.schema
  } else {
    db = db + '.%'
  }
  return `
   SELECT table_schema, table_name, column_name, data_type
    FROM 
      INFORMATION_SCHEMA.\`COLUMNS\`
WHERE
table_schema LIKE '${db}'

ORDER BY 
      table_schema, 
      table_name, 
      ordinal_position
  `
}

/**
 * Simple request function
 * From: https://medium.freecodecamp.org/javascript-from-callbacks-to-async-await-1cc090ddad99
 * @param url
 * @returns {Promise<any>}
 */
function request(url) {
  return new Promise(function(resolve, reject) {
    const xhr = new XMLHttpRequest()
    xhr.timeout = 2000
    xhr.onreadystatechange = function(e) {
      if (xhr.readyState === 4) {
        if (xhr.status === 200) {
          resolve(xhr.response)
        } else {
          reject(xhr.status)
        }
      }
    }
    xhr.ontimeout = function() {
      reject('timeout')
    }
    xhr.open('get', url, true)
    xhr.send()
  })
}

/**
 * Run query for connection
 * Should return { rows, incomplete }
 * @param {string} query
 * @param {object} connection
 */

function runQuery(query, connection) {
  let incomplete = false
  const rows = []
  const port = connection.port || 8047

  const drillConfig = {
    host: connection.host,
    port: connection.port,
    user: connection.username,
    password: connection.password,
    defaultPlugin: connection.database,
    defaultSchema: connection.schema,
    ssl: connection.ssl || false
  }
  const client = new drill.Client(drillConfig)

  return client.query(drillConfig, query).then(result => {
    if (!result) {
      throw new Error('No result returned')
    } else if (result.errorMessage && result.errorMessage.length > 0) {
      console.log('Error with query: ' + query)
      console.log(result.errorMessage)
      throw new Error(result.errorMessage.split('\n')[0])
    }
    if (result.length > connection.maxRows) {
      incomplete = true
      result['rows'] = result['rows'].slice(0, connection.maxRows)
    }
    for (let r = 0; r < result['rows'].length; r++) {
      const row = {}
      for (let c = 0; c < result['columns'].length; c++) {
        row[result['columns'][c]] = result['rows'][r][result['columns'][c]]
      }
      rows.push(row)
    }
    return { rows, incomplete }
  })
}

/**
 * Test connectivity of connection
 * @param {*} connection
 */
function testConnection(connection) {
  const query = "SELECT 'success'  FROM (VALUES(1))"
  return runQuery(query, connection)
}

/**
 * Get schema for connection
 * @param {*} connection
 */
function getSchema(connection) {
  const pluginType = 'jdbc'

  if (pluginType === 'jdbc') {
    const schemaSql = getDrillSchemaSql(connection)
    return runQuery(schemaSql, connection).then(queryResult =>
      formatSchemaQueryResults(queryResult)
    )
  } else {
    //Determine if the storage plugin is file based or not
    client.getPluginType(connection.database)
  }
}

const fields = [
  {
    key: 'host',
    formType: 'TEXT',
    label: 'Host/Server/IP Address'
  },
  {
    key: 'port',
    formType: 'TEXT',
    label: 'Port (optional)'
  },
  {
    key: 'username',
    formType: 'TEXT',
    label: 'Database Username'
  },
  {
    key: 'password',
    formType: 'PASSWORD',
    label: 'Database Password'
  },
  {
    key: 'database',
    formType: 'TEXT',
    label: 'Default Storage Plugin'
  },
  {
    key: 'schema',
    formType: 'TEXT',
    label: 'Default Workspace or Table'
  },
  {
    key: 'ssl',
    formType: 'CHECKBOX',
    label: 'Use SSL to connect to Drill'
  }
]

module.exports = {
  id,
  name,
  fields,
  getSchema,
  runQuery,
  testConnection
}
