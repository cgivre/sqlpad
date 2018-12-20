const jdbc = require('jdbc')
const jinst = require('jdbc/lib/jinst')
const asyncjs = require('async')

const id = 'jdbc'
const name = 'Generic JDBC Connection'
const { formatSchemaQueryResults } = require('../utils')

function runQuery(query, connection) {
  if (!jinst.isJvmCreated()) {
    jinst.addOption('-Xrs')
    jinst.setupClasspath([
      '/usr/local/share/drill/jars/jdbc-driver/drill-jdbc-all-1.12.0.jar'
    ]) // TODO Replace with info from  config
  }
  const rows = []
  let incomplete = false
  const config = {
    user: connection.username,
    password: connection.password,
    //connection_string: connection.url,
    drivername: 'org.apache.drill.jdbc.Driver',
    url: 'jdbc:drill:drillbit=localhost:31010'
  }
  // TODO use connection pool
  // TODO handle connection.maxRows
  jdbcConnection = new jdbc(config)

  /*jdbcConnection.initialize(function (err) {
    if (err) {
      console.log(err)
    }
    console.log('Connection initialized')
    console.log(jdbcConnection)
  })*/
  return initialize(jdbcConnection)
    .then(reserve(jdbcConnection))
    .then(createStatement(jdbcConnection))
}

function testConnection(connection) {
  const query = "SELECT 'success' AS TestQuery;"
  return runQuery(query, connection)
}

function getSchema(connection) {
  const schema_sql = connection.schema_sql
    ? connection.schema_sql
    : SCHEMA_SQL_INFORMATION_SCHEMA
  return runQuery(schema_sql, connection).then(queryResult =>
    formatSchemaQueryResults(queryResult)
  )
}

function createStatement(conn) {
  return new Promise((resolve, reject) => {
    conn.createStatement(function(err, statement) {
      if (err) {
        throw new Error(err)
        reject(err)
      }
      console.log('Statement Created!')
      resolve(statement)
    })
  }).catch(() => {
    reject(err)
  })
}

function initialize(conn) {
  return new Promise((resolve, reject) => {
    conn.initialize(function(err) {
      if (err) {
        throw new Error(err)
      }
      console.log('Connection Initialized!')
      resolve(conn)
    })
  }).catch(() => {
    reject(err)
  })
}
function reserve(conn) {
  return new Promise((resolve, reject) => {
    console.log('Connection:')
    console.log(conn)
    conn.conn.reserve(function(err) {
      if (err) {
        throw new Error(err)
      }
      console.log('Connection Reserved!')
      resolve(conn)
    })
  }).catch(() => {
    reject(err)
  })
}

const fields = [
  {
    key: 'connection_string',
    formType: 'TEXT',
    label: 'JDBC URL'
  },
  {
    key: 'drivername',
    formType: 'TEXT',
    label: 'JDBC Driver'
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
    key: 'driverPath',
    formType: 'TEXT',
    label: '(Optional) Path to Driver'
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
