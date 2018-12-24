const JDBC = require('jdbc')
const jinst = require('jdbc/lib/jinst')

const id = 'jdbc'
const name = 'Generic JDBC Connection'
const { formatSchemaQueryResults } = require('../utils')

function getDrillSchemaSql(catalog, schema) {
  const schemaSql = schema ? `AND table_schema = '${schema}'` : ''
  return `
    SELECT 
      c.table_schema, 
      c.table_name, 
      c.column_name, 
      c.data_type
    FROM 
      INFORMATION_SCHEMA.COLUMNS c
    WHERE
      table_catalog = '${catalog}'
      ${schemaSql}
    ORDER BY 
      c.table_schema, 
      c.table_name, 
      c.ordinal_position
  `
}

function runQuery(query, connection) {
  let incomplete = false
  let rows = []

  if (!jinst.isJvmCreated()) {
    jinst.addOption('-Xrs')
    jinst.setupClasspath([connection.driverPath])
  }

  const config = {
    url: connection.connection_string,
    drivername: connection.drivername,
    minpoolsize: 1,
    maxpoolsize: 100,
    user: connection.username,
    password: connection.password,
    properties: {}
  }
  return runQueryAsPromise(query, config)
    .then(resultSet => {
      if (!resultSet) {
        throw new Error('No results returned')
      } else {
        rows = resultSet
        return { rows, incomplete }
      }
    })
    .catch(error => {
      let errorRegex = /([A-Z]+\sERROR):(.+)/
      let errorMessage = ''
      if (errorRegex.test(error.toString())) {
        let errorParts = errorRegex.exec(error.toString())
        errorMessage = errorParts[0]
      }
      console.log(error)
      throw new Error(errorMessage)
    })
}

function testConnection(connection) {
  const query = "SELECT 'success' AS TestQuery"
  return runQuery(query, connection)
}

function getSchema(connection) {
  const schemaSql = getDrillSchemaSql(connection.drillCatalog)
  return runQuery(schemaSql, connection).then(queryResult =>
    formatSchemaQueryResults(queryResult)
  )
}

function runQueryAsPromise(sqlString, config) {
  // return a Promise. A promise takes a callback with a resolve and reject function
  // call reject whenever an error occurs, or resolve on final result to be returned
  // Promises only return the first resolve/reject called
  return new Promise((resolve, reject) => {
    const jdbcdb = new JDBC(config)
    jdbcdb.initialize(err => {
      if (err) {
        return reject(err)
      }
      jdbcdb.reserve(function(err, connObj) {
        if (connObj) {
          console.log('Using connection: ' + connObj.uuid)
          const conn = connObj.conn
          // Query the database.
          // Select statement example.
          conn.createStatement(function(err, statement) {
            if (err) {
              return reject(err)
            }
            statement.setFetchSize(100, function(err) {
              if (err) {
                return reject(err)
              }
              //  Execute a query
              statement.executeQuery(sqlString, function(err, resultset) {
                if (err) {
                  return reject(err)
                }
                resultset.toObjArray(function(err, results) {
                  if (err) {
                    return reject(err)
                  }
                  jdbcdb.release(connObj, function(err) {
                    if (err) {
                      return reject(err)
                    }
                    // TODO in SQLPad pools are not used and connections are closed immediately after query time
                    return resolve(results)
                  })
                })
              })
            })
          })
        }
      })
    })
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
