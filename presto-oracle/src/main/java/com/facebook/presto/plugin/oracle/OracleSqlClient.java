/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

// Static imports
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;

/*
// import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
// import com.facebook.presto.spi.TableNotFoundException;
// import com.google.common.collect.ImmutableList;
// import oracle.jdbc.OracleResultSetMetaData;
// import java.sql.ResultSetMetaData;
// import java.sql.Types;
// import java.util.ArrayList;
// import java.util.List;

// import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
*/
public class OracleSqlClient
        extends BaseJdbcClient
{
    @Inject
    public OracleSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "", new DriverConnectionFactory(new OracleDriver(), config));
        System.setProperty("oracle.jdbc.J2EE13Compliant", "true");
    }
/*
    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            //If the table is mapped to another user you will need to get the synonym to that table
            //So, in this case, is mandatory to use setIncludeSynonyms
            // ((oracle.jdbc.driver.OracleConnection) connection).setIncludeSynonyms(true);
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
                List<JdbcColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    int columnDataType = resultSet.getInt("DATA_TYPE");
                    int columnPosition = resultSet.getInt("ORDINAL_POSITION");
                    int columnScale = resultSet.getInt("DECIMAL_DIGITS");
                    int columnPrecision = resultSet.getInt("COLUMN_SIZE");
                    Boolean columnHasVariableScale = ((OracleResultSetMetaData) resultSetMetadata).isVariableScale(columnPosition);
                    if (columnDataType == Types.NUMERIC) {
                        if (columnScale == 0 && columnPrecision < 19 && columnPrecision != 0) { // Integer Type
                            if (columnPrecision > 9) {
                                columnDataType = Types.BIGINT;
                            }
                            else if (columnPrecision > 4) {
                                columnDataType = Types.INTEGER;
                            }
                            else if (columnPrecision > 2) {
                                columnDataType = Types.SMALLINT;
                            }
                            else {
                                columnDataType = Types.TINYINT;
                            }
                        }
                        else {
                            columnDataType = Types.DECIMAL; // Let the system deal with a decimal
                            if (columnHasVariableScale) {
                                columnScale = columnPrecision;
                            }
                        }
                    }
                    found = true;
                    Type columnType = toPrestoType(
                            columnDataType,
                            columnPrecision,
                            columnScale);
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                    }
                } // while (resultSet.next())
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
*/
    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    // Oracle only supports 30 byte identifiers in versions 12.1 and below
    // and 128 byte identifiers in versions 12.2 and newer.
    // Using a 30 character identifier for compatibility
    // the basejdbcclient version of this method is 43-characters.
    // note: requires presto 0.194 or newer for this method to be called
    @Override
    protected String generateTemporaryTableName()
    {
        return "tmp_presto_" + Long.toString(System.nanoTime(), Character.MAX_RADIX);
    }

    private static final Map<Type, String> OSQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "number")
            .put(INTEGER, "number")
            .put(SMALLINT, "number")
            .put(TINYINT, "number")
            .put(DOUBLE, "number")
            .put(REAL, "number")
            .put(VARBINARY, "raw")
            .put(DATE, "date")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();

    // Override presto types to oracle types
    @Override
    protected String toSqlType(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }
        if (type instanceof DecimalType) {
            return format("number(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
        }

        String sqlType = OSQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // ((oracle.jdbc.driver.OracleConnection) connection).setRestrictGetTables(true);
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }
}
