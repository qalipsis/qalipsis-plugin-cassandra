package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.type.DataType
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import com.datastax.oss.protocol.internal.ProtocolConstants


/**
 * Naive manual mapper based on default codecs in the driver.
 * @see [registry code](https://github.com/datastax/java-driver/tree/4.x/core/src/main/java/com/datastax/oss/driver/internal/core/type/codec/registry)
 *
 * @author Maxim Golokhov
 */
internal fun getJavaTypeFromCqlType(cqlType: DataType): GenericType<*> {
    return when (cqlType.protocolCode) {
        ProtocolConstants.DataType.CUSTOM, ProtocolConstants.DataType.BLOB -> GenericType.BYTE_BUFFER
        ProtocolConstants.DataType.ASCII, ProtocolConstants.DataType.VARCHAR -> GenericType.STRING
        ProtocolConstants.DataType.BIGINT, ProtocolConstants.DataType.COUNTER -> GenericType.LONG
        ProtocolConstants.DataType.BOOLEAN -> GenericType.BOOLEAN
        ProtocolConstants.DataType.DECIMAL -> GenericType.BIG_DECIMAL
        ProtocolConstants.DataType.DOUBLE -> GenericType.DOUBLE
        ProtocolConstants.DataType.FLOAT -> GenericType.FLOAT
        ProtocolConstants.DataType.INT -> GenericType.INTEGER
        ProtocolConstants.DataType.TIMESTAMP -> GenericType.INSTANT
        ProtocolConstants.DataType.UUID, ProtocolConstants.DataType.TIMEUUID -> GenericType.UUID
        ProtocolConstants.DataType.VARINT -> GenericType.BIG_INTEGER
        ProtocolConstants.DataType.INET -> GenericType.INET_ADDRESS
        ProtocolConstants.DataType.DATE -> GenericType.LOCAL_DATE
        ProtocolConstants.DataType.TIME -> GenericType.LOCAL_TIME
        ProtocolConstants.DataType.SMALLINT -> GenericType.SHORT
        ProtocolConstants.DataType.TINYINT -> GenericType.BYTE
        ProtocolConstants.DataType.DURATION -> GenericType.CQL_DURATION
        ProtocolConstants.DataType.UDT -> GenericType.UDT_VALUE
        ProtocolConstants.DataType.TUPLE -> GenericType.TUPLE_VALUE
        else -> throw CodecNotFoundException(cqlType, null)
    }
}