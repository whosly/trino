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
package io.trino.plugin.deltalake.transactionlog;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;

public class DeltaLakeParquetStatisticsUtils
{
    private static final Logger LOG = Logger.get(DeltaLakeParquetStatisticsUtils.class);

    private DeltaLakeParquetStatisticsUtils() {}

    public static boolean hasInvalidStatistics(Collection<ColumnChunkMetaData> metadataList)
    {
        return metadataList.stream()
                .anyMatch(metadata ->
                        // If any row group does not have stats collected, stats for the file will not be valid
                        !metadata.getStatistics().isNumNullsSet() || metadata.getStatistics().isEmpty() ||
                        // Columns with NaN values are marked by `hasNonNullValue` = false by the Parquet reader. See issue: https://issues.apache.org/jira/browse/PARQUET-1246
                        (!metadata.getStatistics().hasNonNullValue() && metadata.getStatistics().getNumNulls() != metadata.getValueCount()));
    }

    @Nullable
    public static Object jsonValueToTrinoValue(Type type, @Nullable Object jsonValue)
    {
        if (jsonValue == null) {
            return null;
        }

        if (type == SMALLINT || type == TINYINT || type == INTEGER) {
            return (long) (int) jsonValue;
        }
        if (type == BIGINT) {
            return (long) (int) jsonValue;
        }
        if (type == REAL) {
            return (long) floatToRawIntBits((float) (double) jsonValue);
        }
        if (type == DOUBLE) {
            return jsonValue;
        }
        if (type instanceof DecimalType) {
            BigDecimal decimal;
            checkArgument(jsonValue instanceof String || jsonValue instanceof Double, "Value must be instance of String or Double");
            if (jsonValue instanceof String) {
                decimal = new BigDecimal((String) jsonValue);
            }
            else {
                decimal = BigDecimal.valueOf((double) jsonValue);
            }

            if (isShortDecimal(type)) {
                return Decimals.encodeShortScaledValue(decimal, ((DecimalType) type).getScale());
            }
            return Decimals.encodeScaledValue(decimal, ((DecimalType) type).getScale());
        }
        if (type instanceof VarcharType) {
            return jsonValue;
        }
        if (type == DateType.DATE) {
            return LocalDate.parse((String) jsonValue).toEpochDay();
        }
        if (type == TIMESTAMP_MILLIS) {
            return Instant.parse((String) jsonValue).toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
        }
        if (type instanceof RowType rowType) {
            Map<?, ?> values = (Map<?, ?>) jsonValue;
            List<Type> fieldTypes = rowType.getTypeParameters();
            BlockBuilder blockBuilder = new RowBlockBuilder(fieldTypes, null, 1);
            BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
            for (int i = 0; i < values.size(); ++i) {
                Type fieldType = fieldTypes.get(i);
                Object fieldValue = jsonValueToTrinoValue(fieldType, values.get(rowType.getFields().get(i).getName().orElseThrow()));
                writeNativeValue(fieldType, singleRowBlockWriter, fieldValue);
            }

            blockBuilder.closeEntry();
            return blockBuilder.build();
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static Map<String, Object> toJsonValues(Map<String, Type> columnTypeMapping, Map<String, Object> values)
    {
        Map<String, Object> jsonValues = new HashMap<>();
        for (Map.Entry<String, Object> value : values.entrySet()) {
            Type type = columnTypeMapping.get(value.getKey());
            // TODO: Add support for row type
            if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
                continue;
            }
            jsonValues.put(value.getKey(), toJsonValue(columnTypeMapping.get(value.getKey()), value.getValue()));
        }
        return jsonValues;
    }

    @Nullable
    private static Object toJsonValue(Type type, @Nullable Object value)
    {
        if (value == null) {
            return null;
        }

        if (type == SMALLINT || type == TINYINT || type == INTEGER || type == BIGINT) {
            return value;
        }
        if (type == REAL) {
            return intBitsToFloat(toIntExact((long) value));
        }
        if (type == DOUBLE) {
            return value;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return Decimals.toString((long) value, decimalType.getScale());
            }
            return Decimals.toString((Int128) value, decimalType.getScale());
        }

        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        if (type == DateType.DATE) {
            return LocalDate.ofEpochDay((long) value).format(ISO_LOCAL_DATE);
        }
        if (type == TIMESTAMP_TZ_MILLIS) {
            Instant ts = Instant.ofEpochMilli(unpackMillisUtc((long) value));
            return ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC));
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static Map<String, Object> jsonEncodeMin(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn)
    {
        return jsonEncode(stats, typeForColumn, DeltaLakeParquetStatisticsUtils::getMin);
    }

    public static Map<String, Object> jsonEncodeMax(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn)
    {
        return jsonEncode(stats, typeForColumn, DeltaLakeParquetStatisticsUtils::getMax);
    }

    private static Map<String, Object> jsonEncode(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn, BiFunction<Type, Statistics<?>, Optional<Object>> accessor)
    {
        Map<String, Optional<Object>> allStats = stats.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().isPresent() && !entry.getValue().get().isEmpty())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> accessor.apply(typeForColumn.get(entry.getKey()), entry.getValue().get())));

        return allStats.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().isPresent())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    private static Optional<Object> getMin(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMin() == null || !statistics.hasNonNullValue()) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMin());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics) {
                Instant ts = Instant.ofEpochMilli(((LongStatistics) statistics).genericGetMin());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            else if (statistics instanceof BinaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(((BinaryStatistics) statistics).genericGetMin());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.getEpochSeconds(), decodedTimestamp.getNanosOfSecond());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC).truncatedTo(MILLIS)));
            }
        }

        if (type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) {
            checkArgument(statistics instanceof IntStatistics || statistics instanceof LongStatistics,
                    "Column with %s type contained invalid statistics: %s", type, statistics);
            return Optional.of(statistics.genericGetMin());
        }

        if (type.equals(REAL)) {
            checkArgument(statistics instanceof FloatStatistics, "Column with REAL type contained invalid statistics: %s", statistics);
            Float min = ((FloatStatistics) statistics).genericGetMin();
            return Optional.of(min.compareTo(-0.0f) == 0 ? 0.0f : min);
        }

        if (type.equals(DOUBLE)) {
            checkArgument(statistics instanceof DoubleStatistics, "Column with DOUBLE type contained invalid statistics: %s", statistics);
            Double min = ((DoubleStatistics) statistics).genericGetMin();
            return Optional.of(min.compareTo(-0.0d) == 0 ? 0.0d : min);
        }

        if (type instanceof DecimalType) {
            LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
            checkArgument(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, "DECIMAL column had invalid Parquet Logical Type: %s", logicalType);
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

            BigDecimal min;
            if (statistics instanceof IntStatistics) {
                min = BigDecimal.valueOf(((IntStatistics) statistics).getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            else if (statistics instanceof LongStatistics) {
                min = BigDecimal.valueOf(((LongStatistics) statistics).getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            else if (statistics instanceof BinaryStatistics) {
                BigInteger base = new BigInteger(((BinaryStatistics) statistics).genericGetMin().getBytes());
                min = new BigDecimal(base, scale);
                return Optional.of(min.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMin().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN)) {
            // Boolean columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.warn("Accumulating Parquet statistics with Trino type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }

    private static Optional<Object> getMax(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMax() == null || !statistics.hasNonNullValue()) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMax());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics) {
                Instant ts = Instant.ofEpochMilli(((LongStatistics) statistics).genericGetMax());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            else if (statistics instanceof BinaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(((BinaryStatistics) statistics).genericGetMax());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.getEpochSeconds(), decodedTimestamp.getNanosOfSecond());
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(ts, UTC);
                ZonedDateTime truncatedToMillis = zonedDateTime.truncatedTo(MILLIS);
                if (truncatedToMillis.isBefore(zonedDateTime)) {
                    truncatedToMillis = truncatedToMillis.plus(1, MILLIS);
                }
                return Optional.of(ISO_INSTANT.format(truncatedToMillis));
            }
        }

        if (type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) {
            checkArgument(statistics instanceof IntStatistics || statistics instanceof LongStatistics,
                    "Column with %s type contained invalid statistics: %s", type, statistics);
            return Optional.of(statistics.genericGetMax());
        }

        if (type.equals(REAL)) {
            checkArgument(statistics instanceof FloatStatistics, "Column with REAL type contained invalid statistics: %s", statistics);
            return Optional.of(((FloatStatistics) statistics).genericGetMax());
        }

        if (type.equals(DOUBLE)) {
            checkArgument(statistics instanceof DoubleStatistics, "Column with DOUBLE type contained invalid statistics: %s", statistics);
            return Optional.of(((DoubleStatistics) statistics).genericGetMax());
        }

        if (type instanceof DecimalType) {
            LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
            checkArgument(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, "DECIMAL column had invalid Parquet Logical Type: %s", logicalType);
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

            BigDecimal max;
            if (statistics instanceof IntStatistics) {
                max = BigDecimal.valueOf(((IntStatistics) statistics).getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            else if (statistics instanceof LongStatistics) {
                max = BigDecimal.valueOf(((LongStatistics) statistics).getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            else if (statistics instanceof BinaryStatistics) {
                BigInteger base = new BigInteger(((BinaryStatistics) statistics).genericGetMax().getBytes());
                max = new BigDecimal(base, scale);
                return Optional.of(max.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMax().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN)) {
            // Boolean columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.warn("Accumulating Parquet statistics with Trino type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }
}
