package org.apache.spark.sql.hive

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}

import scala.collection.JavaConversions._

/**
 * Wraps with Hive types based on object inspector.
 * TODO: Consolidate all hive OI/data interface code.
 */
private[hive] object HadoopTypeConverter extends HiveInspectors {
  def unwrappers(fieldRefs: Seq[StructField]): Seq[(Any, MutableRow, Int) => Unit] = fieldRefs.map {
    _.getFieldObjectInspector match {
      case oi: BooleanObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
      case oi: ByteObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
      case oi: ShortObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
      case oi: IntObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
      case oi: LongObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
      case oi: FloatObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
      case oi: DoubleObjectInspector =>
        (value: Any, row: MutableRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
      case oi =>
        (value: Any, row: MutableRow, ordinal: Int) => row(ordinal) = unwrapData(value, oi)
    }
  }

  def wrap(a: (Any, ObjectInspector)): Any = a match {
    case (s: String, oi: JavaHiveVarcharObjectInspector) =>
      new HiveVarchar(s, s.size)

    case (bd: BigDecimal, oi: JavaHiveDecimalObjectInspector) =>
      new HiveDecimal(bd.underlying())

    case (row: Row, oi: StandardStructObjectInspector) =>
      val struct = oi.create()
      row.zip(oi.getAllStructFieldRefs: Seq[StructField]).foreach {
        case (data, field) =>
          oi.setStructFieldData(struct, field, wrap(data, field.getFieldObjectInspector))
      }
      struct

    case (s: Seq[_], oi: ListObjectInspector) =>
      val wrappedSeq = s.map(wrap(_, oi.getListElementObjectInspector))
      seqAsJavaList(wrappedSeq)

    case (m: Map[_, _], oi: MapObjectInspector) =>
      val keyOi = oi.getMapKeyObjectInspector
      val valueOi = oi.getMapValueObjectInspector
      val wrappedMap = m.map { case (key, value) => wrap(key, keyOi) -> wrap(value, valueOi) }
      mapAsJavaMap(wrappedMap)

    case (obj, _) =>
      obj
  }
}
