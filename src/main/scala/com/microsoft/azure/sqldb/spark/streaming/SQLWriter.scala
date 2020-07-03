/**
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.sqldb.spark.streaming

import java.sql.{Connection, Date, SQLException, Timestamp}

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils._
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.catalyst.expressions._

import scala.Array
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * The [[SQLWriter]] class is used to write data from a batch query
  * or structured streaming query, given by a [[QueryExecution]], to Azure SQL Database or Azure SQL Data Warehouse.
  */
private[spark] object SQLWriter extends Logging {

  var subset = false
  var DRIVER_NAME: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  var connection: Connection = _
  val daysToMillis = 24L*60L*60L*1000L

  override def toString: String = "SQLWriter"

  def write(
             sparkSession: SparkSession,
             data: DataFrame,
             queryExecution: QueryExecution,
             saveMode: SaveMode,
             parameters: Map[String, String]
           ): Unit = {

    //TODO: (Future Release) Provide the option to the user to define the columns they're writing to and/or column mapping
    val writeConfig = Config(parameters)
    val table = writeConfig.get[String](SqlDBConfig.DBTable)

	  val ignoreColumnNames = Try(writeConfig.get[String]("ignoreColumnNames").map(_.toBoolean).getOrElse(false)).getOrElse(false)
	  connection = ConnectionUtils.getConnection(writeConfig)

    /* Getting Column information */
    try {

      val schema = queryExecution.analyzed.output
      var schemaDatatype = new ListBuffer[DataType]()
      var accessorsArr = new ListBuffer[(SpecializedGetters, Int) => Any]()
      var colNames = ""
      var values = ""

      if (schema.nonEmpty) {
        schema.foreach(col => {
          colNames += col.name + ","
          values +="?,"
          schemaDatatype += col.dataType
          accessorsArr += getAccessor(col.dataType)
        })
      }

      var sql = ""

      if(ignoreColumnNames) {
        sql = "INSERT INTO [" + table + "] VALUES (" + values.substring(0, values.length - 1) + ");"
      } else {
        sql = "INSERT INTO [" + table + "] (" + colNames.substring(0, colNames.length - 1) + ")" + " VALUES (" + values.substring(0, values.length - 1) + ");"
      }

      queryExecution.toRdd.foreachPartition(iter => {
        streamToDB(iter, sql, schemaDatatype, accessorsArr)
      })
    } catch {
      case e: Exception =>
        log.error("Error writing batch data to SQL DB. Error details: " + e)
        throw e
    } finally {
        connection.close()
    }
  }

  /*
  This method checks to see if the table exists in SQL. If it doesn't, it creates the table. This method also ensures that the data types of the data frame are compatible with that of Azure SQL database. If they aren't, it converts them and returns the converted data frame
   */
  def streamToDB(
                  iter: Iterator[InternalRow],
                  sql: String,
                  schemaDatatypes: ListBuffer[DataType],
                  accessorsArr: ListBuffer[(SpecializedGetters, Int) => Any]
                ): Unit = {

    val ps = connection.prepareStatement(sql)
    iter foreach (row => {
      if (ps != null) {
        var rowData = ""
        try {
          var i = 0 //Start Here: Check if there are other ways of representing timestamp
          for (accessor <- accessorsArr) {
            val data = accessor(row, i)
            schemaDatatypes(i) match {
              case BooleanType => ps.setBoolean(i + 1, data.asInstanceOf[Boolean])
              case ByteType => ps.setByte(i + 1, data.asInstanceOf[Byte])
              case ShortType => ps.setShort(i + 1, data.asInstanceOf[Short])
              case IntegerType => ps.setInt(i + 1, data.asInstanceOf[Int])
              case DateType => ps.setDate(i + 1, new Date((data.asInstanceOf[Int] + 1) * daysToMillis)) //Adding 1 as the accessor does not count the number of days inclusively
              case LongType => ps.setLong(i + 1, data.asInstanceOf[Long])
              case TimestampType => ps.setTimestamp(i + 1, new Timestamp(data.asInstanceOf[Long] / 1000L))
              case FloatType => ps.setFloat(i + 1, data.asInstanceOf[Float])
              case DoubleType => ps.setDouble(i + 1, data.asInstanceOf[Double])
              case StringType => ps.setString(i + 1, data.toString)
              case t: DecimalType => ps.setBigDecimal(i + 1, data.asInstanceOf[Decimal].toJavaBigDecimal)
              case BinaryType => ps.setBytes(i + 1, data.asInstanceOf[Array[Byte]])
              case _ => ps.setString(i + 1, String.valueOf(data))
            }
            rowData += String.valueOf(data) + " "
            i += 1
          }
          ps.execute()
        } catch {
          case e: SQLException => {
            val err = e.toString
            if (err.toLowerCase().contains("invalid object name")) { //Handling 'table not found' error
              throw new Exception(err)
            } else {
              log.error("Error inserting row into table. Skipping thw row. Row Data contains: " + rowData)
            }
          }
        }
      } else {
        throw new Exception(s"Error: PreparedStatement is null")
      }
    })

  }

  //Copied from Spark 2.4 (InternalRow.scala class)
  def getAccessor(dataType: DataType): (SpecializedGetters, Int) => Any = dataType match {
    case BooleanType => (input, ordinal) => input.getBoolean(ordinal)
    case ByteType => (input, ordinal) => input.getByte(ordinal)
    case ShortType => (input, ordinal) => input.getShort(ordinal)
    case IntegerType | DateType => (input, ordinal) => input.getInt(ordinal)
    case LongType | TimestampType => (input, ordinal) => input.getLong(ordinal)
    case FloatType => (input, ordinal) => input.getFloat(ordinal)
    case DoubleType => (input, ordinal) => input.getDouble(ordinal)
    case StringType => (input, ordinal) => input.getUTF8String(ordinal)
    case BinaryType => (input, ordinal) => input.getBinary(ordinal)
    case CalendarIntervalType => (input, ordinal) => input.getInterval(ordinal)
    case t: DecimalType => (input, ordinal) => input.getDecimal(ordinal, t.precision, t.scale)
    case t: StructType => (input, ordinal) => input.getStruct(ordinal, t.size)
    case _: ArrayType => (input, ordinal) => input.getArray(ordinal)
    case _: MapType => (input, ordinal) => input.getMap(ordinal)
    //case u: UserDefinedType[_] => getAccessor(u.sqlType)
    case _ => (input, ordinal) => input.get(ordinal, dataType)
  }
}