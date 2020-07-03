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

import java.sql.{Date, Timestamp}
import java.util.Locale
import java.util.concurrent.Executors
import java.sql.Connection

import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import com.microsoft.azure.sqldb.spark.utils.SQLServerTestUtils
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamTest, StreamingQueryException}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.mockito.IdiomaticMockito

import scala.math.BigDecimal


//Creating Schema for data based on the schema that the JDBC driver reads the data
case class SQLTestSchema(binaryType: Array[Byte], boolType: Boolean, byteType: Int, dblType: Double, intType: Int, fltType: Double, decimalType: BigDecimal, longType: Long, shortType: Int, strType: String, dtType: Date, tsType: java.sql.Timestamp) extends Ordered[SQLTestSchema] {
	override def compare(that: SQLTestSchema): Int = {
		if (this.binaryType.deep == that.binaryType.deep && this.boolType.equals(that.boolType) && this.byteType.equals(that.byteType) &&
			this.dblType.equals(that.dblType) && this.intType.equals(that.intType) &&
			this.fltType.equals(that.fltType) && this.decimalType.equals(that.decimalType) && this.longType.equals(that.longType) &&
			this.shortType.equals(that.shortType) && this.strType.equals(that.strType) &&
			this.dtType.equals(that.dtType) && this.tsType.equals(that.tsType)) {
			0
		} else if (this.byteType > that.byteType || this.dblType > that.dblType || this.intType > that.intType || this.fltType > that.fltType) {
			1
		} else {
			-1
		}
	}

	override def equals(obj: scala.Any): Boolean = {
		var that = obj.asInstanceOf[SQLTestSchema]
		if (this.binaryType.deep == that.binaryType.deep && this.boolType.equals(that.boolType) && this.byteType.equals(that.byteType) &&
			this.dblType.equals(that.dblType) && this.intType.equals(that.intType) &&
			this.fltType.equals(that.fltType) && this.decimalType.equals(that.decimalType) && this.longType.equals(that.longType) &&
			this.shortType.equals(that.shortType) && this.strType.equals(that.strType) &&
			this.dtType.equals(that.dtType) && this.tsType.equals(that.tsType)) {
			return true
		} else {
			return false
		}
	}
}

class SQLSinkTest extends StreamTest with SharedSQLContext with IdiomaticMockito {

  trait Setup {
	//  val mockConn = mock[Connection]
//	  when(mockConn.execute) thenReturn "mocked!"
  }
	override val streamingTimeout: Span = 60.seconds
	val config = new SparkConf().setAppName("SQLSinkTest").setMaster("local[*]")
	val sc = new SparkContext(config)
	implicit val sparkSession: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate() //enableHiveSupport().

	import sparkSession.implicits._

	val url = sys.env.getOrElse("DB_SERVER", "localhost")
	val databaseName = sys.env.getOrElse("DATABASE_NAME", "test1")
	val spnId = sys.env.getOrElse("SPN_ID", "test")
	val spnSecret = sys.env.getOrElse("SPN_SECRET", "test")
	val dbTable = "dbo.newtesttable"
	val portNum = sys.env.getOrElse("PORT_NUM", "1433")
	//Definition of the columns for the table in SQL Server

	val service = Executors.newFixedThreadPool(1)
	val TenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47"
	val authority = "https://login.windows.net/" + TenantId
	val resourceAppIdURI = "https://database.windows.net/"
	val context = new AuthenticationContext(authority, true, service);

	//Get access token
	val ClientCred = new ClientCredential(spnId, spnSecret)
	val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
	val accessToken = authResult.get().getAccessToken
	//Definition of the columns for the table in SQL Server
	val columns = Map(
		"binaryType" -> "varbinary(30)",
		"boolType" -> "bit",
		"byteType" -> "smallint",
		"dblType" -> "float",
		"intType" -> "int",
		"fltType" -> "float",
		"decimalType" -> "decimal(14,10)",
		"longType" -> "bigint",
		"shortType" -> "smallint",
		"strType" -> "nvarchar(10)",
		"dtType" -> "date",
		"tsType" -> "datetime2(7)"
	)
	//Pre-defining date & timestamp
	val dt1 = new Date(1546243200000L)
	val dt2 = new Date(28800000L)

	var tableConfig = Config(Map(
		"url" -> url,
		"dbTable" -> "testTable",
		"databaseName" -> databaseName,
		"accessToken" -> accessToken,
		"hostNameInCertificate" -> "*.database.windows.net",
		"encrypt" -> "true"
	))
	var ts1 = Timestamp.valueOf("2019-01-01 10:20:40")
	var ts2 = Timestamp.valueOf("1970-01-01 00:01:07")


	protected var SQLUtils: SQLServerTestUtils = _
	protected var tConfig: Config = null

	override def beforeAll(): Unit = {
		super.beforeAll()
		SQLUtils = new SQLServerTestUtils
		tConfig =  tableConfig
	}

	override def afterAll(): Unit = {
		if (SQLUtils != null) {
			SQLUtils = null
		}
		tConfig = null
		super.afterAll()
	}


	test("Structured Streaming - Write to Azure SQL DB") {
		val columns = Map(
			"input" -> "nvarchar(10)"
		)

		var success = SQLUtils.createTable(tConfig, columns)
		if (!success) {
			fail("Table creation failed. Please check your config")
		}
		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		val df = input.toDF().withColumnRenamed("value", "input")

		withTempDir { checkpointDir =>
			stream = df.writeStream
				.format("sqlserver")
				.option("checkpointLocation",  checkpointDir.getCanonicalPath)
				.option("ignoreColumnNames", "true")
				.option("databaseName", "Recon")
				.option("dbTable", "TestTable")
				.option("url", "reconserverppe.database.windows.net")
				.outputMode("Append")
		}
		var streamStart = stream.start()

		try {
			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}
			checkDatasetUnorderly(spark.read.sqlDB(tConfig).as[String].map(_.toInt), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		} finally {
			success = SQLUtils.dropTable(tConfig)
			if (!success) {
				fail("Table deletion failed. Please check your config")
			}
			streamStart.stop()
		}
	}


	test("Structured Streaming - Incorrect username/password ; Ensure the right error surfaces") {
		tConfig.properties.-("user").+("user" -> "wronguser")

		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		var df = input.toDF().withColumnRenamed("value", "input")

		var ex = intercept[StreamingQueryException] {
			withTempDir { checkpointDir =>
				
				tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
				stream = df.writeStream
					.format("sqlserver")
					.options(tConfig.asOptions)
					.outputMode("Append")
			}
			var streamStart = stream.start()

			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			streamStart.stop()
		}
		assert(
			ex.getMessage
				.toLowerCase(Locale.ROOT)
				.contains(s"login failed for user")
		)


	}

	test("Structured Streaming - Incorrect Server Name ; Ensure the right error surfaces") {
		val wrongurl = "wronguri.database.windows.net"
		tConfig.properties.-("url").+("url" -> wrongurl)

		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		var df = input.toDF().withColumnRenamed("value", "input")

		var ex = intercept[StreamingQueryException] {
			withTempDir { checkpointDir =>
				
				tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
				stream = df.writeStream
					.format("sqlserver")
					.options(tConfig.asOptions)
					.outputMode("Append")
			}
			var streamStart = stream.start()

			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			streamStart.stop()
		}
		assert(
			ex.getMessage
				.toLowerCase(Locale.ROOT)
				.contains(s"connection to the host " + wrongurl + ", port " + portNum + " has failed")
		)
	}

	test("Structured Streaming - Incorrect Database Name ; Ensure the right error surfaces") {
		var wrongdb = "wrongdb"
		
		tConfig.properties.-("databaseName").+("databaseName" -> "wrongdb")

		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		var df = input.toDF().withColumnRenamed("value", "input")

		var ex = intercept[StreamingQueryException] {
			withTempDir { checkpointDir =>
				
				tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
				stream = df.writeStream
					.format("sqlserver")
					.options(tConfig.asOptions)
					.outputMode("Append")
			}
			var streamStart = stream.start()

			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			streamStart.stop()
		}
		assert(
			ex.getMessage
				.toLowerCase(Locale.ROOT)
				.contains(String.format("cannot open database \"%s\" requested by the login", wrongdb))
		)
	}

	test("Structured Streaming - Incomplete options defined ; Ensure the right error surfaces") {
		tConfig.properties.-("user")

		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		val df = input.toDF().withColumnRenamed("value", "input")
		val ex = intercept[StreamingQueryException] {
			withTempDir { checkpointDir =>
				tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
				stream = df.writeStream
					.format("sqlserver")
					.options(tConfig.asOptions)
					.outputMode("Append")
			}
			val  streamStart = stream.start()

			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			streamStart.stop()
		}
		assert(
			ex.getMessage
				.toLowerCase(Locale.ROOT)
				.contains(s"user not found in config")
		)
	}

	test("Structured Streaming - Table does not exist ; Ensure the right error surfaces") {
		val nonexistentTable = "nonexistent"
		tConfig.properties.-("dbTable").+("dbTable" -> nonexistentTable)
		
		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[String]
		input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		var df = input.toDF().withColumnRenamed("value", "input")

		var ex = intercept[StreamingQueryException] {
			withTempDir { checkpointDir =>

				tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
				stream = df.writeStream
					.format("sqlserver")
					.options(tConfig.asOptions)
					.outputMode("Append")
			}
			var streamStart = stream.start()

			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			streamStart.stop()
		}
		assert(
			ex.getMessage
				.toLowerCase(Locale.ROOT)
				.contains(s"invalid object name '$nonexistentTable'")
		)
	}

	test("Structured Streaming - Check Data Type Translation") {

		var success = SQLUtils.createTable(tableConfig, columns)
		if (!success) {
			fail("Table creation failed. Please check your config")
		}
		var stream: DataStreamWriter[Row] = null

		var row1 = SQLTestSchema(Array[Byte](1, 2, 3), true, -128, 2.333333333333, -123, 10.99992F, BigDecimal(104.21027824), -9223372036854775808L, -32768, "test", dt1, ts1)
		var row2 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-21.01221138), 9223372036854775807L, 32767, "testrow2", dt2, ts2)

		val input = MemoryStream[SQLTestSchema]
		input.addData(row1, row2)

		withTempDir { checkpointDir =>
			
			tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
			stream = input.toDF().writeStream
				.format("sqlserver")
				.options(tConfig.asOptions)
				.outputMode("Append")
		}
		var streamStart = stream.start()

		try {
			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			val checkData = spark.read.sqlDB(tableConfig).as[SQLTestSchema]

			checkDatasetUnorderly(checkData, row1, row2)
		}
		finally {
			success = SQLUtils.dropTable(tableConfig)
			if (!success) {
				fail("Table deletion failed. Please check your config")
			}
			streamStart.stop()
		}
	}

	test("Structured Streaming - Skip Rows that fail and document the failed rows") {
		//Updating Column Definition
		val updatedColumns = columns.-("tsType").+("tsType" -> "datetime2(1)")
		val ts3 = Timestamp.valueOf("2019-01-01 10:20:40.123")

		var success = SQLUtils.createTable(tableConfig, updatedColumns)
		if (!success) {
			fail("Table creation failed. Please check your config")
		}
		var stream: DataStreamWriter[Row] = null

		//This row should get inserted correctly
		var row1 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-21.01221138), 9223372036854775807L, 32767, "testrow2", dt2, ts2)

		//Testing for incorrect Decimal
		var row2 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-215324.5012211328), 9223372036854775807L, 32767, "testrow2", dt2, ts2)

		//Testing for incorrect Smallint
		var row3 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-21.01221138), 9223372036854775807L, 32768, "testrow2", dt2, ts2)

		//Testing for incorrect string
		var row4 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-21.01221138), 9223372036854775807L, 32767, "testrow2-incorrect-string", dt2, ts2)

		//Testing for incorrect DateTime
		var row5 = SQLTestSchema(Array[Byte](1, 2, 3, -1, -2, -127), false, 127, 200000.3334210457948, 133055022, 1012300.012345F, BigDecimal(-21.01221138), 9223372036854775807L, 32767, "testrow2-incorrect-string", dt2, ts3)

		val input = MemoryStream[SQLTestSchema]
		input.addData(row1, row2, row3, row4, row5)

		withTempDir { checkpointDir =>
			
			tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
			stream = input.toDF().writeStream
				.format("sqlserver")
				.options(tableConfig.asOptions)
				.outputMode("Append")
		}
		var streamStart = stream.start()

		try {
			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}

			val checkData = spark.read.sqlDB(tableConfig).as[SQLTestSchema]

			checkDatasetUnorderly(checkData, row1) //row1 should be successfuly added and all other should be out of bounds
		}
		finally {
			success = SQLUtils.dropTable(tableConfig)
			if (!success) {
				fail("Table deletion failed. Please check your config")
			}
			streamStart.stop()
		}
	}


	test("Structured Streaming - Timestamp formats and millisecond support") {
		val columns = Map(
			"value" -> "datetime2(7)"
		)

		var ts1 = Timestamp.valueOf("2019-01-01 10:20:40.123") //Support for only milliseconds for now
		var ts2 = Timestamp.valueOf("1970-01-01 00:01:07.456")
		var ts3 = Timestamp.valueOf("2019-02-10 22:28:48.582")
		var ts4 = Timestamp.valueOf("1988-01-01 00:01:00")

		var success = SQLUtils.createTable(tableConfig, columns)
		if (!success) {
			fail("Table creation failed. Please check your config")
		}
		var stream: DataStreamWriter[Row] = null
		val input = MemoryStream[Timestamp]
		input.addData(ts1, ts2, ts3, ts4)
		var df = input.toDF()

		withTempDir { checkpointDir =>
			
			tConfig.properties.+("checkpointLocation" -> checkpointDir.getCanonicalPath)
			stream = df.writeStream
				.format("sqlserver")
				.options(tConfig.asOptions)
				.outputMode("Append")
		}
		var streamStart = stream.start()

		try {
			failAfter(streamingTimeout) {
				streamStart.processAllAvailable()
			}
			val checkData = spark.read.sqlDB(tableConfig).as[Timestamp]
			checkDatasetUnorderly(checkData, ts1, ts2, ts3, ts4)
		} finally {
			success = SQLUtils.dropTable(tableConfig)
			if (!success) {
				fail("Table deletion failed. Please check your config")
			}
			streamStart.stop()
		}
	}

	implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
		def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
	}

}