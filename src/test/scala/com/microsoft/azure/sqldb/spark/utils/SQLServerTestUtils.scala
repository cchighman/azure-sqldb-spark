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
package com.microsoft.azure.sqldb.spark.utils

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils.getConnection


class SQLServerTestUtils {
	def createTable(writeConfig: Config, columns: Map[String, String]): Boolean = {
		if (!dataTypeVerify(columns)) {
			return false
		}
		val sql = createTableSQL(writeConfig, columns)
		if (sql.equals(null)) {
			return false
		}
		val conn = getConnection(writeConfig)
		if (conn.equals(null)) {
			return false
		}
		try {
			val stmt = conn.createStatement()
			stmt.executeUpdate(sql)
			conn.close()
		} catch {
			case e: Exception => return false
		}
		true
	}

	def createTableSQL(writeConfig: Config, columns: Map[String, String]): String = {
		val tableDroppedIfExists = dropTable(writeConfig)
		if (!tableDroppedIfExists) return null

		var columnDef = ""
		for (column <- columns) {
			columnDef += column._1 + " " + column._2
			if(!columns.last.equals(column)) columnDef += ","
		}
		val table = writeConfig.get[String](SqlDBConfig.DBTable).orNull
		if (table.equals(null)) {
			return null
		}
		"CREATE TABLE [" + table + "] (" + columnDef + ");"
	}

	def dropTable(writeConfig: Config): Boolean = {
		val table = writeConfig.get[String](SqlDBConfig.DBTable).orNull
		if (table.equals(null)) {
			return false
		}
		val conn = getConnection(writeConfig)
		if (conn.equals(null)) {
			return false
		}
		// var sql = "IF EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + table + "') DROP TABLE " + table + ";"
		val sql = "DROP TABLE IF EXISTS [" + table + "]" //Supported SQL Server 2016 and beyond
		try {
			val stmt = conn.createStatement()
			stmt.executeUpdate(sql)
			conn.close()
		} catch {
			case e: Exception => return false
		}
		true
	}

	private def dataTypeVerify(typeMap: Map[String, String]): Boolean = {
		for (typeName <- typeMap.values) {
			val dataType = typeName.split('(')(0).trim

			dataType match {
				case "bigint" => ""
				case "binary" => ""
				case "bit" => ""
				case "char" => ""
				case "date" => ""
				case "datetime" => ""
				case "datetime2" => ""
				case "datetimeoffset" => ""
				case "decimal" => ""
				case "float" => ""
				case "image" => return false
				case "int" => ""
				case "money" => ""
				case "nchar" => ""
				case "ntext" => ""
				case "numeric" => ""
				case "nvarchar" => ""
				case "nvarchar(max)" => ""
				case "real" => ""
				case "smalldatetime" => ""
				case "smallint" => ""
				case "smallmoney" => ""
				case "text" => ""
				case "time" => ""
				case "timestamp" => ""
				case "tinyint" => ""
				case "udt" => return false
				case "uniqueidentifier" => ""
				case "varbinary" => ""
				//case "varbinary(max)" => ""
				case "varchar" => ""
				//case "varchar(max)" => ""
				case "xml" => ""
				case "sqlvariant" => ""
				case "geometry" => ""
				case "geography" => ""
				case _ => return false
			}
		}
		true
	}

	def truncateTable(config: Map[String, String]): Boolean = {
		val writeConfig = Config(config)
		val table = writeConfig.get[String](SqlDBConfig.DBTable).orNull
		if (table.equals(null)) {
			return false
		}
		val conn = getConnection(writeConfig)
		if (conn.equals(null)) {
			return false
		}

		val sql = "TRUNCATE TABLE [" + table + "];"
		try {
			val stmt = conn.createStatement()
			stmt.executeUpdate(sql)
			conn.close()
		} catch {
			case e: Exception => return false
		}
		true
	}

	private def preFlightChecks(config: Map[String, String]): Boolean = {
		var writeConfig = Config(config)
		val conn = getConnection(writeConfig)
		val dbName = writeConfig.get[String](SqlDBConfig.DatabaseName).get
		if (conn.equals(null)) {
			return false
		}

		//Check if Server and Database Exist
		var sql = "SELECT name from master.dbo.sys.databases where name='" + dbName + "' OR [name]='" + dbName + "'"
		try {
			val stmt = conn.createStatement()
			val req = stmt.executeQuery(sql)
			conn.close()
			val res = req.getString(1)
			if (res.eq(null) || res.eq("") || !res.eq(dbName)) {
				return false
			}
			true
		} catch {
			case e: Exception =>  false
		}
	}
}