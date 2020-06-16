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

import java.util.concurrent.Executors

import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}
import com.microsoft.azure.sqldb.spark.SqlDBSpark
import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}

 class SQLServerTestUtilsSpec extends SqlDBSpark {
	 protected var SQLUtils: SQLServerTestUtils = new SQLServerTestUtils

	 val url = sys.env.getOrElse("DB_SERVER", "localhost")
	 val databaseName = sys.env.getOrElse("DATABASE_NAME", "test1")
	 val spnId = sys.env.getOrElse("SPN_ID", "test")
	 val spnSecret = sys.env.getOrElse("SPN_SECRET", "test")
	 val dbTable = "dbo.newtesttable"
	 val portNum = sys.env.getOrElse("PORT_NUM", "1433")

	 val service = Executors.newFixedThreadPool(1)
	 val TenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47"
	 val authority = "https://login.windows.net/" + TenantId
	 val resourceAppIdURI = "https://database.windows.net/"
	 val context = new AuthenticationContext(authority, true, service);

	 //Get access token
	 val ClientCred = new ClientCredential(spnId, spnSecret)
	 val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
	 val accessToken = authResult.get().getAccessToken

	 val config = Config(Map(
		 SqlDBConfig.DatabaseName -> databaseName,
		 SqlDBConfig.URL -> url,
		 SqlDBConfig.DBTable -> "testTable",
		 SqlDBConfig.AccessToken -> accessToken,
		 SqlDBConfig.HostNameInCertificate -> "*.database.windows.net",
		 SqlDBConfig.Encrypt -> "true"
	 ))

	 "createTableSQL" should "generate the correct CREATE TABLE statement with one column" in {
		 val query = "CREATE TABLE [testTable] (one nvarchar(10));"
		 val columns = Map(
			 "one" -> "nvarchar(10)"
		 )
		 SQLUtils.createTableSQL(config, columns) should be(query)
	 }
	 "createTableSQL" should "generate the correct CREATE TABLE statement with more than one column" in {
		 val query = "CREATE TABLE [testTable] (one nvarchar(10),two varchar(5));"
		 val columns = Map(
			 "one" -> "nvarchar(10)",
			 "two" -> "varchar(5)"
		 )
		 SQLUtils.createTableSQL(config, columns) should be(query)
	 }
 }