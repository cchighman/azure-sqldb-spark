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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

private[spark] class SqlSink(sqlContext: SQLContext,
							 parameters: Map[String, String])
	extends Sink
		with Logging {

	@volatile private var lastBatchId: Long = -1L

	override def toString: String = "SQLSink"

	override def addBatch(batchId: Long, data: DataFrame): Unit = {
		if (batchId <= lastBatchId) {
			log.info(s"Skipping already committed batch $batchId")
		} else {
			val mode = SaveMode.Append
			SQLWriter.write(sqlContext.sparkSession, data, data.queryExecution, mode, parameters)
			lastBatchId = batchId
		}
	}
}