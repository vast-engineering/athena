package org.cassandraunit.test.json

import org.hamcrest.Matchers.is
import org.hamcrest.Matchers.notNullValue
import org.junit.Assert.assertThat
import org.cassandraunit.AbstractCassandraUnit4TestCase
import org.cassandraunit.dataset.DataSet
import org.cassandraunit.dataset.json.ClassPathJsonDataSet
import org.junit.Test

class AutomaticallyStartAndLoadSimpleJsonDataSetTest extends AbstractCassandraUnit4TestCase {

  override def getDataSet(): DataSet = {
    new ClassPathJsonDataSet("simpleDataSet.json")
  }

  @Test
  def shouldHaveLoadASimpleDataSet() {
    assertThat(getKeyspace, notNullValue())
    assertThat(getKeyspace.getKeyspaceName, is("beautifulKeyspaceName"))
  }
}
