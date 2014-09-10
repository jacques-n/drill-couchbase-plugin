import org.apache.drill.BaseTestQuery;
import org.junit.Test;


public class TestCouchbasePlugin extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCouchbasePlugin.class);

  @Test
  public void testListBuckets() throws Exception{
    test("use couchbase;");
    test("show tables;");
  }
}
