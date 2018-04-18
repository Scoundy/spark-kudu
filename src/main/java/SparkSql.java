import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkSql {
    private static final String KUDU_MASTER = "cdh-master:7051";
    private static String tableName = "impala::jtdb.xdwl_cljbxx";
    public static void main(String[] args){
        SparkSql sq = new SparkSql();
        sq.searchBysparkSql();
    }
    public void searchBysparkSql(){
        SparkSession sparkSession = getSparkSession();
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("cphm", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset ds =  sparkSession.read().format("org.apache.kudu.spark.kudu").schema(schema).option("kudu.master",KUDU_MASTER).option("kudu.table",tableName).load();
        ds.createOrReplaceTempView("xdwl_cljbxx");
        List<Row> list=sparkSession.sql("select id,cphm from xdwl_cljbxx where cphm like '%云H%'").limit(10).collectAsList();
        for (Row row:list) {
            System.out.println("--------------------------rowid = "+row.get(0)+" cphm==="+row.get(1));
        }

        sparkSession.close();
    }


    public SparkSession getSparkSession(){
        SparkConf conf = new SparkConf().setAppName("kuduTest")
                .setMaster("local[*]")
                .set("spark.driver.userClassPathFirst", "true");

        conf.set("spark.sql.crossJoin.enabled", "true");
        SparkContext sparkContext = new SparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        return sparkSession;
    }
}
