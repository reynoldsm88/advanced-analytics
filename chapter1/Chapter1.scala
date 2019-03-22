import org.apache.spark.sql._
import org.apache.spark.sql.types._

def longForm( df : DataFrame ): DataFrame = {
    import df.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion
    val columns = df.schema.map( _.name )
    df.flatMap( row => {
      val metric = row.getAs[String](columns.head)
      columns.tail.map(columnName => (metric, columnName, row.getAs[String](columnName).toDouble) )
    } ).toDF("metric", "field", "value")
}

def useSchema() : StructType = {
	val fields = {
		List(
			StructField( "id_1", IntegerType, true ),
			StructField( "id_2", IntegerType, true ),
			StructField( "cmp_fname_c1", DoubleType, true ),
			StructField( "cmp_fname_c2", DoubleType, true ),
			StructField( "cmp_lname_c1", DoubleType, true ),
			StructField( "cmp_lname_c2", DoubleType, true ),
			StructField( "cmp_sex", IntegerType, true ),
			StructField( "cmp_bd", IntegerType, true ),			
			StructField( "cmp_bm", IntegerType, true ),
			StructField( "cmp_by", IntegerType, true ),
			StructField( "cmp_plz", IntegerType, true ),
			StructField( "is_match", BooleanType, true )
		)
	}

	StructType( fields )
}


val data = spark.read.option( "nullvalue", "?" )
					 .option( "header", "true" )
					 .schema( useSchema() )
					 .csv( "/Users/michael/workspace/advanced-analytics/chapter1/donation" )
					 .cache()


val summary = data.describe()
val matches = data.where( $"is_match" === true ).describe()
val misses = data.where( $"is_match" === false ).describe()

// don't need this
val raw = sc.textFile( "/Users/michael/workspace/advanced-analytics/chapter1/donation" )