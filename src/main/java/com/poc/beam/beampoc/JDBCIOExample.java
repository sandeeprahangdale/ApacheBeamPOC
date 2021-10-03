package com.poc.beam.beampoc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

public class JDBCIOExample {

	public static void main(String[] args) {
		
		Pipeline p = Pipeline.create();
		
		PCollection<String> poutput = p.apply(JdbcIO.<String>read().
				withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
						.create("com.microsoft.sqlserver.jdbc.SQLServerDriver","jdbc:sqlserver://localhost;databaseName=product_catalog")
						.withUsername("sa")
						.withPassword("Test123@1"))
						.withQuery("SELECT product_name, price, discription from products WHERE category = ? ")
						.withCoder(StringUtf8Coder.of())
						.withStatementPreparator(new JdbcIO.StatementPreparator() {
							
							public void setParameters(PreparedStatement preparedStatement) throws Exception {
								// TODO Auto-generated method stub
								preparedStatement.setString(1, "ELECTRONICS");							
							}
						})
						.withRowMapper(new JdbcIO.RowMapper<String>() {
				
							public String mapRow(ResultSet resultSet) throws Exception {
								return resultSet.getString(1)+","+resultSet.getString(2)+","+resultSet.getString(3);
							}
						})
				);
		
		
		poutput.apply(TextIO.write().to("/Users/sandy/Microservices/LLD/beampoc/src/main/resources/jdbc_output.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}
}

