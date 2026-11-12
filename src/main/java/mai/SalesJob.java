package mai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;

public class SalesJob {
    private static final String[][] GROUPS = {
        {"customer_first_name", "customer_last_name", "customer_age", "customer_email", "customer_country", "customer_postal_code", "customer_pet_type", "customer_pet_name", "customer_pet_breed"},
        {"seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"},
        {"product_name", "product_category", "product_price", "product_quantity", "pet_category", "product_weight", "product_color", "product_size", "product_brand", "product_material", "product_description", "product_rating", "product_reviews", "product_release_date", "product_expiry_date"},
        {"store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"},
        {"supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"}
    };
    private static final String[] NAMES = {"customer", "seller", "product", "store", "supplier"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoints");
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("sales")
            .setGroupId("sales-star")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka sales")
            .addSink(new SalesSink()).name("PostgreSQL star");
        env.execute("sales_stream");
    }

    public static class SalesSink extends RichSinkFunction<String> {
        private transient Connection conn;
        private transient ObjectMapper mapper;
        private transient PreparedStatement[] dimensions;
        private transient PreparedStatement dates;
        private transient PreparedStatement facts;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:postgresql://postgres:5432/postgres", "postgres", "postgres");
            conn.setAutoCommit(false);
            mapper = new ObjectMapper();
            dimensions = new PreparedStatement[NAMES.length];
            for (int i = 0; i < NAMES.length; i++) {
                String placeholders = String.join(", ", java.util.Collections.nCopies(GROUPS[i].length + 1, "?"));
                dimensions[i] = conn.prepareStatement("INSERT INTO star.dim_" + NAMES[i]
                    + " VALUES (" + placeholders + ") ON CONFLICT DO NOTHING");
            }
            dates = conn.prepareStatement("INSERT INTO star.dim_date VALUES (?, ?::date, ?, ?) ON CONFLICT DO NOTHING");
            facts = conn.prepareStatement("INSERT INTO star.fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (sale_key) DO NOTHING");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            JsonNode row = mapper.readTree(value);
            try {
                String[] keys = new String[NAMES.length];
                for (int i = 0; i < NAMES.length; i++) {
                    String[] attributes = new String[GROUPS[i].length];
                    for (int j = 0; j < attributes.length; j++) {
                        attributes[j] = row.required(GROUPS[i][j]).asText();
                    }
                    // Хешируем полный набор атрибутов, чтобы не склеить разные версии товара.
                    keys[i] = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(mapper.writeValueAsString(attributes).getBytes(StandardCharsets.UTF_8)));
                    dimensions[i].setString(1, keys[i]);
                    for (int j = 0; j < attributes.length; j++) dimensions[i].setString(j + 2, attributes[j]);
                    dimensions[i].executeUpdate();
                }
                LocalDate date = LocalDate.parse(row.required("sale_date").asText(), DateTimeFormatter.ofPattern("M/d/uuuu").withResolverStyle(java.time.format.ResolverStyle.STRICT));
                String dateKey = date.toString();
                dates.setString(1, dateKey);
                dates.setString(2, dateKey);
                dates.setString(3, dateKey.substring(0, 4));
                dates.setString(4, dateKey.substring(0, 7));
                dates.executeUpdate();
                facts.setString(1, row.required("sale_key").asText());
                facts.setString(2, row.required("source_file").asText());
                facts.setInt(3, Integer.parseInt(row.required("id").asText()));
                for (int i = 0; i < keys.length; i++) facts.setString(i + 4, keys[i]);
                facts.setString(9, dateKey);
                facts.setInt(10, Integer.parseInt(row.required("sale_quantity").asText()));
                facts.setBigDecimal(11, new BigDecimal(row.required("sale_total_price").asText()));
                facts.executeUpdate();
                // Продажа и все ее измерения фиксируются одной транзакцией.
                conn.commit();
            } catch (Exception error) {
                conn.rollback();
                throw error;
            }
        }

        @Override
        public void close() throws Exception {
            if (dimensions != null) for (PreparedStatement statement : dimensions) statement.close();
            if (dates != null) dates.close();
            if (facts != null) facts.close();
            if (conn != null) conn.close();
        }
    }
}
