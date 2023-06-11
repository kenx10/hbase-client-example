package evg299.lab.hbase.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class App {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "/tmp");

        SpringApplication.run(App.class, args);
    }
}
