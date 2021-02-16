import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;

@Slf4j
@SpringBootApplication
@ComponentScan( basePackages = {"config", "service"})
public class MainApp {

    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }

    @PostConstruct
    public void logStartMessage()
    {
        log.info("Application Started");
    }
}