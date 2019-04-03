package com.example.sparkscala.execute;

import com.example.sparkscala.test.TestOneScala;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class TestExecute implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        TestOneScala.mongo();
    }
}
