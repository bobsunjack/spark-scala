package com.example.sparkscala.execute;

import com.example.sparkscala.sample.Sample1;
import com.example.sparkscala.test.TestOneScala;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class TestExecute implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
       // TestOneScala.mongo3();
       // TestOneScala.mongo2();

       // TestOneScala.mongo();
        System.out.println("begin-------------------");
        Sample1.test();
        System.out.println("end-------------------");
    }
}
