package com.srini.SpringBatchPOC.processor;


import com.srini.SpringBatchPOC.model.Student;
import org.springframework.batch.item.ItemProcessor;

public class CustomItemProcessor implements ItemProcessor<Student, Student> {

    public Student process(Student item) {
        System.out.println(item.getStdId() +  "    " + item.getName());
        return item;
    }
}
