package com.Solum.Solumdemo;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class UserItemWriter implements ItemWriter<User> {

    private final UserRepository userRepository;

    @Autowired
    public UserItemWriter(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void write(Chunk<? extends User> chunk) throws Exception {
        userRepository.saveAll(chunk);
    }
}

