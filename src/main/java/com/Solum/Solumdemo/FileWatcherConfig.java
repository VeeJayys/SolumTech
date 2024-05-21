package com.Solum.Solumdemo;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//import javax.annotation.PostConstruct;
//import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class FileWatcherConfig {

    private final FileWatcherService fileWatcherService;

    public FileWatcherConfig(FileWatcherService fileWatcherService) {
        this.fileWatcherService = fileWatcherService;
    }

    @Bean
    public void startFileWatcher() {
        new Thread(() -> fileWatcherService.watchDirectoryPath(Paths.get("./input"))).start();
    }
}

