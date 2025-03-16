/*
 * Copyright (c) 2023-2025, Dariusz Szpakowski
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package tech.kage.event.replicator;

import javax.sql.DataSource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Spring Boot application running the process of event replication.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootApplication
@EnableScheduling
public class Application {
    /**
     * The application's entry point.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    DataSource defaultDataSource(DataSourceProperties dataSourceProperties) {
        var dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();

        dataSource.setJdbcUrl(dataSourceProperties.getUrl());

        return dataSource;
    }

    @Bean(name = "lockManagerDataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    DataSource lockManagerDataSource(DataSourceProperties dataSourceProperties) {
        var dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();

        dataSource.setJdbcUrl(dataSourceProperties.getUrl());
        dataSource.setMaximumPoolSize(1);
        dataSource.setMinimumIdle(1);
        dataSource.setPoolName("HikariPool-LockManager");

        return dataSource;
    }
}
