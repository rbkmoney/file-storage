package com.rbkmoney.file.storage.config;

import com.google.common.base.Predicates;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfiguration {

    @Bean
    public Docket productApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(metaData())
                .select()
                .paths(Predicates.not(PathSelectors.regex("/error")))
                .build();
    }

    private ApiInfo metaData() {
        return new ApiInfoBuilder()
                .title("REST endpoint")
                .description("\"Endpoint для выгрузки документов на сервер\"")
                .version("0.0.1-SNAPSHOT")
                .contact(new Contact("RBK.money", "https://github.com/rbkmoney", "support@rbkmoney.com"))
                .build();
    }
}
