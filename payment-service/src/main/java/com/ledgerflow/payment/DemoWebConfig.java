package com.ledgerflow.payment;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Demo profile only: serves the static dashboard (classpath:/dashboard/) at "/". The files are
 * deliberately not under /static, so the default profile serves nothing new.
 */
@Configuration(proxyBeanMethods = false)
@Profile("demo")
class DemoWebConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/").setViewName("forward:/index.html");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // Each pattern resolves only its ** part against its location.
        registry.addResourceHandler("/index.html", "/favicon.svg").addResourceLocations("classpath:/dashboard/");
        registry.addResourceHandler("/css/**").addResourceLocations("classpath:/dashboard/css/");
        registry.addResourceHandler("/js/**").addResourceLocations("classpath:/dashboard/js/");
    }
}
