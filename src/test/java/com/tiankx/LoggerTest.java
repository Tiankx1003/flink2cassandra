package com.tiankx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/3/28 22:28
 */
public class LoggerTest {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerTest.class);
    public static void main(String[] args) {
        LOG.info("this is INFO!");
        LOG.debug("this is DEBUG!");
        LOG.error("this is ERROR!");
        LOG.warn("this is WARN!");
    }
}
