import logging
import logging.config

logging.config.fileConfig('logging_config.ini')

# create logger
logger = logging.getLogger('root')

# 'application' code
class Test():
    def TestLogging(self):
        logger.debug('debug message')
        logger.info('info message')
        logger.warning('warn message')
        logger.error('error message')
        logger.critical('critical message')


t = Test()
t.TestLogging()

