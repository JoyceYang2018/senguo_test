import logging

# create logger
logger = logging.getLogger('root')

# Set default log level
logger.setLevel(logging.DEBUG)


ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

ch2 = logging.FileHandler('logging.log')
ch2.setLevel(logging.WARNING)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)
ch2.setFormatter(formatter)

# add ch to logger
# The final log level is the higher one between the default and the one in handler
logger.addHandler(ch)
logger.addHandler(ch2)

# 'application' code
# logger.debug('debug message')
# logger.info('info message')
# logger.warning('warn message')
# logger.error('error message')
# logger.critical('critical message')

def test(list):
    return list[1]

try:
    print(test([]))
except IndexError:
    logger.warning("Something Bad Happen", exc_info=True)

except KeyError:
    logger.info("Something Bad Happen", exc_info=True)
