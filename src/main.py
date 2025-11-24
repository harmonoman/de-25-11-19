from logger import AppLogger


def main():
    logger = AppLogger().get_logger()

    logger.debug("Debug message")
    logger.info("User logged in")
    logger.error("Something failed")
    logger.critical("App crashed!")


if __name__ == '__main__':
    main()
