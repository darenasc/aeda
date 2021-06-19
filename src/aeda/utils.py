from configparser import ConfigParser

from config import CONFIG_DB


def get_db_connection_string(db_conf: str) -> dict:
    parser = ConfigParser()
    filename = CONFIG_DB
    parser.read(filename)

    db = {}
    if parser.has_section(db_conf):
        params = parser.items(db_conf)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(db_conf, filename)
        )

    return db
