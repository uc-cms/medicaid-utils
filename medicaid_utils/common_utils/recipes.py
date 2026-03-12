#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sys
import inspect
import os
import errno
import shutil
import re
from math import ceil
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Optional, Union


def log_assert(
    bool_: bool,
    message: str = "",
    logger: Optional[logging.Logger] = None,
    logger_name: str = "",
    verbose: bool = False,
) -> None:
    """Use this as a replacement for assert if you want the failing of the
    assert statement to be logged."""
    if logger is None:
        logger = logging.getLogger(logger_name)
    try:
        assert bool_, message
    except AssertionError:
        # construct an exception message from the code of the calling frame
        last_stackframe = inspect.stack()[-2]
        source_file, line_no, func = last_stackframe[1:4]
        source = (
            "Traceback (most recent call last):\n"
            + '  File "%s", line %s, in %s\n    '
            % (source_file, line_no, func)
        )
        if verbose:
            # include more lines than that where the statement was made
            source_code = open(source_file).readlines()
            source += "".join(source_code[line_no - 3 : line_no + 1])
        else:
            source += last_stackframe[-2][0].strip()
        logger.debug("%s\n%s" % (message, source))
        raise AssertionError("%s\n%s" % (message, source))


def is_number(x: Any) -> bool:
    try:
        int_val = int(float(x))
        return True
    except Exception as ex:
        return False


def convert_to_int_str(x: Any) -> Union[str, Any]:
    try:
        int_val = str(int(float(x)))
        return int_val
    except Exception as ex:
        return x


def setup_logger(
    logger_name: str, log_file: str, level: int = logging.INFO
) -> None:
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s : %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S",
    )
    handler = TimedRotatingFileHandler(log_file, when="W0", backupCount=0)
    handler.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)
    logger.setLevel(level)


def remove_ignore_if_not_exists(filename: str) -> None:
    try:
        os.remove(filename) if not os.path.isdir(filename) else shutil.rmtree(
            filename
        )
    except (
        OSError
    ) as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or data
            raise  # re-raise exception if a different error occurred


def remove_tail_dot_zeros(a: str) -> str:
    return re.compile(r"(?:(\.)|(\.\d*?[1-9]\d*?))0+(?=\b|[^0-9])").sub(
        r"\2", a
    )
