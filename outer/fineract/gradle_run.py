#!/usr/bin/env python3
"""Bound Gradle without dropping upstream module access options.

This convenience launcher is not an evaluator trust boundary. Evaluation restores
canonical build files and supplies its own test paths in a fresh container.
"""
import os
from pathlib import Path
import re
import subprocess
import sys


def main():
    source = Path.cwd()
    if not (source / 'gradlew').is_file():
        raise SystemExit('Run fineract-gradle from source/')
    properties = (source / 'gradle.properties').read_text(encoding='utf-8')
    match = re.search(r'^org\.gradle\.jvmargs=(.*)$', properties, re.MULTILINE)
    if not match:
        raise SystemExit('Pinned Gradle JVM options missing')
    jvm = re.sub(r'-Xmx\S+', '-Xmx2048m', match.group(1))
    args = ['bash', './gradlew', '--no-daemon', '--max-workers=1', '--no-parallel',
            '-Dorg.gradle.jvmargs=' + jvm, '-Dorg.gradle.vfs.watch=false',
            '-I', '/opt/fineract/limits.gradle', *sys.argv[1:]]
    raise SystemExit(subprocess.call(args, env=os.environ.copy()))


if __name__ == '__main__':
    main()
