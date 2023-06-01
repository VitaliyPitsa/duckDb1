#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import duckdb
import typing as t
from pathlib import Path


def display_trains(trains: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Вывести данные о рейсе.
    """
    # Проверить, что список поездов не пуст.
    if trains:
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 20
        )
        print(line)
        # Заголовок таблицы.
        print(
            '| {:^4} | {:^30} | {:^20} | {:^20} |'.format(
                "№",
                "Пункт назначения",
                "Номер поезда",
                "Время отправления"
            )
        )
        print(line)
        # Вывести данные о всех поездах.
        for idx, train in enumerate(trains, 1):
            print(
                '| {:>4} | {:<30} | {:<20} |  {:<19} |'.format(
                    idx,
                    train.get('punkt', ''),
                    train.get('nomer', ''),
                    train.get('time', '')
                )
            )
        print(line)


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    # Создать таблицу с информацией о группах.
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS type_st START 1
        """
    )
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS train_st START 1
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS groups (
            train_id INTEGER PRIMARY KEY,
            train_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о поездах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trains (
            train_id INTEGER PRIMARY KEY,
            train_punkt TEXT NOT NULL,
            train_nomer INTEGER NOT NULL,
            train_time TEXT NOT NULL,
            FOREIGN KEY(train_id) REFERENCES groups(train_id)
        )
        """
    )

    conn.close()


def add_train(
        database_path: Path,
        punkt: str,
        nomer: int,
        time: str
) -> None:
    """
    Добавить данные о поезде.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()
    # Получить идентификатор группы в базе данных.
    # Если такой записи нет, то добавить информацию о новой группе.
    cursor.execute(
        """
        SELECT train_id FROM groups WHERE train_title = ?
        """,
        (nomer,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO groups VALUES (nextval('type_st'), ?)
            """,
            (nomer,)
        )
        cursor.execute(
            """
            SELECT currval('type_st')
            """
        )
        sel = cursor.fetchone()
        nomer = sel[0]
    else:
        nomer = row[0]

    # Добавить информацию о новом поезде.
    cursor.execute(
        """
        INSERT INTO trains (train_id, train_punkt, train_nomer, train_time)
        VALUES (nextval('train_st'), ?, ?, ?)
        """,
        (punkt, nomer, time)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех поездов.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT trains.train_punkt, groups.train_title, trains.train_time
        FROM trains
        INNER JOIN groups ON groups.train_id = trains.train_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "punkt": row[0],
            "nomer": row[1],
            "time": row[2],
        }
        for row in rows
    ]


def select_by_num(
        database_path: Path, nomer: int
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать поезда с заданным номером.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT trains.train_punkt, groups.train_title, trains.train_time
        FROM trains
        INNER JOIN groups ON groups.train_id = trains.train_id
        WHERE groups.train_title = ?
        """,
        (nomer,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "punkt": row[0],
            "nomer": row[1],
            "time": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.cwd() / "trains.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("trains")
    parser.add_argument(
        "--version",
        action="version",
        help="The main parser",
        version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления поезда.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new train"
    )
    add.add_argument(
        "-p",
        "--punkt",
        action="store",
        required=True,
        help="punkt"
    )
    add.add_argument(
        "-n",
        "--nomer",
        type=int,
        action="store",
        help="The train number"
    )
    add.add_argument(
        "-t",
        "--time",
        type=str,
        action="store",
        required=True,
        help="The train time"
    )

    # Создать субпарсер для отображения всех поездов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all trains"
    )

    # Создать субпарсер для выбора поездов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the train"
    )
    select.add_argument(
        "-s",
        "--select",
        action="store",
        required=True,
        help="The required select"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить поезд.
    if args.command == "add":
        add_train(db_path, args.punkt, args.nomer, args.time)

    # Отобразить все поезда.
    elif args.command == "display":
        display_trains(select_all(db_path))

    # Выбрать требуемый поезд.
    elif args.command == "select":
        display_trains(select_by_num(db_path, args.select))


if __name__ == '__main__':
    main()
